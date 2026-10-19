import argparse
import logging
import os
import queue
import random
import threading
import time

from .config import CLUSTER, DEFAULT_TIMING, FOLLOWING, LEADING, LOOKING, VALUE_MAX, VALUE_MIN, quorum
from .coordinator import LeaderCoordinator
from .election import ElectionManager
from .game import TargetState
from .logstore import COMMITTED, PROPOSED, LogStore
from .messages import Ack, AckKind, Commit, Election, Forward, Heartbeat, Op, Propose
from .network import NodeServer, Peer, send_message

log = logging.getLogger("zab")


# all state is changed under self.lock, and while holding it we only
# queue messages and write to the local sqlite, never block on the network
class Node:
    def __init__(self, node_id, cluster=None, data_dir="data", timing=DEFAULT_TIMING):
        self.cluster = dict(cluster or CLUSTER)
        if node_id not in self.cluster:
            raise ValueError(f"unknown node: {node_id}")
        self.node_id = node_id
        self.timing = timing
        self.quorum = quorum(len(self.cluster))

        os.makedirs(data_dir, exist_ok=True)
        self.lock = threading.RLock()
        self.log = LogStore(os.path.join(data_dir, f"{node_id}.db"))
        self.state = TargetState()
        self.state.rebuild(self.log.committed_entries())  # recover the target after a restart

        self.role = LOOKING
        self.accepted_epoch = self.log.accepted_epoch()
        self.leader_id = None
        self.synced = False  # follower: synced with the current leader
        self.last_heartbeat = time.monotonic()
        self.election_timeout = self._random_timeout()
        self._join_sent_at = 0.0

        self.clients = {}  # client name -> socket, clients connected to this node
        self._conn_client = {}
        self._client_out = queue.Queue()

        self.peers = {}
        self.election = ElectionManager(self)
        self.leader = LeaderCoordinator(self)
        self.server = NodeServer(node_id, self.cluster[node_id], self._dispatch, self._on_disconnect)
        self._stopped = threading.Event()

    def start(self):
        self.peers = {pid: Peer(self.node_id, pid, addr, self.timing.heartbeat_interval)
                      for pid, addr in self.cluster.items() if pid != self.node_id}
        self.server.start()
        threading.Thread(target=self._client_sender, name=f"{self.node_id}-clients", daemon=True).start()
        threading.Thread(target=self._timer_loop, name=f"{self.node_id}-timer", daemon=True).start()
        host, port = self.cluster[self.node_id]
        log.info("[%s] started on %s:%d, %d log entries, latest (epoch,seq)=%s, accepted_epoch=%d",
                 self.node_id, host, port, len(self.log.entries()), self.log.last_zxid(), self.accepted_epoch)
        with self.lock:
            self.election.start_looking("node started")
        return self

    def stop(self):
        with self.lock:
            self._stopped.set()
        self.server.stop()
        for peer in self.peers.values():
            peer.close()
        self._client_out.put(None)
        with self.lock:
            self.log.close()

    def status(self):
        with self.lock:
            return self._status_locked()

    def _status_locked(self):
        epoch, seq = self.log.last_zxid()
        return {
            "node_id": self.node_id, "role": self.role, "epoch": self.accepted_epoch,
            "leader_id": self.leader_id, "last_zxid": [epoch, seq],
            "ready": self.leader.active if self.role == LEADING else (self.role == FOLLOWING and self.synced),
            "target": self.state.value, "clients": sorted(self.clients),
        }

    def send(self, peer_id, msg):
        peer = self.peers.get(peer_id)
        if peer is not None:
            peer.send(msg)

    def broadcast(self, msg):
        for peer in self.peers.values():
            peer.send(msg)

    def _to_client(self, conn, msg):
        self._client_out.put((conn, msg))

    def _client_sender(self):
        while True:
            item = self._client_out.get()
            if item is None:
                return
            conn, msg = item
            try:
                send_message(conn, msg)
            except OSError:
                pass  # client gone, its reader thread cleans up

    def _random_timeout(self):
        return random.uniform(*self.timing.election_timeout)

    def reset_timer(self):
        self.last_heartbeat = time.monotonic()
        self.election_timeout = self._random_timeout()

    def set_epoch(self, epoch):
        if epoch != self.accepted_epoch:
            self.accepted_epoch = epoch
            self.log.set_accepted_epoch(epoch)  # persisted to spot stale leaders after a restart

    def apply_committed(self, entries):
        for e in entries:
            self.state.apply(e)
            log.info("[%s] committed (%d,%d) %s %d (%s) -> current target %s",
                     self.node_id, e["epoch"], e["seq"], e["op"], e["value"], e["client"], self.state.value)
            self._notify_clients(e)
            if self.role == LEADING:
                self.leader.on_committed(e)

    def _notify_clients(self, entry):
        msg = Commit(sender_id=self.node_id, epoch=entry["epoch"], seq=entry["seq"],
                     op=entry["op"], value=entry["value"], client=entry["client"])
        for conn in self.clients.values():
            self._to_client(conn, msg)

    def _timer_loop(self):
        while not self._stopped.wait(self.timing.tick):
            with self.lock:
                if self._stopped.is_set():
                    return
                now = time.monotonic()
                if self.role == LEADING:
                    self.leader.tick(now)
                elif self.role == FOLLOWING:
                    waited = now - self.last_heartbeat
                    if waited > self.election_timeout:
                        self.election.start_looking(f"no leader heartbeat for {waited:.2f}s")
                    elif self.leader_id and not self.synced and now - self._join_sent_at > self.timing.ack_timeout:
                        self._send_join()  # join or sync got lost, resend
                else:
                    self.election.tick(now)

    def _dispatch(self, msg, conn):
        with self.lock:
            if self._stopped.is_set():
                return
            if msg.sender_id not in self.cluster:
                self._on_client(msg, conn)
            elif isinstance(msg, Heartbeat):
                self._on_heartbeat(msg)
            elif isinstance(msg, Election):
                self.election.on_election(msg)
            elif isinstance(msg, Propose):
                self._on_propose(msg)
            elif isinstance(msg, Commit):
                self._on_commit(msg)
            elif isinstance(msg, Ack):
                if self.role == LEADING:
                    self.leader.on_ack(msg)
            elif isinstance(msg, Forward):
                self._route(msg.op, msg.value, msg.client_id or "?", entry_node=msg.sender_id)

    def _reject_stale(self, msg):
        self.send(msg.sender_id, Ack(sender_id=self.node_id, epoch=self.accepted_epoch, kind=AckKind.REJECT,
                                     accepted=False, reason=f"stale epoch {msg.epoch} < {self.accepted_epoch}"))

    def _on_heartbeat(self, msg):
        leader = msg.leader_id or msg.sender_id
        if msg.epoch < self.accepted_epoch:
            self._reject_stale(msg)
            return
        if self.role == LEADING:
            if msg.epoch == self.accepted_epoch:
                log.error("[%s] another leader %s in the same epoch %d, ignoring", self.node_id, leader, msg.epoch)
                return
            self.leader.step_down()
        if self.role != FOLLOWING or self.leader_id != leader or msg.epoch != self.accepted_epoch:
            self.role = FOLLOWING
            self.leader_id = leader
            self.synced = False
            self.set_epoch(msg.epoch)
            self.election.reset()
            log.info("[%s] following leader %s (epoch=%d), requesting log sync", self.node_id, leader, msg.epoch)
            self._send_join()
        self.reset_timer()

    def _send_join(self):
        epoch, seq = self.log.last_zxid()
        self.send(self.leader_id, Ack(sender_id=self.node_id, epoch=self.accepted_epoch, seq=seq, kind=AckKind.JOIN))
        self._join_sent_at = time.monotonic()

    def _from_current_leader(self, msg):
        if msg.epoch < self.accepted_epoch:
            self._reject_stale(msg)
            return False
        return self.role == FOLLOWING and msg.sender_id == self.leader_id and msg.epoch == self.accepted_epoch

    def _on_propose(self, msg):
        if not self._from_current_leader(msg):
            return
        if msg.op == Op.SYNC:
            self._apply_sync(msg.value)
            self.send(self.leader_id, Ack(sender_id=self.node_id, epoch=self.accepted_epoch, kind=AckKind.SYNC))
            return
        if not self.synced:
            return  # anything before the sync is already in the sync data
        entry = {"epoch": msg.epoch, "seq": msg.seq, "op": msg.op, "value": msg.value,
                 "client": msg.client, "status": PROPOSED}
        if self.log.append(entry):
            log.info("[%s] logged PROPOSE (%d,%d) %s %s, sending ACK", self.node_id, msg.epoch, msg.seq, msg.op, msg.value)
        self.send(msg.sender_id, Ack(sender_id=self.node_id, epoch=msg.epoch, seq=msg.seq))

    def _apply_sync(self, entries):
        before = self.state.current
        self.log.replace_all(entries)
        self.state.rebuild(self.log.committed_entries())
        first_sync = not self.synced
        self.synced = True
        committed = sum(1 for e in entries if e["status"] == COMMITTED)
        if first_sync:
            log.info("[%s] synchronised with leader %s: %d log entries (%d committed), current target %s",
                     self.node_id, self.leader_id, len(entries), committed, self.state.value)
            # new leader doesn't know our clients yet
            for cid in self.clients:
                self.send(self.leader_id, Forward(sender_id=self.node_id, epoch=self.accepted_epoch,
                                                  op=Op.JOIN, client_id=cid))
        if self.state.current is not None and self.state.current != before:
            self._notify_clients(self.state.current)

    def _on_commit(self, msg):
        if not self._from_current_leader(msg) or not self.synced:
            return
        entry = self.log.get(msg.epoch, msg.seq)
        if entry is None:
            log.warning("[%s] COMMIT (%d,%d) refers to a proposal missing from the local log, requesting resync",
                        self.node_id, msg.epoch, msg.seq)
            self.synced = False
            self._send_join()
            return
        self.apply_committed(self.log.commit_upto(msg.epoch, msg.seq))

    def _on_client(self, msg, conn):
        if not isinstance(msg, Forward):
            self._reply(conn, False, f"clients may only send FORWARD, got {msg.type.value}")
            return
        op, value, cid = (msg.op or "").upper(), msg.value, msg.client_id or msg.sender_id
        if op == Op.STATUS:
            self._reply(conn, True, result=self._status_locked())
        elif op == Op.READ:
            if self.state.current is None:
                self._reply(conn, False, "no target yet")
            else:
                self._reply(conn, True, result=self.state.current)
        elif op == Op.JOIN:
            old = self.clients.get(cid)
            if old is not None and old is not conn:
                self._conn_client.pop(old, None)
            self.clients[cid] = conn
            self._conn_client[conn] = cid
            log.info("[%s] client %s connected", self.node_id, cid)
            self._route(Op.JOIN, None, cid, entry_node=self.node_id)
            self._reply(conn, True, f"connected to {self.node_id}", result=self.state.current)
        elif op in (Op.WRITE, Op.GUESS):
            if not isinstance(value, int) or isinstance(value, bool) or not VALUE_MIN <= value <= VALUE_MAX:
                self._reply(conn, False, f"value must be an integer in {VALUE_MIN}-{VALUE_MAX}, got {value!r}")
            elif op == Op.GUESS and self.state.value is None:
                self._reply(conn, False, "no target yet, send a WRITE first")
            else:
                ok, reason = self._route(op, value, cid, entry_node=self.node_id)
                self._reply(conn, ok, reason)
        else:
            self._reply(conn, False, f"unknown operation {op!r}")

    def _reply(self, conn, ok, reason=None, result=None):
        self._to_client(conn, Ack(sender_id=self.node_id, epoch=self.accepted_epoch, kind=AckKind.CLIENT,
                                  accepted=ok, reason=reason, result=result))

    def _route(self, op, value, client, entry_node):
        # leader handles it directly, a follower forwards it and logs nothing yet
        if self.role == LEADING:
            self.leader.on_forward(op, value, client, entry_node)
            return True, f"submitted to leader {self.node_id}"
        if self.role == FOLLOWING and self.synced and self.leader_id and self.leader_id != entry_node:
            self.send(self.leader_id, Forward(sender_id=self.node_id, epoch=self.accepted_epoch,
                                              op=op, value=value, client_id=client))
            if op in (Op.WRITE, Op.GUESS):
                log.info("[%s] forwarding %s(%s) from %s to leader %s", self.node_id, op, value, client, self.leader_id)
            return True, f"forwarded by {self.node_id} to leader {self.leader_id}"
        return False, "no leader available right now (election or sync in progress), please retry"

    def _on_disconnect(self, conn):
        with self.lock:
            cid = self._conn_client.pop(conn, None)
            if cid is None or self._stopped.is_set():
                return
            if self.clients.get(cid) is conn:
                del self.clients[cid]
                log.info("[%s] client %s disconnected", self.node_id, cid)
                self._route(Op.LEAVE, None, cid, entry_node=self.node_id)


def main():
    parser = argparse.ArgumentParser(description="Start one node")
    parser.add_argument("node_id", choices=sorted(CLUSTER))
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("-v", "--verbose", action="store_true", help="show debug logs")
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s.%(msecs)03d %(message)s", datefmt="%H:%M:%S")
    node = Node(args.node_id, data_dir=args.data_dir).start()
    try:
        threading.Event().wait()
    except KeyboardInterrupt:
        node.stop()


if __name__ == "__main__":
    main()
