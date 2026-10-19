"""
Node: one ZooKeeper-like node. Wires the components together and implements the follower-side
and client-side logic.

    NodeServer         (server.py)       accepts incoming connections, dispatches messages
    Peer               (transport.py)    outbound connections to other nodes (queue + sender thread each)
    LogStore           (logstore.py)     persistent log (epoch, seq, op, value, status)
    ElectionManager    (election.py)     heartbeat-timeout detection, elections
    LeaderCoordinator  (coordinator.py)  assigns (epoch, seq), PROPOSE, collects ACKs, COMMIT, sync, guess rounds

Roles: LOOKING (electing) / FOLLOWING / LEADING.

Follower side:
    HEARTBEAT  stale epoch -> reply ACK(REJECT) so the old leader steps down; otherwise follow that
               leader, sending ACK(JOIN) the first time to ask for a log sync
    PROPOSE    op=SYNC: replace the local log with the leader's, reply ACK(SYNC);
               normal proposal: log as PROPOSED, reply ACK(epoch, seq). Ignored until synchronised
    COMMIT     mark (epoch, seq) and all earlier entries COMMITTED, update the target and push it
               to this node's clients; if the entry is missing (a message was lost) -> ask for a resync

Client side (FORWARD):
    JOIN online / READ current target / STATUS node status -- answered by this node directly
    WRITE(x) / GUESS(x) -- followers forward them to the leader (and log nothing until the PROPOSE arrives)
    Every time a new target is committed, the node pushes the COMMIT to all clients connected to it

Thread safety: all state is read and written under self.lock; while holding it we only enqueue
messages and write to the local sqlite. Messages to nodes are sent by the Peer threads; messages
to clients are sent, in order, by one dedicated thread.
"""

from __future__ import annotations

import argparse
import logging
import os
import queue
import random
import socket
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

from .config import CLUSTER, DEFAULT_TIMING, FOLLOWING, LEADING, LOOKING, VALUE_MAX, VALUE_MIN, Address, Timing, quorum
from .coordinator import LeaderCoordinator
from .election import ElectionManager
from .framing import send_message
from .game import TargetState
from .logstore import COMMITTED, PROPOSED, Entry, LogStore
from .messages import Ack, AckKind, Commit, Election, Forward, Heartbeat, Message, Op, Propose
from .server import NodeServer
from .transport import Peer

log = logging.getLogger("node_message")


class Node:
    def __init__(self, node_id: str, cluster: Optional[Dict[str, Address]] = None,
                 data_dir: str = "data", timing: Timing = DEFAULT_TIMING) -> None:
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
        self.state.rebuild(self.log.committed_entries())     # after a restart, recover the target from the log

        self.role = LOOKING
        self.accepted_epoch = self.log.accepted_epoch()
        self.leader_id: Optional[str] = None
        self.synced = False                                   # follower: synchronised with the current leader?
        self.last_heartbeat = time.monotonic()
        self.election_timeout = self._random_timeout()
        self._join_sent_at = 0.0

        self.clients: Dict[str, socket.socket] = {}           # clients connected to this node
        self._conn_client: Dict[socket.socket, str] = {}
        self._client_out: "queue.Queue[Optional[Tuple[socket.socket, Message]]]" = queue.Queue()

        self.peers: Dict[str, Peer] = {}
        self.election = ElectionManager(self)
        self.leader = LeaderCoordinator(self)
        self.server = NodeServer(node_id, self.cluster[node_id], self._dispatch, self._on_disconnect)
        self._stopped = threading.Event()

    # ===================== lifecycle =====================

    def start(self) -> "Node":
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

    def stop(self) -> None:
        with self.lock:
            self._stopped.set()
        self.server.stop()
        for peer in self.peers.values():
            peer.close()
        self._client_out.put(None)
        with self.lock:
            self.log.close()

    def status(self) -> Dict[str, Any]:
        with self.lock:
            return self._status_locked()

    def _status_locked(self) -> Dict[str, Any]:
        epoch, seq = self.log.last_zxid()
        return {
            "node_id": self.node_id, "role": self.role, "epoch": self.accepted_epoch,
            "leader_id": self.leader_id, "last_zxid": [epoch, seq],
            "ready": self.leader.active if self.role == LEADING else (self.role == FOLLOWING and self.synced),
            "target": self.state.value, "clients": sorted(self.clients),
        }

    # ===================== sending =====================

    def send(self, peer_id: str, msg: Message) -> None:
        peer = self.peers.get(peer_id)
        if peer is not None:
            peer.send(msg)

    def broadcast(self, msg: Message) -> None:
        for peer in self.peers.values():
            peer.send(msg)

    def _to_client(self, conn: socket.socket, msg: Message) -> None:
        self._client_out.put((conn, msg))

    def _client_sender(self) -> None:
        while True:
            item = self._client_out.get()
            if item is None:
                return
            conn, msg = item
            try:
                send_message(conn, msg)
            except OSError:
                pass  # client is gone; its reader thread will clean up

    # ===================== state helpers (call with the lock held) =====================

    def _random_timeout(self) -> float:
        return random.uniform(*self.timing.election_timeout)

    def reset_timer(self) -> None:
        self.last_heartbeat = time.monotonic()
        self.election_timeout = self._random_timeout()

    def set_epoch(self, epoch: int) -> None:
        if epoch != self.accepted_epoch:
            self.accepted_epoch = epoch
            self.log.set_accepted_epoch(epoch)   # persisted: stale leaders are still recognised after a restart

    def apply_committed(self, entries: List[Entry]) -> None:
        """Apply newly committed entries: update the target and push it to this node's clients."""
        for e in entries:
            self.state.apply(e)
            log.info("[%s] committed (%d,%d) %s %d (%s) -> current target %s",
                     self.node_id, e["epoch"], e["seq"], e["op"], e["value"], e["client"], self.state.value)
            self._notify_clients(e)
            if self.role == LEADING:
                self.leader.on_committed(e)

    def _notify_clients(self, entry: Entry) -> None:
        msg = Commit(sender_id=self.node_id, epoch=entry["epoch"], seq=entry["seq"],  # type: ignore[arg-type]
                     op=entry["op"], value=entry["value"], client=entry["client"])  # type: ignore[arg-type]
        for conn in self.clients.values():
            self._to_client(conn, msg)

    # ===================== timer =====================

    def _timer_loop(self) -> None:
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
                        self._send_join()   # sync request or sync data was lost: resend
                else:
                    self.election.tick(now)

    # ===================== dispatch =====================

    def _dispatch(self, msg: Message, conn: socket.socket) -> None:
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

    # ===================== follower side =====================

    def _reject_stale(self, msg: Message) -> None:
        self.send(msg.sender_id, Ack(sender_id=self.node_id, epoch=self.accepted_epoch, kind=AckKind.REJECT,
                                     accepted=False, reason=f"stale epoch {msg.epoch} < {self.accepted_epoch}"))

    def _on_heartbeat(self, msg: Heartbeat) -> None:
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

    def _send_join(self) -> None:
        epoch, seq = self.log.last_zxid()
        assert self.leader_id is not None
        self.send(self.leader_id, Ack(sender_id=self.node_id, epoch=self.accepted_epoch, seq=seq, kind=AckKind.JOIN))
        self._join_sent_at = time.monotonic()

    def _from_current_leader(self, msg: Message) -> bool:
        if msg.epoch < self.accepted_epoch:
            self._reject_stale(msg)
            return False
        return self.role == FOLLOWING and msg.sender_id == self.leader_id and msg.epoch == self.accepted_epoch

    def _on_propose(self, msg: Propose) -> None:
        if not self._from_current_leader(msg):
            return
        if msg.op == Op.SYNC:
            self._apply_sync(msg.value)
            self.send(self.leader_id, Ack(sender_id=self.node_id, epoch=self.accepted_epoch, kind=AckKind.SYNC))  # type: ignore[arg-type]
            return
        if not self.synced:
            return  # proposals sent before the sync are included in the sync data
        entry: Entry = {"epoch": msg.epoch, "seq": msg.seq, "op": msg.op, "value": msg.value,
                        "client": msg.client, "status": PROPOSED}
        if self.log.append(entry):
            log.info("[%s] logged PROPOSE (%d,%d) %s %s, sending ACK", self.node_id, msg.epoch, msg.seq, msg.op, msg.value)
        self.send(msg.sender_id, Ack(sender_id=self.node_id, epoch=msg.epoch, seq=msg.seq))

    def _apply_sync(self, entries: List[Entry]) -> None:
        before = self.state.current
        self.log.replace_all(entries)
        self.state.rebuild(self.log.committed_entries())
        first_sync = not self.synced
        self.synced = True
        committed = sum(1 for e in entries if e["status"] == COMMITTED)
        if first_sync:
            log.info("[%s] synchronised with leader %s: %d log entries (%d committed), current target %s",
                     self.node_id, self.leader_id, len(entries), committed, self.state.value)
            # the new leader does not know this node's clients yet: register them again
            for cid in self.clients:
                self.send(self.leader_id, Forward(sender_id=self.node_id, epoch=self.accepted_epoch,  # type: ignore[arg-type]
                                                  op=Op.JOIN, client_id=cid))
        if self.state.current is not None and self.state.current != before:
            self._notify_clients(self.state.current)

    def _on_commit(self, msg: Commit) -> None:
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

    # ===================== client side =====================

    def _on_client(self, msg: Message, conn: socket.socket) -> None:
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

    def _reply(self, conn: socket.socket, ok: bool, reason: Optional[str] = None, result: Any = None) -> None:
        self._to_client(conn, Ack(sender_id=self.node_id, epoch=self.accepted_epoch, kind=AckKind.CLIENT,
                                  accepted=ok, reason=reason, result=result))

    def _route(self, op: str, value: Any, client: str, entry_node: str) -> Tuple[bool, str]:
        """Hand a client request to the leader: handle it directly if we are the leader, otherwise forward it."""
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

    def _on_disconnect(self, conn: socket.socket) -> None:
        with self.lock:
            cid = self._conn_client.pop(conn, None)
            if cid is None or self._stopped.is_set():
                return
            if self.clients.get(cid) is conn:
                del self.clients[cid]
                log.info("[%s] client %s disconnected", self.node_id, cid)
                self._route(Op.LEAVE, None, cid, entry_node=self.node_id)


def main() -> None:
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
