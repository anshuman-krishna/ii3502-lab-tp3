"""
LeaderCoordinator: active only on the leader.

    - Taking over: epoch = election round, seq restarts at 0; send the full log to every follower
      (PROPOSE op=SYNC). Once a majority (including the leader) has confirmed the sync, the
      PROPOSED entries left by the previous leader are committed and the leader starts serving
      (active). Client requests received before that are queued and handled on activation.
    - Writes: assign (epoch, seq) -> log as PROPOSED -> broadcast PROPOSE -> once ACKs reach the
      quorum (3 votes including the leader) -> log as COMMITTED -> broadcast COMMIT. Commits happen
      in seq order; on ACK timeout the PROPOSE is resent to followers that have not acknowledged.
    - Guesses: collect the round's guesses; once the round is settled, the guess closest to the
      target goes through the full write flow as a GUESS.
    - A follower joining (ACK kind=JOIN, e.g. after a restart) is sent the full log again.
"""

from __future__ import annotations

import logging
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Dict, List, Set, Tuple

from .config import LEADING
from .game import GuessRound
from .logstore import PROPOSED, Entry
from .messages import Ack, AckKind, Commit, Heartbeat, Op, Propose

if TYPE_CHECKING:
    from .node import Node

log = logging.getLogger("node_message")


class LeaderCoordinator:
    def __init__(self, node: "Node") -> None:
        self.node = node
        self._reset()

    def _reset(self) -> None:
        self.epoch = 0
        self.active = False
        self.next_seq = 0
        self.pending: "OrderedDict[int, Dict[str, Any]]" = OrderedDict()  # seq -> {entry, acks, sent_at}
        self.synced: Set[str] = set()                 # followers synchronised with this leader
        self.sync_sent: Dict[str, float] = {}         # follower -> when SYNC was last sent
        self.queued: List[Tuple[str, Any, str, str]] = []   # requests received before activation
        self.active_clients: Dict[str, str] = {}      # online client -> node it is connected to
        self.round = GuessRound()
        self.next_heartbeat = 0.0

    # All methods below are called with node.lock held

    # ---------- taking over / stepping down ----------

    def become_leader(self, epoch: int) -> None:
        n = self.node
        self._reset()
        self.epoch = epoch
        n.role = LEADING
        n.leader_id = n.node_id
        n.synced = True
        n.set_epoch(epoch)
        self.active_clients = {cid: n.node_id for cid in n.clients}
        log.info("[%s] * became leader, epoch=%d (seq reset), synchronising logs", n.node_id, epoch)
        now = time.monotonic()
        self._heartbeat(now)
        for peer in n.peers:
            self._send_sync(peer, now)
        self._try_activate()

    def step_down(self) -> None:
        if self.epoch:
            log.info("[%s] stepping down as leader (epoch=%d)", self.node.node_id, self.epoch)
        self._reset()

    def _heartbeat(self, now: float) -> None:
        n = self.node
        n.broadcast(Heartbeat(sender_id=n.node_id, epoch=self.epoch, leader_id=n.node_id))
        self.next_heartbeat = now + n.timing.heartbeat_interval

    # ---------- synchronisation ----------

    def _send_sync(self, follower: str, now: float) -> None:
        n = self.node
        entries = n.log.entries()
        n.send(follower, Propose(sender_id=n.node_id, epoch=self.epoch, op=Op.SYNC, value=entries))
        self.sync_sent[follower] = now
        log.debug("[%s] sending log sync to %s (%d entries)", n.node_id, follower, len(entries))

    def _try_activate(self) -> None:
        n = self.node
        if self.active or len(self.synced) + 1 < n.quorum:
            return
        # A majority now holds the leader's full log: the previous leader's PROPOSED entries can be committed safely
        epoch, seq = n.log.last_zxid()
        leftovers = n.log.commit_upto(epoch, seq)
        if leftovers:
            log.info("[%s] committing %d uncommitted entries left by the previous leader", n.node_id, len(leftovers))
            n.apply_committed(leftovers)
            now = time.monotonic()
            for follower in self.synced:
                self._send_sync(follower, now)   # so followers also see them as committed
        self.active = True
        log.info("[%s] * %d/%d nodes synchronised, leader is now serving (epoch=%d)",
                 n.node_id, len(self.synced) + 1, len(n.cluster), self.epoch)
        queued, self.queued = self.queued, []
        for args in queued:
            self.on_forward(*args)

    # ---------- message handling ----------

    def on_ack(self, msg: Ack) -> None:
        n = self.node
        if msg.kind == AckKind.REJECT and msg.epoch > self.epoch:
            log.warning("[%s] %s reports a newer epoch %d, stepping down", n.node_id, msg.sender_id, msg.epoch)
            n.set_epoch(msg.epoch)
            n.election.start_looking("found a higher epoch")
            return
        if msg.epoch != self.epoch:
            return
        if msg.kind == AckKind.JOIN:
            self.synced.discard(msg.sender_id)
            self._send_sync(msg.sender_id, time.monotonic())
        elif msg.kind == AckKind.SYNC:
            if msg.sender_id not in self.synced:
                log.info("[%s] %s finished log sync", n.node_id, msg.sender_id)
            self.synced.add(msg.sender_id)
            self.sync_sent.pop(msg.sender_id, None)
            self._try_activate()
        elif msg.kind == AckKind.PROPOSAL:
            p = self.pending.get(msg.seq)
            if p is None or msg.sender_id not in self.synced:
                return
            p["acks"].add(msg.sender_id)
            log.info("[%s] ACK from %s for (%d,%d) (%d/%d votes)",
                     n.node_id, msg.sender_id, self.epoch, msg.seq, len(p["acks"]) + 1, n.quorum)
            self._commit_ready()

    def on_forward(self, op: str, value: Any, client: str, entry_node: str) -> None:
        n = self.node
        if op == Op.JOIN:
            self.active_clients[client] = entry_node
            return
        if op == Op.LEAVE:
            self.active_clients.pop(client, None)
            self.round.guesses.pop(client, None)
            self._maybe_close_round()
            return
        if not self.active:
            log.info("[%s] leader still synchronising, queueing %s(%s)", n.node_id, op, value)
            self.queued.append((op, value, client, entry_node))
            return
        if op == Op.WRITE:
            log.info("[%s] WRITE(%s) from %s (via %s)", n.node_id, value, client, entry_node)
            self.propose(Op.WRITE, value, client)
        elif op == Op.GUESS:
            if n.state.value is None:
                log.warning("[%s] no target yet, ignoring guess from %s", n.node_id, client)
                return
            self.round.add(client, value, time.monotonic())
            log.info("[%s] round %d: guess %s from %s (via %s), %d guess(es) so far, waiting for %s",
                     n.node_id, self.round.number, value, client, entry_node, len(self.round.guesses),
                     sorted(self._expected_guessers() - set(self.round.guesses)) or "nobody")
            self._maybe_close_round()

    # ---------- guess rounds ----------

    def _expected_guessers(self) -> Set[str]:
        return set(self.active_clients) - {self.node.state.author}

    def _maybe_close_round(self, now: float = 0.0) -> None:
        if not self.active or not self.round.guesses:
            return
        if self.round.is_complete(self._expected_guessers()):
            self._close_round("every client has guessed")
        elif now and now - (self.round.started_at or now) >= self.node.timing.round_timeout:
            self._close_round(f"{self.node.timing.round_timeout:g} s timeout")

    def _close_round(self, why: str) -> None:
        n = self.node
        target = n.state.value
        assert target is not None
        client, value = self.round.winner(target)
        log.info("[%s] round %d settled (%s): target %d, guesses %s -> closest is %d from %s",
                 n.node_id, self.round.number, why, target, dict(self.round.guesses), value, client)
        self.round.next()
        self.propose(Op.GUESS, value, client)

    # ---------- write flow ----------

    def _propose_msg(self, entry: Entry) -> Propose:
        return Propose(sender_id=self.node.node_id, epoch=self.epoch, seq=entry["seq"],  # type: ignore[arg-type]
                       op=entry["op"], value=entry["value"], client=entry["client"])  # type: ignore[arg-type]

    def propose(self, op: str, value: int, client: str) -> None:
        n = self.node
        self.next_seq += 1
        entry: Entry = {"epoch": self.epoch, "seq": self.next_seq, "op": op, "value": value,
                        "client": client, "status": PROPOSED}
        n.log.append(entry)
        self.pending[self.next_seq] = {"entry": entry, "acks": set(), "sent_at": time.monotonic()}
        n.broadcast(self._propose_msg(entry))
        log.info("[%s] PROPOSE (%d,%d) %s %d", n.node_id, self.epoch, self.next_seq, op, value)
        self._commit_ready()

    def _commit_ready(self) -> None:
        """Commit, in seq order, every proposal that has reached the quorum (the leader counts as 1 vote)."""
        n = self.node
        while self.pending:
            seq, p = next(iter(self.pending.items()))
            if len(p["acks"]) + 1 < n.quorum:
                break
            self.pending.popitem(last=False)
            e = p["entry"]
            log.info("[%s] (%d,%d) has %d/%d votes, quorum reached -> broadcasting COMMIT",
                     n.node_id, self.epoch, seq, len(p["acks"]) + 1, len(n.cluster))
            n.broadcast(Commit(sender_id=n.node_id, epoch=self.epoch, seq=seq,
                               op=e["op"], value=e["value"], client=e["client"]))
            n.apply_committed(n.log.commit_upto(self.epoch, seq))

    def on_committed(self, entry: Entry) -> None:
        """After the leader applies a commit: a new WRITE changed the target, so the open round's guesses are void."""
        if entry["op"] == Op.WRITE and self.round.guesses:
            log.info("[%s] target changed to %s by WRITE, discarding this round's guesses %s",
                     self.node.node_id, entry["value"], dict(self.round.guesses))
            self.round.clear()

    # ---------- timers ----------

    def tick(self, now: float) -> None:
        n = self.node
        if now >= self.next_heartbeat:
            self._heartbeat(now)
        for follower, sent_at in list(self.sync_sent.items()):
            if follower not in self.synced and now - sent_at > n.timing.ack_timeout:
                self._send_sync(follower, now)
        for seq, p in self.pending.items():
            if now - p["sent_at"] > n.timing.ack_timeout:
                missing = self.synced - p["acks"]
                log.warning("[%s] (%d,%d) ACK timeout (%d/%d votes), resending to %s",
                            n.node_id, self.epoch, seq, len(p["acks"]) + 1, n.quorum, sorted(missing) or "nobody")
                for follower in missing:
                    n.send(follower, self._propose_msg(p["entry"]))
                p["sent_at"] = now
        self._maybe_close_round(now)
