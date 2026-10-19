"""
ElectionManager: heartbeat-timeout detection + election (ZAB style: the node with the highest
(epoch, seq) wins; ties are broken by the lowest node ID).

Flow:
    1. On startup, or when a follower hears no leader heartbeat for election_timeout -> LOOKING.
       round = max(round, accepted_epoch) + 1; the node first votes for itself:
       ELECTION(epoch, seq = its own latest log entry, candidate = itself).
    2. On a vote from the same round: if the sender's candidate is better ((epoch, seq) higher,
       or equal with a lower ID), switch to that candidate and rebroadcast. Every LOOKING node
       also rebroadcasts its vote once per heartbeat interval.
       A vote from a higher round -> move to that round; from a lower round -> send our vote back
       so the sender can catch up.
    3. Once the candidate we support has a majority (3 of 5, including ourselves) and no better
       candidate appears within finalize_wait -> the election ends:
         - if the candidate is ourselves -> become leader with epoch = round (seq restarts at 0)
           and start log synchronisation;
         - otherwise -> become a follower and wait for the candidate's heartbeat (re-elect on timeout).
    4. Nodes that already have a leader (FOLLOWING / LEADING) do not vote; a node still LOOKING
       joins a leader directly when it receives that leader's heartbeat.

Why new epoch = round is safe: winning requires a majority, and every voter's round is at least
its accepted epoch + 1. Any epoch that was ever established was accepted by some majority; two
majorities always intersect, so the new epoch is strictly greater.
"""

from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Dict, Optional, Tuple

from .config import FOLLOWING, LEADING, LOOKING, node_num
from .messages import Election

if TYPE_CHECKING:
    from .node import Node

log = logging.getLogger("node_message")

Vote = Tuple[str, Tuple[int, int]]   # (candidate, candidate's latest (epoch, seq))


def vote_key(vote: Vote) -> Tuple[int, int, int]:
    candidate, (epoch, seq) = vote
    return epoch, seq, -node_num(candidate)


class ElectionManager:
    def __init__(self, node: "Node") -> None:
        self.node = node
        self.round = 0
        self.vote: Optional[Vote] = None
        self.votes: Dict[str, Vote] = {}
        self.majority_since: Optional[float] = None
        self.last_broadcast = 0.0

    # All methods below are called with node.lock held

    def _self_vote(self) -> Vote:
        return self.node.node_id, self.node.log.last_zxid()

    def start_looking(self, reason: str) -> None:
        n = self.node
        if n.role == LEADING:
            n.leader.step_down()
        n.role = LOOKING
        n.leader_id = None
        n.synced = False
        self.round = max(self.round, n.accepted_epoch) + 1
        self.vote = self._self_vote()
        self.votes = {n.node_id: self.vote}
        self.majority_since = None
        epoch, seq = self.vote[1]
        log.info("[%s] %s -> LOOKING, election round %d, voting for itself (epoch=%d, seq=%d)",
                 n.node_id, reason, self.round, epoch, seq)
        self._broadcast()

    def reset(self) -> None:
        self.votes = {}
        self.vote = None
        self.majority_since = None

    def _broadcast(self, to: Optional[str] = None) -> None:
        assert self.vote is not None
        candidate, (epoch, seq) = self.vote
        msg = Election(sender_id=self.node.node_id, epoch=epoch, seq=seq, candidate_id=candidate, round=self.round)
        if to is None:
            self.node.broadcast(msg)
            self.last_broadcast = time.monotonic()
        else:
            self.node.send(to, msg)

    def on_election(self, msg: Election) -> None:
        n = self.node
        if n.role != LOOKING:
            return  # we already have a leader: don't vote; the sender will join via that leader's heartbeat
        theirs: Vote = (msg.candidate_id or msg.sender_id, (msg.epoch, msg.seq))
        if msg.round > self.round:
            self.round = msg.round
            self.vote = max(self._self_vote(), theirs, key=vote_key)
            self.votes = {n.node_id: self.vote, msg.sender_id: theirs}
            self.majority_since = None
            self._broadcast()
        elif msg.round < self.round:
            self._broadcast(to=msg.sender_id)
            return
        else:
            self.votes[msg.sender_id] = theirs
            assert self.vote is not None
            if vote_key(theirs) > vote_key(self.vote):
                self.vote = theirs
                self.votes[n.node_id] = theirs
                self.majority_since = None
                log.info("[%s] switching vote to %s ((epoch, seq)=(%d, %d) is newer, or equal with a lower ID)",
                         n.node_id, theirs[0], *theirs[1])
                self._broadcast()
        self._check(time.monotonic())

    def tick(self, now: float) -> None:
        if now - self.last_broadcast >= self.node.timing.heartbeat_interval:
            self._broadcast()
        self._check(now)

    def _check(self, now: float) -> None:
        support = sum(1 for v in self.votes.values() if v == self.vote)
        if support < self.node.quorum:
            self.majority_since = None
            return
        if self.majority_since is None:
            self.majority_since = now
        elif now - self.majority_since >= self.node.timing.finalize_wait:
            self._decide(support)

    def _decide(self, support: int) -> None:
        n = self.node
        assert self.vote is not None
        candidate = self.vote[0]
        log.info("[%s] election round %d finished: %s won %d/%d votes", n.node_id, self.round, candidate, support, len(n.cluster))
        epoch = self.round
        self.reset()
        if candidate == n.node_id:
            n.leader.become_leader(epoch)
        else:
            n.role = FOLLOWING          # leader_id stays empty until the new leader's heartbeat; re-elect on timeout
            n.reset_timer()
