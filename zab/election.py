import logging
import time

from .config import FOLLOWING, LEADING, LOOKING, node_num
from .messages import Election

log = logging.getLogger("zab")


# a vote is (candidate, (epoch, seq)): highest (epoch, seq) wins, then lowest node id
def vote_key(vote): # spec 2
    candidate, (epoch, seq) = vote
    return epoch, seq, -node_num(candidate) # spec 2


# all methods are called with node.lock held
class ElectionManager:
    def __init__(self, node):
        self.node = node
        self.round = 0
        self.vote = None
        self.votes = {}
        self.majority_since = None
        self.last_broadcast = 0.0

    def _self_vote(self):
        return self.node.node_id, self.node.log.last_zxid()

    def start_looking(self, reason): #spec 1
        n = self.node
        if n.role == LEADING:
            n.leader.step_down()
        n.role = LOOKING
        n.leader_id = None
        n.synced = False
        # round > accepted epoch, so the winner's new epoch is always higher. not max() with
        # the old round: after a restart or a false timeout it can be stale and skip an epoch
        self.round = n.accepted_epoch + 1
        self.vote = self._self_vote()
        self.votes = {n.node_id: self.vote}
        self.majority_since = None
        epoch, seq = self.vote[1]
        log.info("[%s] %s -> LOOKING, election round %d, voting for itself (epoch=%d, seq=%d)",
                 n.node_id, reason, self.round, epoch, seq)
        self._broadcast() #spec 1

    def reset(self):
        self.votes = {}
        self.vote = None
        self.majority_since = None

    def _broadcast(self, to=None):
        candidate, (epoch, seq) = self.vote
        msg = Election(sender_id=self.node.node_id, epoch=epoch, seq=seq, candidate_id=candidate, round=self.round)
        if to is None:
            self.node.broadcast(msg)
            self.last_broadcast = time.monotonic()
        else:
            self.node.send(to, msg)

    def on_election(self, msg):
        n = self.node
        if n.role != LOOKING:
            return  # we have a leader, the sender will join it via heartbeats
        theirs = (msg.candidate_id or msg.sender_id, (msg.epoch, msg.seq))
        if msg.round > self.round:
            self.round = msg.round
            self.vote = max(self._self_vote(), theirs, key=vote_key)
            self.votes = {n.node_id: self.vote, msg.sender_id: theirs}
            self.majority_since = None
            self._broadcast()
        elif msg.round < self.round:
            self._broadcast(to=msg.sender_id)  # help the sender catch up
            return
        else:
            self.votes[msg.sender_id] = theirs
            if vote_key(theirs) > vote_key(self.vote):
                self.vote = theirs
                self.votes[n.node_id] = theirs
                self.majority_since = None
                log.info("[%s] switching vote to %s ((epoch, seq)=(%d, %d) is newer, or equal with a lower ID)",
                         n.node_id, theirs[0], *theirs[1])
                self._broadcast()
        self._check(time.monotonic())

    def tick(self, now):
        if now - self.last_broadcast >= self.node.timing.heartbeat_interval:
            self._broadcast()
        self._check(now)

    def _check(self, now):
        support = sum(1 for v in self.votes.values() if v == self.vote)
        if support < self.node.quorum:
            self.majority_since = None
            return
        if self.majority_since is None:
            self.majority_since = now
        elif now - self.majority_since >= self.node.timing.finalize_wait:
            self._decide(support)

    def _decide(self, support):
        n = self.node
        candidate = self.vote[0]
        log.info("[%s] election round %d finished: %s won %d/%d votes", n.node_id, self.round, candidate, support, len(n.cluster))
        epoch = self.round
        self.reset()
        if candidate == n.node_id:
            n.leader.become_leader(epoch)
        else:
            n.role = FOLLOWING  # leader_id is set by the first heartbeat
            n.reset_timer()
