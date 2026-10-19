"""Fixed topology and timing parameters."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

Address = Tuple[str, int]

# 5 nodes, each one a separate process listening on its own localhost port
CLUSTER: Dict[str, Address] = {f"n{i}": ("127.0.0.1", 5000 + i) for i in range(1, 6)}

VALUE_MIN, VALUE_MAX = 0, 100          # allowed range for WRITE / GUESS

# node roles (ZooKeeper terminology)
LOOKING, FOLLOWING, LEADING = "LOOKING", "FOLLOWING", "LEADING"


def quorum(cluster_size: int) -> int:
    """Majority: 5 nodes -> 3 (the leader counts as 1 vote)."""
    return cluster_size // 2 + 1


def node_num(node_id: str) -> int:
    """'n3' -> 3, used for "lowest ID wins" comparisons."""
    return int(node_id.lstrip("n"))


@dataclass(frozen=True)
class Timing:
    heartbeat_interval: float = 0.3        # interval between leader heartbeats
    election_timeout: Tuple[float, float] = (1.0, 2.0)  # follower heartbeat timeout (randomised to avoid simultaneous elections)
    finalize_wait: float = 0.4             # once a candidate has a majority, wait this long for a better candidate
    ack_timeout: float = 1.0               # timeout for ACKs / sync confirmations; resend after it
    round_timeout: float = 5.0             # max duration of a guess round, counted from its first guess
    tick: float = 0.05                     # timer thread check interval

    def __post_init__(self) -> None:
        # The send backoff equals the heartbeat interval; the election timeout must be well above it,
        # otherwise a node that just came back would wrongly conclude the leader is gone.
        if self.heartbeat_interval * 3 > self.election_timeout[0]:
            raise ValueError("election timeout lower bound must be at least 3x the heartbeat interval")


DEFAULT_TIMING = Timing()
# For tests: all durations shortened, same behaviour
FAST_TIMING = Timing(heartbeat_interval=0.05, election_timeout=(0.3, 0.6), finalize_wait=0.1,
                     ack_timeout=0.3, round_timeout=1.0, tick=0.02)
