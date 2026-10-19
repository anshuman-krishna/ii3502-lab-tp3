from dataclasses import dataclass

# 5 nodes, one process per port on localhost
CLUSTER = {f"n{i}": ("127.0.0.1", 5000 + i) for i in range(1, 6)}

VALUE_MIN, VALUE_MAX = 0, 100

LOOKING, FOLLOWING, LEADING = "LOOKING", "FOLLOWING", "LEADING"


def quorum(cluster_size):
    # 5 nodes -> 3, the leader counts as 1 vote
    return cluster_size // 2 + 1


def node_num(node_id):
    return int(node_id.lstrip("n"))


@dataclass(frozen=True)
class Timing:
    heartbeat_interval: float = 0.3
    election_timeout: tuple = (1.0, 2.0)  # randomised so nodes don't all time out together
    finalize_wait: float = 0.4            # wait for a better candidate once we have a majority
    ack_timeout: float = 1.0              # resend propose / sync after this
    round_timeout: float = 5.0            # counted from the first guess of the round
    tick: float = 0.05

    def __post_init__(self):
        # send backoff = heartbeat interval, so the election timeout must be well above it
        if self.heartbeat_interval * 3 > self.election_timeout[0]:
            raise ValueError("election timeout lower bound must be at least 3x the heartbeat interval")


DEFAULT_TIMING = Timing()
# shorter timings for tests
FAST_TIMING = Timing(heartbeat_interval=0.05, election_timeout=(0.3, 0.6), finalize_wait=0.1,
                     ack_timeout=0.3, round_timeout=1.0, tick=0.02)
