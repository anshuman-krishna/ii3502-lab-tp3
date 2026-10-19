from .config import CLUSTER, DEFAULT_TIMING, FAST_TIMING, FOLLOWING, LEADING, LOOKING, Timing
from .framing import MessageBuffer, iter_messages, send_message
from .game import GuessRound, TargetState, closest
from .messages import (
    Ack,
    AckKind,
    Commit,
    Election,
    Forward,
    Heartbeat,
    Message,
    MessageDecodeError,
    MessageType,
    Op,
    Propose,
)


def __getattr__(name):
    # node / client / logstore are also entry points run with `python -m`; importing them lazily here
    # avoids the runpy warning "found in sys.modules ... prior to execution"
    if name == "Node":
        from .node import Node
        return Node
    if name in ("Client", "node_status"):
        from . import client
        return getattr(client, name)
    if name in ("LogStore", "PROPOSED", "COMMITTED", "format_table"):
        from . import logstore
        return getattr(logstore, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "MessageType", "MessageDecodeError", "Message", "Op", "AckKind",
    "Propose", "Ack", "Commit", "Heartbeat", "Election", "Forward",
    "MessageBuffer", "send_message", "iter_messages",
    "CLUSTER", "Timing", "DEFAULT_TIMING", "FAST_TIMING", "LOOKING", "FOLLOWING", "LEADING",
    "LogStore", "PROPOSED", "COMMITTED", "format_table",
    "GuessRound", "TargetState", "closest",
    "Node", "Client", "node_status",
]
