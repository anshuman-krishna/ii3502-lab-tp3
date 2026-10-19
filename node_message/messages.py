"""
Message model between nodes, and between clients and nodes (message definitions and serialization only).

Wire format:
    each message = one JSON object + one newline "\\n", UTF-8 encoded.
    e.g. {"type":"ACK","sender_id":"n2","epoch":3,"seq":7,...}\\n

Fields shared by all messages (envelope):
    type       message type, see MessageType
    sender_id  sender ID (node n1..n5, or a client name) -- the "from" of the lab document
    epoch      leader generation (incremented each time a new leader is elected)
    msg_id     unique message ID (uuid4 hex, generated automatically)
    timestamp  send time, Unix seconds (float)

Mapping to the fields suggested by the lab document:
    PROPOSE   epoch, seq, op, value          (+ client: the client that proposed the value)
    ACK       epoch, seq, from               (+ kind: what is being acknowledged, see AckKind)
    COMMIT    epoch, seq                     (+ op, value, client: included when pushed to clients)
    HEARTBEAT epoch, from                    (+ leader_id)
    ELECTION  epoch, seq, from               (+ candidate_id, round)
    FORWARD   op, value, from client         (client_id)
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import asdict, dataclass, field, fields
from enum import Enum
from typing import Any, ClassVar, Dict, Optional, Type, Union

ENCODING = "utf-8"
DELIMITER = b"\n"


class MessageType(str, Enum):
    PROPOSE = "PROPOSE"
    ACK = "ACK"
    COMMIT = "COMMIT"
    HEARTBEAT = "HEARTBEAT"
    ELECTION = "ELECTION"
    FORWARD = "FORWARD"


class Op:
    """Operation types used in the log and in requests."""
    WRITE = "WRITE"     # set the target (written to the log)
    GUESS = "GUESS"     # a guess (sent by clients; the closest guess of a round is logged as GUESS)
    SYNC = "SYNC"       # PROPOSE only: the leader sends its full log to a follower (not logged)
    JOIN = "JOIN"       # FORWARD only: a client came online
    LEAVE = "LEAVE"     # FORWARD only: a client went offline
    READ = "READ"       # FORWARD only: read the current target (answered by the entry node)
    STATUS = "STATUS"   # FORWARD only: node status (answered by the entry node, used by test scripts)


class AckKind:
    PROPOSAL = "PROPOSAL"   # follower has logged proposal (epoch, seq)
    JOIN = "JOIN"           # follower accepted a new leader and asks to be synchronised
    SYNC = "SYNC"           # follower has replaced its log with the leader's
    REJECT = "REJECT"       # rejects a stale leader (the epoch field carries the newer epoch)
    CLIENT = "CLIENT"       # a node's reply to a client request (result carries the data)


class MessageDecodeError(ValueError):
    """Raised when received data cannot be parsed into a valid message."""


# type -> subclass registry, used to dispatch on deserialization
_REGISTRY: Dict[MessageType, Type["Message"]] = {}


def _new_msg_id() -> str:
    return uuid.uuid4().hex


@dataclass
class Message:
    """Base class of all messages. Subclasses only declare TYPE and their own fields."""

    TYPE: ClassVar[MessageType]

    sender_id: str
    epoch: int = 0
    msg_id: str = field(default_factory=_new_msg_id)
    timestamp: float = field(default_factory=time.time)

    # ---- automatic subclass registration ----
    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        msg_type = cls.__dict__.get("TYPE")
        if msg_type is not None:
            if msg_type in _REGISTRY:
                raise TypeError(f"message type registered twice: {msg_type}")
            _REGISTRY[msg_type] = cls

    @property
    def type(self) -> MessageType:
        return self.TYPE

    # ---- serialization ----
    def to_dict(self) -> Dict[str, Any]:
        data = asdict(self)
        return {"type": self.TYPE.value, **data}

    def to_json(self) -> str:
        # Compact form. json.dumps escapes newlines inside strings as \\n,
        # so the output never contains a real newline and "\n" is safe as a delimiter.
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"))

    def encode(self) -> bytes:
        """Encode to bytes ready to be written to a socket (with the trailing newline)."""
        return self.to_json().encode(ENCODING) + DELIMITER

    # ---- deserialization ----
    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Message":
        if not isinstance(data, dict):
            raise MessageDecodeError(f"message must be a JSON object, got {type(data).__name__}")
        raw_type = data.get("type")
        try:
            msg_type = MessageType(raw_type)
        except ValueError:
            raise MessageDecodeError(f"unknown message type: {raw_type!r}") from None

        cls = _REGISTRY[msg_type]
        allowed = {f.name for f in fields(cls)}
        kwargs = {k: v for k, v in data.items() if k in allowed}  # ignore unknown fields (forward compatibility)
        try:
            return cls(**kwargs)
        except TypeError as exc:  # missing required field, etc.
            raise MessageDecodeError(f"bad {msg_type.value} fields: {exc}") from None

    @staticmethod
    def from_json(text: str) -> "Message":
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise MessageDecodeError(f"invalid JSON: {exc}") from None
        return Message.from_dict(data)

    @staticmethod
    def decode(raw: Union[bytes, str]) -> "Message":
        """Decode a single message (with or without the trailing newline)."""
        if isinstance(raw, bytes):
            try:
                raw = raw.decode(ENCODING)
            except UnicodeDecodeError as exc:
                raise MessageDecodeError(f"not UTF-8: {exc}") from None
        return Message.from_json(raw.strip())


# ===================== The 6 message types =====================

@dataclass
class Propose(Message):
    """Leader -> follower: propose transaction (epoch, seq).
    With op=SYNC, value is the leader's full log (a list), used to synchronise a follower
    when a new leader takes over or a follower joins."""
    TYPE: ClassVar[MessageType] = MessageType.PROPOSE

    seq: int = 0
    op: str = ""
    value: Any = None
    client: Optional[str] = None


@dataclass
class Ack(Message):
    """Acknowledgement. kind: see AckKind; for PROPOSAL, (epoch, seq) is the acknowledged transaction."""
    TYPE: ClassVar[MessageType] = MessageType.ACK

    seq: int = 0
    kind: str = AckKind.PROPOSAL
    accepted: bool = True
    reason: Optional[str] = None
    result: Any = None


@dataclass
class Commit(Message):
    """Leader -> follower: commit (epoch, seq). Nodes include op / value / client when pushing it to clients."""
    TYPE: ClassVar[MessageType] = MessageType.COMMIT

    seq: int = 0
    op: Optional[str] = None
    value: Any = None
    client: Optional[str] = None


@dataclass
class Heartbeat(Message):
    """Periodic heartbeat sent by the leader."""
    TYPE: ClassVar[MessageType] = MessageType.HEARTBEAT

    leader_id: Optional[str] = None


@dataclass
class Election(Message):
    """Election vote: the sender currently supports candidate_id, whose latest log entry is (epoch, seq).
    round is the election round; once the election succeeds it becomes the new leader's epoch.
    candidate_id defaults to sender_id."""
    TYPE: ClassVar[MessageType] = MessageType.ELECTION

    seq: int = 0
    candidate_id: Optional[str] = None
    round: int = 0

    def __post_init__(self) -> None:
        if self.candidate_id is None:
            self.candidate_id = self.sender_id


@dataclass
class Forward(Message):
    """A client request, or a follower forwarding a client request to the leader. op: see Op."""
    TYPE: ClassVar[MessageType] = MessageType.FORWARD

    op: str = ""
    value: Any = None
    client_id: Optional[str] = None


__all__ = [
    "ENCODING",
    "DELIMITER",
    "MessageType",
    "Op",
    "AckKind",
    "MessageDecodeError",
    "Message",
    "Propose",
    "Ack",
    "Commit",
    "Heartbeat",
    "Election",
    "Forward",
]
