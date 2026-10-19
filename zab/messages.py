import json
import time
import uuid
from dataclasses import asdict, dataclass, field, fields
from enum import Enum
from typing import Any, Optional

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
    WRITE = "WRITE"
    GUESS = "GUESS"
    SYNC = "SYNC"       # propose only: full log sent to a follower
    JOIN = "JOIN"       # forward only: client online
    LEAVE = "LEAVE"     # forward only: client offline
    READ = "READ"
    STATUS = "STATUS"


class AckKind:
    PROPOSAL = "PROPOSAL"
    JOIN = "JOIN"           # follower asks the new leader for a sync
    SYNC = "SYNC"           # follower replaced its log
    REJECT = "REJECT"       # stale leader, epoch field holds the newer epoch
    CLIENT = "CLIENT"       # reply to a client request


class MessageDecodeError(ValueError):
    pass


def new_msg_id():
    return uuid.uuid4().hex


@dataclass
class Message:
    TYPE = None

    sender_id: str
    epoch: int = 0
    msg_id: str = field(default_factory=new_msg_id)
    timestamp: float = field(default_factory=time.time)

    @property
    def type(self):
        return self.TYPE

    def to_dict(self):
        return {"type": self.TYPE.value, **asdict(self)}

    def to_json(self):
        # json.dumps escapes newlines, so "\n" is safe as a delimiter
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"))

    def encode(self):
        return self.to_json().encode(ENCODING) + DELIMITER

    @staticmethod
    def from_dict(data):
        if not isinstance(data, dict):
            raise MessageDecodeError(f"message must be a JSON object, got {type(data).__name__}")
        raw_type = data.get("type")
        try:
            msg_type = MessageType(raw_type)
        except ValueError:
            raise MessageDecodeError(f"unknown message type: {raw_type!r}") from None

        cls = MESSAGE_CLASSES[msg_type]
        allowed = {f.name for f in fields(cls)}
        kwargs = {k: v for k, v in data.items() if k in allowed}  # unknown fields are ignored
        try:
            return cls(**kwargs)
        except TypeError as exc:
            raise MessageDecodeError(f"bad {msg_type.value} fields: {exc}") from None

    @staticmethod
    def from_json(text):
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise MessageDecodeError(f"invalid JSON: {exc}") from None
        return Message.from_dict(data)

    @staticmethod
    def decode(raw):
        if isinstance(raw, bytes):
            try:
                raw = raw.decode(ENCODING)
            except UnicodeDecodeError as exc:
                raise MessageDecodeError(f"not UTF-8: {exc}") from None
        return Message.from_json(raw.strip())


@dataclass
class Propose(Message):
    TYPE = MessageType.PROPOSE

    seq: int = 0
    op: str = ""
    value: Any = None   # whole log when op is sync
    client: Optional[str] = None


@dataclass
class Ack(Message):
    TYPE = MessageType.ACK

    seq: int = 0
    kind: str = AckKind.PROPOSAL
    accepted: bool = True
    reason: Optional[str] = None
    result: Any = None


@dataclass
class Commit(Message):
    TYPE = MessageType.COMMIT

    seq: int = 0
    op: Optional[str] = None
    value: Any = None
    client: Optional[str] = None


@dataclass
class Heartbeat(Message):
    TYPE = MessageType.HEARTBEAT

    leader_id: Optional[str] = None


@dataclass
class Election(Message):
    TYPE = MessageType.ELECTION

    seq: int = 0
    candidate_id: Optional[str] = None
    round: int = 0

    def __post_init__(self):
        if self.candidate_id is None:
            self.candidate_id = self.sender_id


@dataclass
class Forward(Message):
    TYPE = MessageType.FORWARD

    op: str = ""
    value: Any = None
    client_id: Optional[str] = None


MESSAGE_CLASSES = {
    MessageType.PROPOSE: Propose,
    MessageType.ACK: Ack,
    MessageType.COMMIT: Commit,
    MessageType.HEARTBEAT: Heartbeat,
    MessageType.ELECTION: Election,
    MessageType.FORWARD: Forward,
}
