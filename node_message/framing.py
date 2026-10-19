"""
Newline framing for a TCP stream (NDJSON).

TCP is a byte stream: a single recv() may return half a message, or several messages.
MessageBuffer accumulates the received bytes and splits complete messages on "\\n".
"""

from __future__ import annotations

import socket
from typing import Iterator, List

from .messages import DELIMITER, Message, MessageDecodeError

DEFAULT_MAX_LINE = 1024 * 1024  # max 1 MiB per message, so a peer that never sends "\n" cannot exhaust memory


class MessageBuffer:
    """Incremental decoder: feed() takes any byte chunk and returns the messages completed by it."""

    def __init__(self, max_line: int = DEFAULT_MAX_LINE) -> None:
        self._buf = bytearray()
        self._max_line = max_line

    def feed(self, chunk: bytes) -> List[Message]:
        self._buf.extend(chunk)
        messages: List[Message] = []
        while True:
            idx = self._buf.find(DELIMITER)
            if idx < 0:
                break
            line = bytes(self._buf[:idx])
            del self._buf[: idx + 1]
            if line.strip():  # skip empty lines
                messages.append(Message.decode(line))
        if len(self._buf) > self._max_line:
            self._buf.clear()
            raise MessageDecodeError(f"message exceeds {self._max_line} bytes without a newline")
        return messages

    @property
    def pending(self) -> int:
        """Number of buffered bytes not yet framed into a message."""
        return len(self._buf)


def send_message(sock: socket.socket, msg: Message) -> None:
    """Write one message to the socket."""
    sock.sendall(msg.encode())


def iter_messages(sock: socket.socket, bufsize: int = 4096) -> Iterator[Message]:
    """Read from the socket and yield messages one by one; stops when the peer closes the connection."""
    buffer = MessageBuffer()
    while True:
        chunk = sock.recv(bufsize)
        if not chunk:
            return
        yield from buffer.feed(chunk)
