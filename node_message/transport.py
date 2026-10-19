"""
Outbound connections between nodes.

One Peer per remote node: send() only puts the message on a queue, and a sender thread
dedicated to that peer sends messages in order. Calling send() while holding a lock therefore
never blocks, and one slow or dead peer cannot hold up the others.
"""

from __future__ import annotations

import logging
import queue
import socket
import threading
import time
from typing import Optional

from .config import Address
from .framing import send_message
from .messages import Election, Heartbeat, Message

CONNECT_TIMEOUT = 0.5
SEND_TIMEOUT = 2.0

log = logging.getLogger("node_message")


class Peer:
    def __init__(self, owner_id: str, peer_id: str, addr: Address, backoff: float) -> None:
        self.owner_id = owner_id
        self.peer_id = peer_id
        self.addr = addr
        self.backoff = backoff
        self._queue: "queue.Queue[Optional[Message]]" = queue.Queue()
        self._thread = threading.Thread(target=self._run, name=f"{owner_id}->{peer_id}", daemon=True)
        self._thread.start()

    def send(self, msg: Message) -> None:
        self._queue.put(msg)

    def close(self) -> None:
        self._queue.put(None)

    def _run(self) -> None:
        sock: Optional[socket.socket] = None
        down_until = 0.0
        while True:
            msg = self._queue.get()
            if msg is None:
                break
            wait = down_until - time.monotonic()
            if wait > 0:
                # The peer was unreachable a moment ago. Heartbeats and votes are resent periodically,
                # so drop them; PROPOSE / ACK / COMMIT must not be lost, so retry after the backoff
                # (this only blocks this peer's sender thread).
                if isinstance(msg, (Heartbeat, Election)):
                    continue
                time.sleep(wait)
            for attempt in (1, 2):
                try:
                    if sock is None:
                        sock = socket.create_connection(self.addr, timeout=CONNECT_TIMEOUT)
                        sock.settimeout(SEND_TIMEOUT)
                        if down_until:
                            log.info("[%s] reconnected to %s", self.owner_id, self.peer_id)
                            down_until = 0.0
                    send_message(sock, msg)
                    break
                except OSError as exc:
                    if sock is not None:
                        sock.close()
                        sock = None
                    if attempt == 2:
                        if not down_until:
                            log.warning("[%s] %s unreachable: %s", self.owner_id, self.peer_id, exc)
                        down_until = time.monotonic() + self.backoff
        if sock is not None:
            sock.close()
