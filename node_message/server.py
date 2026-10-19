"""
NodeServer: accepts incoming connections and hands every received message to the node for dispatch.

The accept thread only accepts connections (with a timeout, so it can be stopped); each incoming
connection gets its own reader thread, so a blocking read on one connection never affects the
others, nor the node's timer thread.
"""

from __future__ import annotations

import logging
import socket
import threading
from typing import Callable, Optional, Set

from .config import Address
from .framing import iter_messages
from .messages import Message, MessageDecodeError

log = logging.getLogger("node_message")

OnMessage = Callable[[Message, socket.socket], None]
OnDisconnect = Callable[[socket.socket], None]


class NodeServer:
    def __init__(self, name: str, addr: Address, on_message: OnMessage, on_disconnect: OnDisconnect) -> None:
        self.name = name
        self.addr = addr
        self._on_message = on_message
        self._on_disconnect = on_disconnect
        self._sock: Optional[socket.socket] = None
        self._conns: Set[socket.socket] = set()
        self._lock = threading.Lock()
        self._stopped = threading.Event()

    def start(self) -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(self.addr)
        sock.listen()
        sock.settimeout(0.5)  # lets the accept loop check for stop regularly
        self._sock = sock
        threading.Thread(target=self._accept_loop, name=f"{self.name}-accept", daemon=True).start()

    def stop(self) -> None:
        self._stopped.set()
        if self._sock is not None:
            self._sock.close()
        with self._lock:
            conns = list(self._conns)
        for conn in conns:
            try:
                conn.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            conn.close()

    def _accept_loop(self) -> None:
        assert self._sock is not None
        while not self._stopped.is_set():
            try:
                conn, _ = self._sock.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            conn.settimeout(None)
            with self._lock:
                self._conns.add(conn)
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket) -> None:
        try:
            for msg in iter_messages(conn):
                self._on_message(msg, conn)
        except (OSError, MessageDecodeError) as exc:
            if not self._stopped.is_set():
                log.debug("[%s] connection closed: %s", self.name, exc)
        finally:
            with self._lock:
                self._conns.discard(conn)
            if not self._stopped.is_set():
                self._on_disconnect(conn)
            conn.close()
