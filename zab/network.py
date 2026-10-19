import logging
import queue
import socket
import threading
import time

from .messages import DELIMITER, Election, Heartbeat, Message, MessageDecodeError

log = logging.getLogger("zab")

MAX_LINE = 1024 * 1024  # 1mb cap, in case a peer never sends a newline
CONNECT_TIMEOUT = 0.5
SEND_TIMEOUT = 2.0


# tcp is a byte stream: one recv() can hold half a message or several
class MessageBuffer:
    def __init__(self, max_line=MAX_LINE):
        self._buf = bytearray()
        self._max_line = max_line

    def feed(self, chunk):
        self._buf.extend(chunk)
        messages = []
        while True:
            idx = self._buf.find(DELIMITER)
            if idx < 0:
                break
            line = bytes(self._buf[:idx])
            del self._buf[: idx + 1]
            if line.strip():
                messages.append(Message.decode(line))
        if len(self._buf) > self._max_line:
            self._buf.clear()
            raise MessageDecodeError(f"message exceeds {self._max_line} bytes without a newline")
        return messages

    @property
    def pending(self):
        return len(self._buf)


def send_message(sock, msg):
    sock.sendall(msg.encode())


def iter_messages(sock, bufsize=4096):
    buffer = MessageBuffer()
    while True:
        chunk = sock.recv(bufsize)
        if not chunk:
            return
        yield from buffer.feed(chunk)


# outbound connection to one node: send() only queues, a sender thread per peer does the io
class Peer:
    def __init__(self, owner_id, peer_id, addr, backoff):
        self.owner_id = owner_id
        self.peer_id = peer_id
        self.addr = addr
        self.backoff = backoff
        self._queue = queue.Queue()
        self._thread = threading.Thread(target=self._run, name=f"{owner_id}->{peer_id}", daemon=True)
        self._thread.start()

    def send(self, msg):
        self._queue.put(msg)

    def close(self):
        self._queue.put(None)

    def _run(self):
        sock = None
        down_until = 0.0
        while True:
            msg = self._queue.get()
            if msg is None:
                break
            wait = down_until - time.monotonic()
            if wait > 0:
                # peer was just down: drop heartbeats/votes (resent anyway), retry the rest
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


# accept thread + one reader thread per incoming connection
class NodeServer:
    def __init__(self, name, addr, on_message, on_disconnect):
        self.name = name
        self.addr = addr
        self._on_message = on_message
        self._on_disconnect = on_disconnect
        self._sock = None
        self._conns = set()
        self._lock = threading.Lock()
        self._stopped = threading.Event()

    def start(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(self.addr)
        sock.listen()
        sock.settimeout(0.5)  # so the accept loop can see stop()
        self._sock = sock
        threading.Thread(target=self._accept_loop, name=f"{self.name}-accept", daemon=True).start()

    def stop(self):
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

    def _accept_loop(self):
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

    def _serve(self, conn):
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
