"""
ClientApp: connects to any node, sends Write(x) / Guess(x), reads and live-displays the current target.

    python -m node_message.client --name alice --node n2

Interactive commands: write <x>, guess <x>, read, status, quit.
Whenever the cluster commits a new target, the connected node pushes a COMMIT and the client
prints it immediately. If the connected node dies, the client reconnects to another node.

Scripts and tests can use the Client class directly.
"""

from __future__ import annotations

import argparse
import queue
import socket
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from .config import CLUSTER, Address
from .framing import iter_messages, send_message
from .messages import Ack, Commit, Forward, Op

Entry = Dict[str, Any]


class Client:
    def __init__(self, name: str, node_id: Optional[str] = None, cluster: Optional[Dict[str, Address]] = None,
                 on_event: Optional[Callable[[str], None]] = None) -> None:
        self.name = name
        self.cluster = dict(cluster or CLUSTER)
        self.preferred = node_id or next(iter(self.cluster))
        self.node_id: Optional[str] = None            # node currently connected to
        self.target: Optional[Entry] = None           # latest target seen (a log entry)
        self.history: List[Entry] = []                # every commit notification received
        self._on_event = on_event or (lambda text: None)
        self._sock: Optional[socket.socket] = None
        self._replies: "queue.Queue[Ack]" = queue.Queue()
        self._request_lock = threading.Lock()
        self._cond = threading.Condition()
        self._closed = threading.Event()

    # ---------- connection ----------

    def connect(self, timeout: float = 5.0) -> "Client":
        deadline = time.monotonic() + timeout
        avoid = None
        while True:
            for nid in self._candidates(avoid):
                try:
                    self._connect_to(nid)
                    return self
                except OSError:
                    continue
            if time.monotonic() > deadline:
                raise ConnectionError(f"{self.name}: no node reachable")
            time.sleep(0.3)

    def _candidates(self, avoid: Optional[str]) -> List[str]:
        others = [n for n in self.cluster if n not in (self.preferred, avoid)]
        order = ([self.preferred] if self.preferred != avoid else []) + others
        return order + ([avoid] if avoid else [])

    def _connect_to(self, nid: str) -> None:
        sock = socket.create_connection(self.cluster[nid], timeout=1.0)
        sock.settimeout(None)
        self._sock, self.node_id = sock, nid
        threading.Thread(target=self._reader, args=(sock, nid), name=f"{self.name}-reader", daemon=True).start()
        ack = self._request(Op.JOIN, None, timeout=2.0)
        if ack.result:
            self._set_target(ack.result, announce=False)

    def close(self) -> None:
        self._closed.set()
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._sock.close()

    def _reader(self, sock: socket.socket, nid: str) -> None:
        try:
            for msg in iter_messages(sock):
                if isinstance(msg, Commit):
                    self._set_target({"epoch": msg.epoch, "seq": msg.seq, "op": msg.op,
                                      "value": msg.value, "client": msg.client}, announce=True)
                elif isinstance(msg, Ack):
                    self._replies.put(msg)
        except OSError:
            pass
        if self._closed.is_set() or sock is not self._sock:
            return
        self._on_event(f"lost connection to {nid}, reconnecting to another node...")
        while not self._closed.is_set():
            for other in self._candidates(avoid=nid):
                try:
                    self._connect_to(other)
                    self._on_event(f"reconnected to {other}")
                    return
                except OSError:
                    continue
            time.sleep(0.5)

    def _set_target(self, entry: Entry, announce: bool) -> None:
        with self._cond:
            changed = self.target is None or (entry["epoch"], entry["seq"]) != (self.target["epoch"], self.target["seq"])
            self.target = entry
            if changed and announce:
                self.history.append(entry)
            self._cond.notify_all()
        if changed and announce:
            how = "set by" if entry["op"] == Op.WRITE else "closest guess from"
            self._on_event(f"new target = {entry['value']} ({how} {entry['client']}, epoch={entry['epoch']}, seq={entry['seq']})")

    # ---------- requests ----------

    def _request(self, op: str, value: Any = None, timeout: float = 3.0) -> Ack:
        with self._request_lock:
            while not self._replies.empty():
                self._replies.get_nowait()
            if self._sock is None:
                raise ConnectionError("not connected")
            send_message(self._sock, Forward(sender_id=self.name, op=op, value=value, client_id=self.name))
            try:
                return self._replies.get(timeout=timeout)
            except queue.Empty:
                raise TimeoutError(f"{self.node_id} did not reply within {timeout}s") from None

    def write(self, value: int) -> Ack:
        return self._request(Op.WRITE, value)

    def guess(self, value: int) -> Ack:
        return self._request(Op.GUESS, value)

    def read(self) -> Optional[int]:
        ack = self._request(Op.READ)
        return ack.result["value"] if ack.accepted else None

    def status(self) -> Dict[str, Any]:
        return self._request(Op.STATUS).result

    def wait_target(self, value: int, timeout: float = 10.0) -> Entry:
        """Wait until the target (as pushed by the node) becomes value."""
        deadline = time.monotonic() + timeout
        with self._cond:
            while self.target is None or self.target["value"] != value:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"{self.name} did not see target {value} within {timeout}s (current: {self.target})")
                self._cond.wait(remaining)
            return self.target


def node_status(node_id: str, cluster: Optional[Dict[str, Address]] = None, timeout: float = 1.0) -> Dict[str, Any]:
    """Query a node's status (without registering as a client)."""
    addr = (cluster or CLUSTER)[node_id]
    with socket.create_connection(addr, timeout=timeout) as sock:
        send_message(sock, Forward(sender_id="admin", op=Op.STATUS, client_id="admin"))
        for reply in iter_messages(sock):
            if isinstance(reply, Ack):
                return reply.result
    raise ConnectionError(f"{node_id} did not reply")


HELP = "commands: write <0-100> | guess <0-100> | read | status | quit"


def main() -> None:
    parser = argparse.ArgumentParser(description="Number-guessing client")
    parser.add_argument("--name", required=True, help="client name, e.g. alice")
    parser.add_argument("--node", default="n2", choices=sorted(CLUSTER), help="node to connect to first")
    args = parser.parse_args()

    client = Client(args.name, node_id=args.node, on_event=lambda text: print(f"\n  [event] {text}\n> ", end="", flush=True))
    try:
        client.connect()
    except (ConnectionError, TimeoutError) as exc:
        raise SystemExit(f"connection failed: {exc}. Start the nodes first "
                         f"(python run_local.py -i, or python -m node_message.node n1 ...)")
    current = client.target["value"] if client.target else "(none yet)"
    print(f"{args.name} connected to {client.node_id}, current target: {current}\n{HELP}")
    try:
        while True:
            line = input("> ").strip().split()
            if not line:
                continue
            cmd = line[0].lower()
            try:
                if cmd in ("quit", "q", "exit"):
                    break
                elif cmd in ("write", "w", "guess", "g") and len(line) == 2:
                    ack = (client.write if cmd.startswith("w") else client.guess)(int(line[1]))
                    print(f"  {'OK' if ack.accepted else 'REJECTED'}: {ack.reason}")
                elif cmd in ("read", "r"):
                    value = client.read()
                    print(f"  current target: {value if value is not None else '(none yet)'}")
                elif cmd == "status":
                    s = client.status()
                    print(f"  {s['node_id']}: {s['role']}, epoch={s['epoch']}, leader={s['leader_id']}, "
                          f"latest (epoch,seq)={tuple(s['last_zxid'])}, target={s['target']}")
                else:
                    print(f"  {HELP}")
            except ValueError:
                print("  the value must be an integer")
            except (OSError, TimeoutError) as exc:
                print(f"  failed: {exc} (if reconnecting, try again in a moment)")
    except (EOFError, KeyboardInterrupt):
        print()
    finally:
        client.close()


if __name__ == "__main__":
    main()
