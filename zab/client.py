import argparse
import queue
import socket
import threading
import time

from .config import CLUSTER
from .messages import Ack, Commit, Forward, Op
from .network import iter_messages, send_message


class Client:
    def __init__(self, name, node_id=None, cluster=None, on_event=None):
        self.name = name
        self.cluster = dict(cluster or CLUSTER)
        self.preferred = node_id or next(iter(self.cluster))
        self.node_id = None   # node we are connected to
        self.target = None    # latest target entry seen
        self.history = []     # every commit pushed to us
        self._on_event = on_event or (lambda text: None)
        self._sock = None
        self._replies = queue.Queue()
        self._request_lock = threading.Lock()
        self._cond = threading.Condition()
        self._closed = threading.Event()

    def connect(self, timeout=5.0):
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

    def _candidates(self, avoid):
        others = [n for n in self.cluster if n not in (self.preferred, avoid)]
        order = ([self.preferred] if self.preferred != avoid else []) + others
        return order + ([avoid] if avoid else [])

    def _connect_to(self, nid):
        sock = socket.create_connection(self.cluster[nid], timeout=1.0)
        sock.settimeout(None)
        self._sock, self.node_id = sock, nid
        threading.Thread(target=self._reader, args=(sock, nid), name=f"{self.name}-reader", daemon=True).start()
        ack = self._request(Op.JOIN, None, timeout=2.0)
        if ack.result:
            self._set_target(ack.result, announce=False)

    def close(self):
        self._closed.set()
        if self._sock is not None:
            try:
                self._sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._sock.close()

    def _reader(self, sock, nid):
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
        # our node died, try the others
        self._on_event(f"lost connection to {nid}, reconnecting to another node")
        while not self._closed.is_set():
            for other in self._candidates(avoid=nid):
                try:
                    self._connect_to(other)
                    self._on_event(f"reconnected to {other}")
                    return
                except OSError:
                    continue
            time.sleep(0.5)

    def _set_target(self, entry, announce):
        with self._cond:
            changed = self.target is None or (entry["epoch"], entry["seq"]) != (self.target["epoch"], self.target["seq"])
            self.target = entry
            if changed and announce:
                self.history.append(entry)
            self._cond.notify_all()
        if changed and announce:
            how = "set by" if entry["op"] == Op.WRITE else "closest guess from"
            self._on_event(f"new target = {entry['value']} ({how} {entry['client']}, epoch={entry['epoch']}, seq={entry['seq']})")

    def _request(self, op, value=None, timeout=3.0):
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

    def write(self, value):
        return self._request(Op.WRITE, value)

    def guess(self, value):
        return self._request(Op.GUESS, value)

    def read(self):
        ack = self._request(Op.READ)
        return ack.result["value"] if ack.accepted else None

    def status(self):
        return self._request(Op.STATUS).result

    def wait_target(self, value, timeout=10.0):
        deadline = time.monotonic() + timeout
        with self._cond:
            while self.target is None or self.target["value"] != value:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(f"{self.name} did not see target {value} within {timeout}s (current: {self.target})")
                self._cond.wait(remaining)
            return self.target


def node_status(node_id, cluster=None, timeout=1.0):
    # ask a node for its status without registering as a client
    addr = (cluster or CLUSTER)[node_id]
    with socket.create_connection(addr, timeout=timeout) as sock:
        send_message(sock, Forward(sender_id="admin", op=Op.STATUS, client_id="admin"))
        for reply in iter_messages(sock):
            if isinstance(reply, Ack):
                return reply.result
    raise ConnectionError(f"{node_id} did not reply")


HELP = "commands: write <0-100> | guess <0-100> | read | status | quit"


def main():
    parser = argparse.ArgumentParser(description="Number-guessing client")
    parser.add_argument("--name", required=True, help="client name, e.g. alice")
    parser.add_argument("--node", default="n2", choices=sorted(CLUSTER), help="node to connect to first")
    args = parser.parse_args()

    client = Client(args.name, node_id=args.node, on_event=lambda text: print(f"\n  [event] {text}\n> ", end="", flush=True))
    try:
        client.connect()
    except (ConnectionError, TimeoutError) as exc:
        raise SystemExit(f"connection failed: {exc}. Start the nodes first "
                         f"(python3 run_local.py -i, or python3 -m zab.node n1 ...)")
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
