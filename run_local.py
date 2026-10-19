# starts the 5 nodes as separate processes and runs the 3 lab scenarios
#   python3 run_local.py                # all 3 scenarios, prints pass / fail
#   python3 run_local.py --scenario 2   # one scenario
#   python3 run_local.py -i             # interactive admin mode, run clients in other terminals

import argparse
import glob
import os
import shlex
import socket
import subprocess
import sys
import time

from zab.client import Client, node_status
from zab.config import CLUSTER, LEADING
from zab.logstore import LogStore, format_table

ROOT = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(ROOT, "data")
LOG_DIR = os.path.join(ROOT, "logs")


def say(text=""):
    print(text, flush=True)


def banner(text):
    say(f"\n{'=' * 64}\n  {text}\n{'=' * 64}")


class Check:
    def __init__(self):
        self.failures = []

    def __call__(self, ok, what):
        say(f"    [{'OK' if ok else 'FAIL'}] {what}")
        if not ok:
            self.failures.append(what)
        return ok


class LocalCluster:
    def __init__(self):
        self.procs = {}
        self._logs = []

    def start(self, nid):
        if nid in self.procs and self.procs[nid].poll() is None:
            say(f"  {nid} is already running")
            return
        log = open(os.path.join(LOG_DIR, f"{nid}.log"), "a", encoding="utf-8")
        self._logs.append(log)
        self.procs[nid] = subprocess.Popen(
            [sys.executable, "-u", "-m", "zab.node", nid, "--data-dir", DATA_DIR],
            cwd=ROOT, stdout=log, stderr=subprocess.STDOUT,
        )
        wait_port(CLUSTER[nid])
        say(f"  started {nid} (pid {self.procs[nid].pid}, port {CLUSTER[nid][1]})")

    def kill(self, nid):
        proc = self.procs.get(nid)
        if proc is None or proc.poll() is not None:
            say(f"  {nid} is not running")
            return
        proc.kill()  # sigkill, simulates a crash
        proc.wait()
        say(f"  kill -9 {nid} (pid {proc.pid})")

    def stop_all(self):
        for proc in self.procs.values():
            if proc.poll() is None:
                proc.terminate()
        for proc in self.procs.values():
            try:
                proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                proc.kill()
        for f in self._logs:
            f.close()

    def alive(self):
        return [nid for nid, p in self.procs.items() if p.poll() is None]

    def statuses(self):
        out = {}
        for nid in CLUSTER:
            if nid not in self.alive():
                out[nid] = None
                continue
            try:
                out[nid] = node_status(nid)
            except OSError:
                out[nid] = None
        return out

    def wait_stable(self, timeout=15.0):
        # one ready leader, every live node follows it and is synced
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            live = [s for s in self.statuses().values() if s]
            leaders = [s for s in live if s["role"] == LEADING and s["ready"]]
            if len(leaders) == 1 and all(s["ready"] and s["leader_id"] == leaders[0]["node_id"] for s in live):
                return leaders[0]["node_id"]
            time.sleep(0.1)
        raise TimeoutError(f"cluster did not stabilise within {timeout}s: {self.statuses()}")

    def print_status(self):
        for nid, s in self.statuses().items():
            if s is None:
                say(f"  {nid}: stopped")
            else:
                say(f"  {nid}: {s['role']:<9} epoch={s['epoch']}  leader={s['leader_id']}  "
                    f"latest(epoch,seq)={tuple(s['last_zxid'])}  target={s['target']}  clients={s['clients']}")


def read_log(nid):
    path = os.path.join(DATA_DIR, f"{nid}.db")
    if not os.path.exists(path):
        return []
    store = LogStore(path, readonly=True)
    try:
        return store.entries()
    finally:
        store.close()


def print_logs(node_ids):
    for nid in node_ids:
        say(f"  [{nid}]")
        say(format_table(read_log(nid)))


def logs_synced(node_ids, timeout=5.0):
    # identical logs on all these nodes, everything committed
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        logs = [read_log(n) for n in node_ids]
        if all(l == logs[0] for l in logs) and all(e["status"] == "COMMITTED" for e in logs[0]):
            return True
        time.sleep(0.1)
    return False


def wait_port(addr, timeout=5.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            socket.create_connection(addr, timeout=0.2).close()
            return
        except OSError:
            time.sleep(0.05)
    raise TimeoutError(f"{addr} did not start listening within {timeout}s")


def check_ports_free():
    busy = []
    for nid, addr in CLUSTER.items():
        try:
            socket.create_connection(addr, timeout=0.2).close()
            busy.append(f"{nid}={addr[1]}")
        except OSError:
            pass
    if busy:
        sys.exit(f"ports already in use (nodes from a previous run may still be alive): {', '.join(busy)}\n"
                 f"stop them with: pkill -f zab.node")


def reset_dirs():
    os.makedirs(DATA_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    for path in glob.glob(os.path.join(DATA_DIR, "n*.db*")) + glob.glob(os.path.join(LOG_DIR, "n*.log")):
        os.remove(path)


class Demo:
    def __init__(self, cluster):
        self.cluster = cluster
        self.check = Check()
        self.clients = {}

    def client(self, name, node):
        c = Client(name, node_id=node, on_event=lambda text, n=name: say(f"      [{n} received] {text}")).connect()
        self.clients[name] = c
        say(f"  client {name} connected to {c.node_id}")
        return c

    def close(self):
        for c in self.clients.values():
            c.close()

    def setup(self):
        leader = self.cluster.wait_stable()
        say(f"\n  cluster ready: leader = {leader}")
        self.cluster.print_status()
        # dave sits on the leader so scenario 3 shows a client reconnect
        for name, node in (("alice", "n2"), ("bob", "n3"), ("carol", "n4"), ("dave", leader)):
            self.client(name, node)
        time.sleep(0.3)  # let the leader register the clients
        return leader

    def all_see(self, value, timeout=8.0):
        ok = True
        for c in self.clients.values():
            try:
                c.wait_target(value, timeout=timeout)
            except TimeoutError:
                ok = False
        return self.check(ok, f"all clients {sorted(self.clients)} see target = {value}")

    def submit(self, name, op, value):
        c = self.clients[name]
        ack = (c.write if op == "write" else c.guess)(value)
        say(f"  {name} -> {c.node_id}: {op.upper()}({value})  [{ack.reason}]")
        self.check(ack.accepted, f"{op.upper()}({value}) from {name} accepted")

    def scenario_write(self):
        banner("Scenario 1: normal write (Write -> PROPOSE -> ACK -> COMMIT)")
        self.submit("alice", "write", 50)
        self.all_see(50)
        alive = self.cluster.alive()
        self.check(logs_synced(alive), f"logs of all {len(alive)} nodes are identical, entry (epoch, seq) is COMMITTED")
        print_logs(alive[:2])
        say("  (the other nodes are identical, see logs/n*.log for the full message flow)")

    def scenario_guess(self):
        banner("Scenario 2: guessing (clients guess -> leader picks closest -> new target -> next round)")
        if self.clients["alice"].target is None:
            say("  setup: alice sets the target to 50")
            self.submit("alice", "write", 50)
            self.all_see(50)
        target = self.clients["alice"].target["value"]

        say(f"\n  Round 1 (target {target}): bob guesses 30, carol 62, dave does not guess -> settled by the 5 s timeout")
        t0 = time.monotonic()
        self.submit("bob", "guess", 30)
        self.submit("carol", "guess", 62)
        self.all_see(62)
        self.check(time.monotonic() - t0 >= 4.5, f"waited {time.monotonic() - t0:.1f}s (timeout settlement)")

        say("\n  Round 2 (target 62, its author carol need not guess): alice 70, bob 60, dave 66 -> settled once all have guessed")
        t0 = time.monotonic()
        for name, v in (("alice", 70), ("bob", 60), ("dave", 66)):
            self.submit(name, "guess", v)
        self.all_see(60)
        self.check(time.monotonic() - t0 < 3, f"took {time.monotonic() - t0:.1f}s (settled as soon as everyone guessed)")

        alive = self.cluster.alive()
        self.check(logs_synced(alive), "logs of all nodes are identical")
        print_logs(alive[:1])

    def scenario_crash(self):
        banner("Scenario 3: leader crash and recovery")
        if self.clients["alice"].target is None:
            say("  setup: alice sets the target to 50")
            self.submit("alice", "write", 50)
            self.all_see(50)
        old = self.cluster.wait_stable()
        old_epoch = node_status(old)["epoch"]
        say(f"\n  current leader = {old} (epoch {old_epoch}), killing it with kill -9:")
        t0 = time.monotonic()
        self.cluster.kill(old)
        new = self.cluster.wait_stable()
        took = time.monotonic() - t0
        new_epoch = node_status(new)["epoch"]
        say(f"\n  after {took:.1f}s: new leader = {new}, epoch {old_epoch} -> {new_epoch}")
        self.cluster.print_status()
        self.check(new != old and new_epoch == old_epoch + 1, "new leader elected, epoch + 1")
        alive = self.cluster.alive()
        self.check(logs_synced(alive), f"logs of live nodes {alive} are synchronised")

        say("\n  dave was connected to the old leader and should have reconnected automatically:")
        deadline = time.monotonic() + 5
        while self.clients["dave"].node_id == old and time.monotonic() < deadline:
            time.sleep(0.1)
        self.check(self.clients["dave"].node_id != old, f"dave reconnected to {self.clients['dave'].node_id}")
        time.sleep(0.3)

        say("\n  operations resume: alice writes 80, then a round with bob 75 / carol 85 / dave 90"
            " (75 and 85 are equally close, bob guessed first so he wins)")
        self.submit("alice", "write", 80)
        self.all_see(80)
        for name, v in (("bob", 75), ("carol", 85), ("dave", 90)):
            self.submit(name, "guess", v)
        self.all_see(75)
        log = read_log(new)
        new_entries = [e for e in log if e["epoch"] == new_epoch]
        self.check([e["seq"] for e in new_entries] == list(range(1, len(new_entries) + 1)),
                   f"seq restarts at 1 in the new epoch {new_epoch}")

        say(f"\n  restarting the old leader {old}: it should see the newer epoch, follow {new} and catch up its log")
        self.cluster.start(old)
        self.check(self.cluster.wait_stable() == new, f"{old} did not take back leadership after restarting")
        self.check(logs_synced(list(CLUSTER)), "logs of all 5 nodes are identical")
        print_logs([new, old])


HELP = (
    "commands:\n"
    "  status          role / epoch / leader / latest (epoch,seq) / target of every node\n"
    "  logs [id]       print a node's persistent log (all nodes by default)\n"
    "  kill [id]       kill -9 a node (the current leader by default)\n"
    "  start <id>      restart a node\n"
    "  help / q        help / quit (stops all nodes)\n"
    "run clients in other terminals: python3 -m zab.client --name alice --node n2"
)


def run_interactive(cluster):
    leader = cluster.wait_stable()
    say(f"\ncluster ready, leader = {leader}\n{HELP}")
    while True:
        try:
            line = input("\nadmin> ").strip()
        except (EOFError, KeyboardInterrupt):
            say()
            return
        if not line:
            continue
        cmd, *args = shlex.split(line)
        try:
            if cmd in ("q", "quit", "exit"):
                return
            elif cmd == "help":
                say(HELP)
            elif cmd == "status":
                cluster.print_status()
            elif cmd == "logs":
                print_logs(args or list(CLUSTER))
            elif cmd == "kill":
                target = args[0] if args else next(
                    (n for n, s in cluster.statuses().items() if s and s["role"] == LEADING), None)
                if target is None:
                    say("  there is no leader right now")
                else:
                    cluster.kill(target)
            elif cmd == "start":
                cluster.start(args[0])
            else:
                say(f"  unknown command {cmd!r}, type help")
        except (IndexError, KeyError) as exc:
            say(f"  bad argument: {exc}")
        except (TimeoutError, OSError) as exc:
            say(f"  failed: {exc}")


def main():
    parser = argparse.ArgumentParser(description="Start 5 node processes and demonstrate the 3 test scenarios")
    parser.add_argument("-i", "--interactive", action="store_true", help="interactive mode")
    parser.add_argument("--scenario", type=int, choices=(1, 2, 3), help="run a single scenario")
    args = parser.parse_args()

    check_ports_free()
    reset_dirs()
    cluster = LocalCluster()
    say("starting 5 node processes (node logs in logs/, persistent logs in data/)")
    demo = None
    try:
        for nid in CLUSTER:
            cluster.start(nid)
        if args.interactive:
            run_interactive(cluster)
            return
        demo = Demo(cluster)
        demo.setup()
        scenarios = {1: demo.scenario_write, 2: demo.scenario_guess, 3: demo.scenario_crash}
        for n in ([args.scenario] if args.scenario else [1, 2, 3]):
            scenarios[n]()
        banner("PASS" if not demo.check.failures else "FAIL: " + "; ".join(demo.check.failures))
        sys.exit(0 if not demo.check.failures else 1)
    finally:
        if demo is not None:
            demo.close()
        cluster.stop_all()


if __name__ == "__main__":
    main()
