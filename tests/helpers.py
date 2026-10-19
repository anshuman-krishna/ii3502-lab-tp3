"""In-process cluster for tests: random free ports, temporary data directory, shortened timings."""

import os
import socket
import tempfile
import time
import unittest
from typing import Dict, List, Optional

from node_message import FAST_TIMING, LEADING, Client, LogStore, Node


def free_cluster(n: int = 5) -> Dict[str, tuple]:
    socks = [socket.socket() for _ in range(n)]
    for s in socks:
        s.bind(("127.0.0.1", 0))
    cluster = {f"n{i + 1}": s.getsockname() for i, s in enumerate(socks)}
    for s in socks:
        s.close()
    return cluster


class ClusterTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.cluster = free_cluster()
        self.nodes: Dict[str, Node] = {}
        self.clients: List[Client] = []

    def tearDown(self) -> None:
        for c in self.clients:
            c.close()
        for node in self.nodes.values():
            node.stop()
        self.tmp.cleanup()

    def start(self, *node_ids: str) -> None:
        for nid in node_ids or tuple(self.cluster):
            self.nodes[nid] = Node(nid, cluster=self.cluster, data_dir=self.tmp.name, timing=FAST_TIMING).start()

    def kill(self, nid: str) -> None:
        self.nodes.pop(nid).stop()

    def client(self, name: str, node_id: str) -> Client:
        c = Client(name, node_id=node_id, cluster=self.cluster).connect()
        self.clients.append(c)
        return c

    def wait_stable(self, timeout: float = 5.0) -> str:
        """Wait until the live nodes have exactly one active leader and all others follow it and are synced. Returns the leader id."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            statuses = [n.status() for n in self.nodes.values()]
            leaders = [s for s in statuses if s["role"] == LEADING and s["ready"]]
            if len(leaders) == 1 and all(s["ready"] and s["leader_id"] == leaders[0]["node_id"] for s in statuses):
                return leaders[0]["node_id"]
            time.sleep(0.02)
        self.fail(f"cluster did not stabilise: {[n.status() for n in self.nodes.values()]}")

    def log_of(self, nid: str) -> list:
        store = LogStore(os.path.join(self.tmp.name, f"{nid}.db"), readonly=True)
        try:
            return store.entries()
        finally:
            store.close()

    def wait_logs_equal(self, node_ids: Optional[List[str]] = None, timeout: float = 3.0) -> list:
        node_ids = node_ids or list(self.nodes)
        deadline = time.time() + timeout
        while time.time() < deadline:
            logs = [self.log_of(n) for n in node_ids]
            if all(l == logs[0] for l in logs) and all(e["status"] == "COMMITTED" for e in logs[0]):
                return logs[0]
            time.sleep(0.05)
        self.fail("logs did not converge:\n" + "\n".join(f"{n}: {self.log_of(n)}" for n in node_ids))
