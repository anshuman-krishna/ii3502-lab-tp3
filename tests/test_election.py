"""Test scenario 3 (leader crash and recovery), plus election rules and log sync."""

import time
import unittest

from helpers import ClusterTestCase

from node_message import LEADING


class TestElectionAndRecovery(ClusterTestCase):
    def test_heartbeat_keeps_leader(self):
        self.start()
        self.wait_stable()
        time.sleep(1.5)  # well past the election timeout (0.3-0.6 s in tests)
        self.assertTrue(all(n.status()["epoch"] == 1 and n.status()["leader_id"] == "n1" for n in self.nodes.values()))

    def test_leader_crash_and_recovery(self):
        self.start()
        self.wait_stable()
        alice, bob = self.client("alice", "n2"), self.client("bob", "n1")  # bob is connected to the leader
        alice.write(50)
        alice.wait_target(50)

        self.kill("n1")
        leader = self.wait_stable()
        self.assertEqual(leader, "n2")  # equal logs -> lowest live node ID
        self.assertEqual(self.nodes["n2"].status()["epoch"], 2)
        self.wait_logs_equal()

        # operations resume: bob reconnected to another node; seq restarts at 1 in the new epoch
        deadline = time.time() + 3
        while bob.node_id == "n1" and time.time() < deadline:
            time.sleep(0.05)
        self.assertNotEqual(bob.node_id, "n1")
        self.assertTrue(bob.write(77).accepted)
        alice.wait_target(77)
        log = self.wait_logs_equal()
        self.assertEqual([(e["epoch"], e["seq"], e["value"]) for e in log], [(1, 1, 50), (2, 1, 77)])

        # old leader restarts: sees the newer epoch, follows the new leader and catches up its log
        self.start("n1")
        self.assertEqual(self.wait_stable(), "n2")
        self.wait_logs_equal()

    def test_node_with_latest_log_wins(self):
        self.start()
        self.wait_stable()
        alice = self.client("alice", "n3")
        self.kill("n2")                      # n2 is down and misses the next write
        alice.write(10)
        alice.wait_target(10)
        self.kill("n1")                      # the leader dies too
        self.start("n2")                     # n2 comes back with latest (epoch, seq) = (0, 0), older than (1, 1) on n3/n4/n5
        leader = self.wait_stable()
        self.assertEqual(leader, "n3")       # not n2 (lowest ID) but n3 (lowest ID among the most up-to-date)
        log = self.wait_logs_equal()         # n2 has been caught up by the sync
        self.assertEqual([e["value"] for e in log], [10])

    def test_restart_whole_cluster(self):
        self.start()
        self.wait_stable()
        alice = self.client("alice", "n2")
        alice.write(50)
        alice.wait_target(50)
        self.kill("n1")                      # n1 falls behind
        self.wait_stable()
        alice.write(60)
        alice.wait_target(60)
        alice.close()
        for nid in list(self.nodes):         # kill everything
            self.kill(nid)
        self.start()                         # restart everything (n1 has the oldest log)
        leader = self.wait_stable()
        self.assertNotEqual(leader, "n1")
        self.assertGreater(self.nodes[leader].status()["epoch"], 2)  # epoch is persisted, it does not go back to 1
        log = self.wait_logs_equal()
        self.assertEqual([e["value"] for e in log], [50, 60])
        self.assertTrue(all(n.status()["target"] == 60 for n in self.nodes.values()))

    def test_no_leader_without_quorum(self):
        self.start("n1", "n2")
        time.sleep(1.0)
        self.assertFalse(any(n.status()["role"] == LEADING for n in self.nodes.values()))
        alice = self.client("alice", "n2")
        self.assertFalse(alice.write(5).accepted)
        self.start("n3")                     # 3 nodes are a quorum -> a leader is elected
        self.assertEqual(self.wait_stable(), "n1")


if __name__ == "__main__":
    unittest.main()
