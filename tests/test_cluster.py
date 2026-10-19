"""Test scenarios 1 (normal write) and 2 (guessing)."""

import time
import unittest

from helpers import ClusterTestCase


class TestWriteAndGuess(ClusterTestCase):
    def test_initial_leader_is_lowest_id(self):
        self.start()
        self.assertEqual(self.wait_stable(), "n1")  # all logs empty -> equal (epoch, seq) -> lowest ID wins
        self.assertEqual(self.nodes["n1"].status()["epoch"], 1)

    def test_write_flow(self):
        self.start()
        self.wait_stable()
        alice, bob, carol = self.client("alice", "n2"), self.client("bob", "n3"), self.client("carol", "n1")
        ack = alice.write(50)
        self.assertTrue(ack.accepted, ack.reason)
        for c in (alice, bob, carol):  # every client (whichever node it uses) sees the new target
            self.assertEqual(c.wait_target(50)["client"], "alice")
        log = self.wait_logs_equal()
        self.assertEqual([(e["epoch"], e["seq"], e["op"], e["value"]) for e in log], [(1, 1, "WRITE", 50)])
        self.assertEqual(bob.read(), 50)

    def test_invalid_requests_rejected(self):
        self.start()
        self.wait_stable()
        alice = self.client("alice", "n2")
        self.assertFalse(alice.guess(10).accepted)   # no target yet
        self.assertFalse(alice.write(101).accepted)  # outside 0-100
        self.assertIsNone(alice.read())

    def test_guess_round_closes_when_all_guessed(self):
        self.start()
        self.wait_stable()
        alice, bob, carol = self.client("alice", "n2"), self.client("bob", "n3"), self.client("carol", "n4")
        alice.write(50)
        alice.wait_target(50)
        bob.guess(30)
        carol.guess(62)  # alice set the target, so nobody waits for her -> settles as soon as both have guessed
        for c in (alice, bob, carol):
            self.assertEqual(c.wait_target(62, timeout=0.8)["client"], "carol")
        # next round: carol is now the target's author, wait for alice and bob
        alice.guess(70)
        bob.guess(60)
        self.assertEqual(carol.wait_target(60, timeout=0.8)["client"], "bob")
        log = self.wait_logs_equal()
        self.assertEqual([(e["op"], e["value"]) for e in log], [("WRITE", 50), ("GUESS", 62), ("GUESS", 60)])

    def test_guess_round_closes_on_timeout(self):
        self.start()
        self.wait_stable()
        alice, bob, carol = self.client("alice", "n2"), self.client("bob", "n3"), self.client("carol", "n4")
        alice.write(50)
        alice.wait_target(50)
        t0 = time.time()
        bob.guess(45)  # carol doesn't guess -> settles after round_timeout (1 s in tests)
        alice.wait_target(45, timeout=3)
        self.assertGreaterEqual(time.time() - t0, 0.9)

    def test_write_discards_open_round(self):
        self.start()
        self.wait_stable()
        alice, bob, carol = self.client("alice", "n2"), self.client("bob", "n3"), self.client("carol", "n4")
        alice.write(50)
        alice.wait_target(50)
        bob.guess(10)
        carol.write(90)            # new target, this round's guesses are void
        alice.wait_target(90)
        time.sleep(1.5)            # past round_timeout, 10 must not win
        self.assertEqual(alice.read(), 90)


if __name__ == "__main__":
    unittest.main()
