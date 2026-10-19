import unittest

from node_message import GuessRound, TargetState, closest


class TestGameRules(unittest.TestCase):
    def test_doc_example(self):
        # lab document: target = 50, guesses = {30, 62} -> closest = 62
        self.assertEqual(closest(50, [("bob", 30), ("carol", 62)]), ("carol", 62))

    def test_tie_goes_to_earliest(self):
        self.assertEqual(closest(80, [("bob", 75), ("carol", 85)]), ("bob", 75))
        self.assertEqual(closest(80, [("carol", 85), ("bob", 75)]), ("carol", 85))

    def test_round(self):
        r = GuessRound()
        self.assertIsNone(r.started_at)
        r.add("bob", 30, now=10.0)
        r.add("carol", 62, now=11.0)
        r.add("bob", 49, now=12.0)  # guessing again: the last guess counts
        self.assertEqual(r.started_at, 10.0)
        self.assertEqual(r.winner(50), ("bob", 49))
        self.assertFalse(r.is_complete({"bob", "carol", "dave"}))
        self.assertTrue(r.is_complete({"bob", "carol"}))
        self.assertFalse(r.is_complete(set()))  # with nobody to wait for, only the timeout settles the round
        r.next()
        self.assertEqual((r.guesses, r.started_at, r.number), ({}, None, 2))

    def test_target_state(self):
        s = TargetState()
        s.rebuild([{"op": "WRITE", "value": 50, "client": "alice"}, {"op": "GUESS", "value": 62, "client": "carol"}])
        self.assertEqual((s.value, s.author), (62, "carol"))
        with self.assertRaises(ValueError):
            closest(1, [])


if __name__ == "__main__":
    unittest.main()
