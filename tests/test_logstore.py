import os
import tempfile
import unittest

from node_message import COMMITTED, PROPOSED, LogStore


def entry(epoch, seq, value=0, status=PROPOSED, op="WRITE"):
    return {"epoch": epoch, "seq": seq, "op": op, "value": value, "client": "c", "status": status}


class TestLogStore(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "n1.db")
        self.store = LogStore(self.path)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_append_is_idempotent_and_ordered(self):
        self.assertEqual(self.store.last_zxid(), (0, 0))
        self.assertTrue(self.store.append(entry(2, 1, 20)))
        self.assertTrue(self.store.append(entry(1, 2, 12)))
        self.assertFalse(self.store.append(entry(1, 2, 99)))  # a resent PROPOSE
        self.assertEqual([(e["epoch"], e["seq"]) for e in self.store.entries()], [(1, 2), (2, 1)])
        self.assertEqual(self.store.get(1, 2)["value"], 12)
        self.assertEqual(self.store.last_zxid(), (2, 1))

    def test_commit_upto(self):
        for e in [entry(1, 1), entry(1, 2), entry(2, 1), entry(2, 2)]:
            self.store.append(e)
        newly = self.store.commit_upto(2, 1)
        self.assertEqual([(e["epoch"], e["seq"], e["status"]) for e in newly],
                         [(1, 1, COMMITTED), (1, 2, COMMITTED), (2, 1, COMMITTED)])
        self.assertEqual(self.store.commit_upto(2, 1), [])
        self.assertEqual(self.store.get(2, 2)["status"], PROPOSED)
        self.assertEqual(self.store.last_committed()["seq"], 1)

    def test_replace_all_and_persistence(self):
        self.store.append(entry(1, 1))
        self.store.replace_all([entry(1, 1, 5, COMMITTED), entry(2, 1, 7)])
        self.store.set_accepted_epoch(3)
        self.store.close()
        self.store = LogStore(self.path)  # simulate a restart
        self.assertEqual([(e["value"], e["status"]) for e in self.store.entries()], [(5, COMMITTED), (7, PROPOSED)])
        self.assertEqual(self.store.accepted_epoch(), 3)


if __name__ == "__main__":
    unittest.main()
