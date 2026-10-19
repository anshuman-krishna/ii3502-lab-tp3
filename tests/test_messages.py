import socket
import unittest

from node_message import (
    Ack,
    Commit,
    Election,
    Forward,
    Heartbeat,
    Message,
    MessageBuffer,
    MessageDecodeError,
    MessageType,
    Propose,
    iter_messages,
    send_message,
)

SAMPLES = [
    Propose(sender_id="n1", epoch=2, seq=5, op="WRITE", value=42, client="alice"),
    Ack(sender_id="n2", epoch=2, seq=5),
    Commit(sender_id="n1", epoch=2, seq=5, op="WRITE", value=42, client="alice"),
    Heartbeat(sender_id="n1", epoch=2, leader_id="n1"),
    Election(sender_id="n3", epoch=2, seq=5, round=3),
    Forward(sender_id="n2", op="GUESS", value=62, client_id="multi\nline"),
    Propose(sender_id="n1", epoch=2, op="SYNC", value=[{"epoch": 1, "seq": 1, "op": "WRITE", "value": 50,
                                                      "client": "a", "status": "COMMITTED"}]),
]


class TestMessages(unittest.TestCase):
    def test_round_trip_all_types(self):
        self.assertEqual({m.type for m in SAMPLES}, set(MessageType))
        for msg in SAMPLES:
            raw = msg.encode()
            self.assertTrue(raw.endswith(b"\n"))
            self.assertEqual(raw.count(b"\n"), 1)  # newlines inside values are escaped
            decoded = Message.decode(raw)
            self.assertIsInstance(decoded, type(msg))
            self.assertEqual(decoded, msg)

    def test_election_defaults_candidate(self):
        self.assertEqual(Election(sender_id="n3").candidate_id, "n3")

    def test_unknown_fields_ignored(self):
        m = Message.from_json('{"type":"COMMIT","sender_id":"n1","seq":3,"extra":1}')
        self.assertIsInstance(m, Commit)

    def test_bad_input(self):
        for bad in [b"not json", b'{"type":"NOPE","sender_id":"x"}', b'{"type":"ACK"}', b"[1,2]"]:
            with self.assertRaises(MessageDecodeError):
                Message.decode(bad)

    def test_buffer_split_and_merge(self):
        stream = b"".join(m.encode() for m in SAMPLES)
        buf = MessageBuffer()
        out = []
        for i in range(0, len(stream), 7):  # deliberately split into fragments
            out.extend(buf.feed(stream[i : i + 7]))
        self.assertEqual(out, SAMPLES)
        self.assertEqual(buf.pending, 0)

    def test_over_socket(self):
        a, b = socket.socketpair()
        with a, b:
            for m in SAMPLES:
                send_message(a, m)
            a.shutdown(socket.SHUT_WR)
            self.assertEqual(list(iter_messages(b)), SAMPLES)


if __name__ == "__main__":
    unittest.main()
