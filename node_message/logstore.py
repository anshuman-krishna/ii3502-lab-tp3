"""
LogStore: persistent log (sqlite3, one data/<id>.db file per node).

Table log, one row per transaction:
    epoch   INT   leader generation
    seq     INT   proposal number within the epoch (starts at 1 in each new epoch)
    op      TEXT  WRITE / GUESS
    value   INT   proposed value
    client  TEXT  client that proposed the value
    status  TEXT  PROPOSED / COMMITTED

Table meta stores accepted_epoch (the highest epoch this node has accepted); after a restart
the node uses it to reject stale leaders.

Print a node's log: python -m node_message.logstore data/n1.db
"""

from __future__ import annotations

import argparse
import sqlite3
import threading
from typing import Dict, List, Optional, Tuple

PROPOSED, COMMITTED = "PROPOSED", "COMMITTED"
COLUMNS = ("epoch", "seq", "op", "value", "client", "status")

Entry = Dict[str, object]
Zxid = Tuple[int, int]


class LogStore:
    def __init__(self, path: str, readonly: bool = False) -> None:
        self.path = path
        self._lock = threading.Lock()
        if readonly:
            self._db = sqlite3.connect(f"file:{path}?mode=ro", uri=True, check_same_thread=False)
            return
        self._db = sqlite3.connect(path, check_same_thread=False, isolation_level=None)  # autocommit
        self._db.execute("PRAGMA synchronous=FULL")  # every write reaches disk, survives kill -9
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS log (epoch INTEGER, seq INTEGER, op TEXT, value INTEGER,"
            " client TEXT, status TEXT, PRIMARY KEY (epoch, seq))"
        )
        self._db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value INTEGER)")

    def close(self) -> None:
        with self._lock:
            self._db.close()

    # ---------- read ----------

    def _select(self, where: str = "", args: tuple = ()) -> List[Entry]:
        with self._lock:
            rows = self._db.execute(
                f"SELECT {', '.join(COLUMNS)} FROM log {where} ORDER BY epoch, seq", args
            ).fetchall()
        return [dict(zip(COLUMNS, row)) for row in rows]

    def entries(self) -> List[Entry]:
        return self._select()

    def committed_entries(self) -> List[Entry]:
        return self._select("WHERE status = ?", (COMMITTED,))

    def get(self, epoch: int, seq: int) -> Optional[Entry]:
        rows = self._select("WHERE epoch = ? AND seq = ?", (epoch, seq))
        return rows[0] if rows else None

    def last_zxid(self) -> Zxid:
        """(epoch, seq) of the latest entry, whatever its status; (0, 0) for an empty log. Compared in elections."""
        with self._lock:
            row = self._db.execute("SELECT epoch, seq FROM log ORDER BY epoch DESC, seq DESC LIMIT 1").fetchone()
        return (row[0], row[1]) if row else (0, 0)

    def last_committed(self) -> Optional[Entry]:
        rows = self.committed_entries()
        return rows[-1] if rows else None

    # ---------- write ----------

    def append(self, entry: Entry) -> bool:
        """Append an entry; ignored if (epoch, seq) already exists (a resent PROPOSE is idempotent)."""
        with self._lock:
            cur = self._db.execute(
                f"INSERT OR IGNORE INTO log ({', '.join(COLUMNS)}) VALUES (?, ?, ?, ?, ?, ?)",
                tuple(entry[c] for c in COLUMNS),
            )
            return cur.rowcount == 1

    def commit_upto(self, epoch: int, seq: int) -> List[Entry]:
        """Mark (epoch, seq) and every earlier PROPOSED entry as COMMITTED; return those entries in order."""
        where = "WHERE status = ? AND (epoch < ? OR (epoch = ? AND seq <= ?))"
        with self._lock:
            self._db.execute("BEGIN")
            rows = self._db.execute(
                f"SELECT {', '.join(COLUMNS)} FROM log {where} ORDER BY epoch, seq", (PROPOSED, epoch, epoch, seq)
            ).fetchall()
            self._db.execute(f"UPDATE log SET status = ? {where}", (COMMITTED, PROPOSED, epoch, epoch, seq))
            self._db.execute("COMMIT")
        return [dict(zip(COLUMNS, row), status=COMMITTED) for row in rows]

    def replace_all(self, entries: List[Entry]) -> None:
        """Sync: replace the whole local log with the leader's (in a single transaction)."""
        with self._lock:
            self._db.execute("BEGIN")
            self._db.execute("DELETE FROM log")
            self._db.executemany(
                f"INSERT INTO log ({', '.join(COLUMNS)}) VALUES (?, ?, ?, ?, ?, ?)",
                [tuple(e[c] for c in COLUMNS) for e in entries],
            )
            self._db.execute("COMMIT")

    # ---------- metadata ----------

    def accepted_epoch(self) -> int:
        with self._lock:
            row = self._db.execute("SELECT value FROM meta WHERE key = 'accepted_epoch'").fetchone()
        return row[0] if row else 0

    def set_accepted_epoch(self, epoch: int) -> None:
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('accepted_epoch', ?)", (epoch,))


def format_table(entries: List[Entry]) -> str:
    if not entries:
        return "  (empty)"
    lines = [f"  {'epoch':>5} {'seq':>4}  {'op':<6} {'value':>5}  {'client':<8} status"]
    for e in entries:
        lines.append(f"  {e['epoch']:>5} {e['seq']:>4}  {e['op']:<6} {e['value']:>5}  {e['client'] or '-':<8} {e['status']}")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Print node logs")
    parser.add_argument("paths", nargs="+", help="e.g. data/n1.db")
    args = parser.parse_args()
    for path in args.paths:
        store = LogStore(path, readonly=True)
        print(f"--- {path} (accepted_epoch={store.accepted_epoch()})")
        print(format_table(store.entries()))
        store.close()


if __name__ == "__main__":
    main()
