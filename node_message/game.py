"""
Number-guessing game rules (pure logic, independent of networking and replication).

    - The first client WRITEs a number (0-100) that becomes the target.
    - Other clients submit GUESSes.
    - The leader collects the guesses of the round, picks the one closest to the target,
      commits it as the new target, and a new round begins.
      e.g. target = 50, guesses = {30, 62} -> closest is 62 -> new target = 62.
    - On a tie, the guess submitted first wins.
    - A round is settled as soon as either: every online client (except the author of the
      current target) has guessed, or round_timeout (5 s by default) has passed since the
      round's first guess.
"""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Optional, Tuple

from .messages import Op

Entry = Dict[str, Any]   # one log entry: epoch, seq, op, value, client, status


def closest(target: int, guesses: Iterable[Tuple[str, int]]) -> Tuple[str, int]:
    """Return (client, value): the guess closest to target; on a tie, the earliest one."""
    best: Optional[Tuple[int, int, str, int]] = None
    for order, (client, value) in enumerate(guesses):
        key = (abs(value - target), order, client, value)
        if best is None or key < best:
            best = key
    if best is None:
        raise ValueError("no guesses in this round")
    return best[2], best[3]


class GuessRound:
    """Guesses collected by the leader for the current round (kept in leader memory only;
    once settled, only the winning guess is written to the log)."""

    def __init__(self) -> None:
        self.guesses: Dict[str, int] = {}     # client -> value, in submission order
        self.started_at: Optional[float] = None
        self.number = 1

    def add(self, client: str, value: int, now: float) -> None:
        if not self.guesses:
            self.started_at = now
        self.guesses.pop(client, None)        # guessing again in the same round: the last guess counts (moved to the end)
        self.guesses[client] = value

    def is_complete(self, expected_clients: Iterable[str]) -> bool:
        expected = set(expected_clients)
        return bool(expected) and expected <= set(self.guesses)

    def winner(self, target: int) -> Tuple[str, int]:
        return closest(target, self.guesses.items())

    def clear(self) -> None:
        self.guesses.clear()
        self.started_at = None

    def next(self) -> None:
        self.clear()
        self.number += 1


class TargetState:
    """Current target, derived from the committed log. Every node applies the same entries
    in the same order, so they all end up with the same target."""

    def __init__(self) -> None:
        self.current: Optional[Entry] = None

    @property
    def value(self) -> Optional[int]:
        return None if self.current is None else self.current["value"]

    @property
    def author(self) -> Optional[str]:
        return None if self.current is None else self.current["client"]

    def apply(self, entry: Entry) -> None:
        if entry["op"] in (Op.WRITE, Op.GUESS):
            self.current = dict(entry)

    def rebuild(self, committed: List[Entry]) -> None:
        self.current = None
        for entry in committed:
            self.apply(entry)
