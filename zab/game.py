from .messages import Op


def closest(target, guesses):
    # (client, value) closest to target, ties go to the earliest guess
    best = None
    for order, (client, value) in enumerate(guesses):
        key = (abs(value - target), order, client, value)
        if best is None or key < best:
            best = key
    if best is None:
        raise ValueError("no guesses in this round")
    return best[2], best[3]


# guesses of the current round, kept in leader memory only
class GuessRound:
    def __init__(self):
        self.guesses = {}  # client -> value, in submission order
        self.started_at = None
        self.number = 1

    def add(self, client, value, now):
        if not self.guesses:
            self.started_at = now
        self.guesses.pop(client, None)  # guessing again: last guess counts
        self.guesses[client] = value

    def is_complete(self, expected_clients):
        # a single guess never settles early, it waits for the timeout
        expected = set(expected_clients)
        return len(self.guesses) >= 2 and bool(expected) and expected <= set(self.guesses)

    def winner(self, target):
        return closest(target, self.guesses.items())

    def clear(self):
        self.guesses.clear()
        self.started_at = None

    def next(self):
        self.clear()
        self.number += 1


# current target, rebuilt from the committed log
class TargetState:
    def __init__(self):
        self.current = None

    @property
    def value(self):
        return None if self.current is None else self.current["value"]

    @property
    def author(self):
        return None if self.current is None else self.current["client"]

    def apply(self, entry):
        if entry["op"] in (Op.WRITE, Op.GUESS):
            self.current = dict(entry)

    def rebuild(self, committed):
        self.current = None
        for entry in committed:
            self.apply(entry)
