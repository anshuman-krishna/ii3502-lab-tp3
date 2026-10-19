# ZooKeeper-like System: Distributed Number Game

II.3502 lab. We built a small ZooKeeper-like cluster of 5 nodes (1 leader, 4 followers) with leader election, log replication, quorum commits and recovery after a leader crash. A number guessing game runs on top of it.

- Nodes talk over plain TCP sockets, one JSON object per line
- Python 3.9+, standard library only (`socket`, `threading`, `sqlite3`)
- Nodes `n1` to `n5` are separate processes on `127.0.0.1:5001` to `5005`. Up to 5 clients can connect, each to any node

## Quick start

Run everything from the project root:

```bash
python3 run_local.py                # runs the 3 lab scenarios and prints PASS / FAIL
python3 run_local.py --scenario 3   # runs only one scenario (1, 2 or 3)
```

## Running it by hand

### 1. Start the 5 nodes

Easiest way, all nodes in one terminal with an admin prompt:

```bash
python3 run_local.py -i
```

Or one terminal per node:

```bash
python3 -m zab.node n1     # same for n2 ... n5, add -v for debug logs
```

### 2. Start some clients

One terminal each, on different nodes:

```bash
python3 -m zab.client --name alice --node n2
python3 -m zab.client --name bob   --node n3
python3 -m zab.client --name carol --node n4
```

Client commands: `write <0-100>` (sets the target), `guess <0-100>`, `read` (current target), `status` (status of the node you are connected to), `quit`.

Every time a new target gets committed, all clients print `[event] new target = ...` straight away.

### 3. Crash the leader

At the `admin>` prompt of `run_local.py -i`:

| Command | What it does |
|-|-|
| `status` | role, epoch, leader, latest (epoch, seq), target and clients of every node |
| `kill` | `kill -9` the current leader (or `kill n3` for a given node) |
| `start n1` | restart a node |
| `logs` / `logs n2` | print the persistent log of the nodes |
| `q` | quit and stop all nodes |

If you started one terminal per node, just hit Ctrl+C in the leader's terminal. Clients that were on the dead node reconnect to another one by themselves.

### Logs

- Node output: `logs/n1.log` etc. (only when started through `run_local.py`)
- Persistent logs: `python3 -m zab.logstore data/n1.db data/n2.db`

`run_local.py` wipes `data/` and `logs/` every time it starts. Nodes started by hand keep their `data/*.db` and replay it, so do `rm -rf data/ logs/` first if you want a clean run. If some old node is still holding a port, `pkill -f zab.node` kills it.

## Test scenarios (lab document, page 7)

| Scenario | Flow | What `run_local.py` does |
|-|-|-|
| 1. Normal write | client sets a target -> leader PROPOSE -> quorum of ACKs -> COMMIT -> every client sees the new target | alice writes 50 through n2, all 4 clients get notified, the 5 logs are identical |
| 2. Guessing | clients guess -> leader picks the closest -> commits it as new target -> next round | round 1: bob 30, carol 62, dave doesn't guess so the 5 s timeout settles it -> 62. Round 2: all 3 guess, settled right away -> 60 |
| 3. Leader crash and recovery | kill the leader -> followers miss the heartbeat -> new leader -> logs synced -> things keep working | kill -9 n1 -> n2 elected (epoch 1 -> 2) -> logs identical -> writes and guesses work -> n1 comes back, follows n2 and catches up |

## Architecture

```
zab/
  messages.py     the 6 message types and their JSON encoding
  network.py      TCP framing (MessageBuffer), Peer (outgoing connections), NodeServer (incoming connections)
  config.py       cluster addresses, quorum, timings
  logstore.py     LogStore, the sqlite log: append, read, commit, latest committed entry
  election.py     ElectionManager, heartbeat timeout and elections
  coordinator.py  LeaderCoordinator, leader side: (epoch, seq), PROPOSE, quorum, COMMIT, sync, guess rounds
  node.py         Node, puts everything together, follower side and client handling
  game.py         game rules: closest guess, rounds, current target
  client.py       Client, connects to any node, write / guess / read, shows target updates live
run_local.py      starts 5 processes, runs the 3 scenarios, interactive admin mode
```

### Roles

| Role | Job |
|-|-|
| Leader | gives out (epoch, seq), sends PROPOSE, collects ACKs, sends COMMIT and heartbeats, runs the guess rounds |
| Follower | logs proposals as PROPOSED, sends ACKs, applies COMMITs, forwards client writes and guesses, watches heartbeats |
| Client | connects to any node, sends write(x) / guess(x), shows the current target |

### Write flow

1. The client sends `FORWARD(op=WRITE, value=x)` to whatever node it is on. If that node is a follower it forwards the request to the leader and does not log anything until the PROPOSE comes back.
2. The leader picks the next `(epoch, seq)`, logs it as PROPOSED and sends `PROPOSE(epoch, seq, op, value)` to all followers.
3. Each follower writes it to its log as PROPOSED and answers `ACK(epoch, seq)`.
4. When the leader has a quorum (3 out of 5, the leader itself counts as one, so it needs 2 ACKs) it sends `COMMIT(epoch, seq)` to everyone.
5. Every node marks the entry COMMITTED, updates the target and pushes the COMMIT to its own clients.

Commits always happen in seq order. If ACKs don't arrive within 1 s the leader resends the PROPOSE to the followers that haven't answered.

### Game rules

- The first client sets the target (0 to 100) with `write x`.
- The others `guess y`. Followers forward guesses to the leader, which keeps them for the current round.
- A round ends when either every online client except the author of the current target has guessed (and there are at least 2 guesses), or 5 seconds have passed since the first guess of the round. So with only one guesser online the round always waits for the timeout.
- The leader picks the closest guess (on a tie the earliest guess wins) and pushes it through the normal PROPOSE -> ACK -> COMMIT flow as a GUESS entry. Once committed it is the new target and the next round starts.
- Example: target 50, guesses {30, 62} -> 62 wins -> new target is 62.
- If someone does a `write` while a round is open, the target changes and the guesses so far are thrown away.

### Heartbeats and elections

- The leader sends `HEARTBEAT(epoch)` every 0.3 s. A follower that hears nothing for a random 1 to 2 s goes LOOKING and starts an election.
- Each node sends its vote with its latest `(epoch, seq)`: `ELECTION(epoch, seq, candidate, round)`. Nodes switch to a better candidate when they see one: higher `(epoch, seq)` wins, and on a tie the lower node id wins.
- When a candidate has a majority (3 votes), nodes wait another 0.4 s in case a better candidate shows up, then the election is over.
- The new leader bumps the epoch and resets seq to 0, so its first entry is `(epoch, 1)`.
- Nodes that already have a leader don't vote. If one follower times out by mistake, the leader's next heartbeat pulls it back and the rest of the cluster is not disturbed.

### Log sync after an election

- Right after winning, the new leader sends its whole log to every follower (`PROPOSE op=SYNC`). Followers replace their log with it and answer `ACK(kind=SYNC)`.
- Once a majority is synced, the leader commits any PROPOSED entries left over from the old leader, and only then starts serving clients. Requests that come in before that are queued.
- The election always picks the most up to date node, so the new leader has every committed entry.
- Nodes that join later (the restarted old leader, or a follower that missed messages) send `ACK(kind=JOIN)` when they start following and get the full log again.

### Persistent log

One sqlite file per node, `data/<id>.db`, one row per entry:

| Field | Type | Meaning |
|-|-|-|
| epoch | int | leader generation, goes up at every election |
| seq | int | proposal number inside the epoch |
| op | string | WRITE or GUESS |
| value | int | the proposed value |
| client | string | client that proposed it (extra field) |
| status | string | PROPOSED or COMMITTED |

A second table `meta` stores `accepted_epoch`, the highest epoch the node has accepted. After a restart the node uses it to reject old leaders, and it rebuilds the target from its committed entries. The db runs with `PRAGMA synchronous=FULL` so writes survive a `kill -9`.

### Messages

| Message | Fields from the lab document | Extra fields |
|-|-|-|
| PROPOSE | epoch, seq, op, value | client. With op=SYNC, value is the whole log |
| ACK | epoch, seq, from | kind: PROPOSAL / JOIN / SYNC / REJECT / CLIENT |
| COMMIT | epoch, seq | op, value, client (when pushed to clients) |
| HEARTBEAT | epoch, from | leader_id |
| ELECTION | epoch, seq, from | candidate_id, round |
| FORWARD | op, value, from client | op can also be JOIN / LEAVE / READ / STATUS |

Every message also has `type, sender_id (= from), epoch, msg_id, timestamp`. For example:

```
{"type":"PROPOSE","sender_id":"n1","epoch":1,"msg_id":"9f2c...","timestamp":1760687000.1,"seq":2,"op":"GUESS","value":62,"client":"carol"}
```

Since TCP is a stream, a single `recv()` can return half a message or several at once. `MessageBuffer` keeps the leftover bytes and only hands out complete lines.

### Threads and timeouts

- NodeServer has an accept thread (with a timeout so it can stop) and one reader thread per incoming connection.
- Each peer has its own send queue and sender thread. Connects and sends have timeouts. If a peer just went down, heartbeats and votes to it are dropped for one heartbeat interval, while PROPOSE / ACK / COMMIT are retried after that wait.
- Messages to clients go out in order from one dedicated thread.
- A timer thread ticks every 50 ms and handles heartbeat, ACK and round timeouts using `time.monotonic()`.
- All node state is changed under one lock (`self.lock`, an RLock). While holding it, a node only puts messages on queues and writes to its local sqlite, it never waits on the network.

### Known limitations

- Guesses of an open round only live in the leader's memory. If the leader crashes before the round is settled those guesses are lost and clients have to guess again. Committed targets are safe.
- Sync always sends the full log. That's fine at lab scale but there is no incremental sync or snapshotting.
- An old leader that gets cut off from the network only steps down once it sees a higher epoch. It can't reach a quorum though, so it can't commit anything in the meantime.
