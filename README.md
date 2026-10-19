# zookeeper-like system: distributed number game

ii.3502 lab. we built a small zookeeper-like cluster of 5 nodes (1 leader, 4 followers) with leader election, log replication, quorum commits and recovery after a leader crash. a number guessing game runs on top of it.

- nodes talk over plain tcp sockets, one json object per line
- python 3.9+, standard library only (`socket`, `threading`, `sqlite3`)
- nodes `n1` to `n5` are separate processes on `127.0.0.1:5001` to `5005`. up to 5 clients can connect, each to any node

## quick start

run everything from the project root:

```bash
python3 run_local.py                # runs the 3 lab scenarios and prints PASS / FAIL
python3 run_local.py --scenario 3   # runs only one scenario (1, 2 or 3)
```

## running it by hand

### 1. start the 5 nodes

easiest way, all nodes in one terminal with an admin prompt:

```bash
python3 run_local.py -i
```

or one terminal per node:

```bash
python3 -m zab.node n1     # same for n2 ... n5, add -v for debug logs
```

### 2. start some clients

one terminal each, on different nodes:

```bash
python3 -m zab.client --name alice --node n2
python3 -m zab.client --name bob   --node n3
python3 -m zab.client --name carol --node n4
```

client commands: `write <0-100>` (sets the target), `guess <0-100>`, `read` (current target), `status` (status of the node you are connected to), `quit`.

every time a new target gets committed, all clients print `[event] new target = ...` straight away.

### 3. crash the leader

at the `admin>` prompt of `run_local.py -i`:

| command | what it does |
|-|-|
| `status` | role, epoch, leader, latest (epoch, seq), target and clients of every node |
| `kill` | `kill -9` the current leader (or `kill n3` for a given node) |
| `start n1` | restart a node |
| `logs` / `logs n2` | print the persistent log of the nodes |
| `q` | quit and stop all nodes |

if you started one terminal per node, just hit ctrl+c in the leader's terminal. clients that were on the dead node reconnect to another one by themselves.

### logs

- node output: `logs/n1.log` etc. (only when started through `run_local.py`)
- persistent logs: `python3 -m zab.logstore data/n1.db data/n2.db`

`run_local.py` wipes `data/` and `logs/` every time it starts. nodes started by hand keep their `data/*.db` and replay it, so do `rm -rf data/ logs/` first if you want a clean run. if some old node is still holding a port, `pkill -f zab.node` kills it.

## test scenarios (lab document, page 7)

| scenario | flow | what `run_local.py` does |
|-|-|-|
| 1. normal write | client sets a target -> leader propose -> quorum of acks -> commit -> every client sees the new target | alice writes 50 through n2, all 4 clients get notified, the 5 logs are identical |
| 2. guessing | clients guess -> leader picks the closest -> commits it as new target -> next round | round 1: bob 30, carol 62, dave doesn't guess so the 5 s timeout settles it -> 62. round 2: all 3 guess, settled right away -> 60 |
| 3. leader crash and recovery | kill the leader -> followers miss the heartbeat -> new leader -> logs synced -> things keep working | kill -9 n1 -> n2 elected (epoch 1 -> 2) -> logs identical -> writes and guesses work -> n1 comes back, follows n2 and catches up |

## architecture

```
zab/
  messages.py     the 6 message types and their json encoding
  network.py      tcp framing (MessageBuffer), Peer (outgoing connections), NodeServer (incoming connections)
  config.py       cluster addresses, quorum, timings
  logstore.py     LogStore, the sqlite log: append, read, commit, latest committed entry
  election.py     ElectionManager, heartbeat timeout and elections
  coordinator.py  LeaderCoordinator, leader side: (epoch, seq), propose, quorum, commit, sync, guess rounds
  node.py         Node, puts everything together, follower side and client handling
  game.py         game rules: closest guess, rounds, current target
  client.py       Client, connects to any node, write / guess / read, shows target updates live
run_local.py      starts 5 processes, runs the 3 scenarios, interactive admin mode
```

### roles

| role | job |
|-|-|
| leader | gives out (epoch, seq), sends propose, collects acks, sends commit and heartbeats, runs the guess rounds |
| follower | logs proposals as proposed, sends acks, applies commits, forwards client writes and guesses, watches heartbeats |
| client | connects to any node, sends write(x) / guess(x), shows the current target |

### write flow

1. the client sends `FORWARD(op=WRITE, value=x)` to whatever node it is on. if that node is a follower it forwards the request to the leader and does not log anything until the propose comes back.
2. the leader picks the next `(epoch, seq)`, logs it as proposed and sends `PROPOSE(epoch, seq, op, value)` to all followers.
3. each follower writes it to its log as proposed and answers `ACK(epoch, seq)`.
4. when the leader has a quorum (3 out of 5, the leader itself counts as one, so it needs 2 acks) it sends `COMMIT(epoch, seq)` to everyone.
5. every node marks the entry committed, updates the target and pushes the commit to its own clients.

commits always happen in seq order. if acks don't arrive within 1 s the leader resends the propose to the followers that haven't answered.

### game rules

- the first client sets the target (0 to 100) with `write x`.
- the others `guess y`. followers forward guesses to the leader, which keeps them for the current round.
- a round ends when either every online client except the author of the current target has guessed (and there are at least 2 guesses), or 5 seconds have passed since the first guess of the round. so with only one guesser online the round always waits for the timeout.
- the leader picks the closest guess (on a tie the earliest guess wins) and pushes it through the normal propose -> ack -> commit flow as a guess entry. once committed it is the new target and the next round starts.
- example: target 50, guesses {30, 62} -> 62 wins -> new target is 62.
- if someone does a `write` while a round is open, the target changes and the guesses so far are thrown away.

### heartbeats and elections

- the leader sends `HEARTBEAT(epoch)` every 0.3 s. a follower that hears nothing for a random 1 to 2 s goes looking and starts an election.
- each node sends its vote with its latest `(epoch, seq)`: `ELECTION(epoch, seq, candidate, round)`. nodes switch to a better candidate when they see one: higher `(epoch, seq)` wins, and on a tie the lower node id wins.
- when a candidate has a majority (3 votes), nodes wait another 0.4 s in case a better candidate shows up, then the election is over.
- the new leader bumps the epoch and resets seq to 0, so its first entry is `(epoch, 1)`.
- nodes that already have a leader don't vote. if one follower times out by mistake, the leader's next heartbeat pulls it back and the rest of the cluster is not disturbed.

### log sync after an election

- right after winning, the new leader sends its whole log to every follower (`PROPOSE op=SYNC`). followers replace their log with it and answer `ACK(kind=SYNC)`.
- once a majority is synced, the leader commits any proposed entries left over from the old leader, and only then starts serving clients. requests that come in before that are queued.
- the election always picks the most up to date node, so the new leader has every committed entry.
- nodes that join later (the restarted old leader, or a follower that missed messages) send `ACK(kind=JOIN)` when they start following and get the full log again.

### persistent log

one sqlite file per node, `data/<id>.db`, one row per entry:

| field | type | meaning |
|-|-|-|
| epoch | int | leader generation, goes up at every election |
| seq | int | proposal number inside the epoch |
| op | string | `WRITE` or `GUESS` |
| value | int | the proposed value |
| client | string | client that proposed it (extra field) |
| status | string | `PROPOSED` or `COMMITTED` |

a second table `meta` stores `accepted_epoch`, the highest epoch the node has accepted. after a restart the node uses it to reject old leaders, and it rebuilds the target from its committed entries. the db runs with `PRAGMA synchronous=FULL` so writes survive a `kill -9`.

### messages

| message | fields from the lab document | extra fields |
|-|-|-|
| `PROPOSE` | epoch, seq, op, value | client. with `op=SYNC`, value is the whole log |
| `ACK` | epoch, seq, from | kind: `PROPOSAL` / `JOIN` / `SYNC` / `REJECT` / `CLIENT` |
| `COMMIT` | epoch, seq | op, value, client (when pushed to clients) |
| `HEARTBEAT` | epoch, from | leader_id |
| `ELECTION` | epoch, seq, from | candidate_id, round |
| `FORWARD` | op, value, from client | op can also be `JOIN` / `LEAVE` / `READ` / `STATUS` |

every message also has `type, sender_id (= from), epoch, msg_id, timestamp`. for example:

```
{"type":"PROPOSE","sender_id":"n1","epoch":1,"msg_id":"9f2c...","timestamp":1760687000.1,"seq":2,"op":"GUESS","value":62,"client":"carol"}
```

since tcp is a stream, a single `recv()` can return half a message or several at once. `MessageBuffer` keeps the leftover bytes and only hands out complete lines.

### threads and timeouts

- nodeserver has an accept thread (with a timeout so it can stop) and one reader thread per incoming connection.
- each peer has its own send queue and sender thread. connects and sends have timeouts. if a peer just went down, heartbeats and votes to it are dropped for one heartbeat interval, while propose / ack / commit are retried after that wait.
- messages to clients go out in order from one dedicated thread.
- a timer thread ticks every 50 ms and handles heartbeat, ack and round timeouts using `time.monotonic()`.
- all node state is changed under one lock (`self.lock`, an rlock). while holding it, a node only puts messages on queues and writes to its local sqlite, it never waits on the network.

### known limitations

- guesses of an open round only live in the leader's memory. if the leader crashes before the round is settled those guesses are lost and clients have to guess again. committed targets are safe.
- sync always sends the full log. that's fine at lab scale but there is no incremental sync or snapshotting.
- an old leader that gets cut off from the network only steps down once it sees a higher epoch. it can't reach a quorum though, so it can't commit anything in the meantime.
