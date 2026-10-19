# ZooKeeper-like System: Distributed Number Game

II.3502 lab: a ZooKeeper-like coordination system of 5 nodes (1 leader + 4 followers) with leader election, log replication, quorum commits and recovery after leader failure, running a turn-based number game on top.

- Communication: raw TCP sockets, one JSON object per line (newline-delimited)
- Language: Python 3.9+, standard library only (`socket`, `threading`, `sqlite3`)
- Topology: `n1`–`n5`, each a separate process listening on `127.0.0.1:5001`–`5005`; up to 5 clients, connecting to any node

## Quick start

From the project root:

```bash
python3 run_local.py                # demo the 3 test scenarios of the lab document, prints PASS / FAIL
python3 run_local.py --scenario 3   # demo a single scenario (1 / 2 / 3)
python3 -m unittest discover tests  # unit + integration tests (24)
```

## Running by hand (for the live defence)

**1. Start the 5 nodes**, either:

```bash
python3 run_local.py -i             # starts all nodes in one terminal and gives admin commands (recommended)
```

or one terminal per node:

```bash
python3 -m node_message.node n1     # same for n2 … n5; add -v for debug logs
```

**2. Run clients**, one terminal each, connected to different nodes:

```bash
python3 -m node_message.client --name alice --node n2
python3 -m node_message.client --name bob   --node n3
python3 -m node_message.client --name carol --node n4
```

Client commands: `write <0-100>` (set the target), `guess <0-100>`, `read` (current target), `status` (status of the connected node), `quit`.
Whenever a new target is committed, every client immediately prints `[event] new target = …`.

**3. Simulate a leader crash** at the `admin>` prompt of `run_local.py -i`:

| Command | Effect |
|---|---|
| `status` | role, epoch, leader, latest (epoch, seq), target and connected clients of every node |
| `kill` | `kill -9` the current leader (or `kill n3` for a specific node) |
| `start n1` | restart a node |
| `logs` / `logs n2` | print the persistent log table of the nodes |
| `q` | quit and stop all nodes |

With one terminal per node, press Ctrl+C in the leader's terminal or close it. Clients connected to the killed node reconnect to another node automatically.

**Logs**:

- Node output: `logs/n1.log` … (when started by `run_local.py`)
- Persistent logs: `python3 -m node_message.logstore data/n1.db data/n2.db`

`run_local.py` clears `data/` and `logs/` on every start; nodes started by hand keep and replay their existing `data/*.db`.

## Test scenarios (lab document, page 7)

| Scenario | Flow | What `run_local.py` shows |
|---|---|---|
| 1. Normal write | client sets a target → leader PROPOSE → quorum ACKs → COMMIT → all clients see the new target | alice writes 50 via n2, all 4 clients are notified, the 5 logs are identical |
| 2. Guessing | clients guess → leader picks the closest → commits the new target → next round | round 1: bob 30, carol 62 (dave doesn't guess, settled by the 5 s timeout) → 62; round 2: all 3 guess, settled immediately → 60 |
| 3. Leader crash and recovery | kill the leader → followers detect the missing heartbeat → new leader → logs synchronised → operations resume | kill -9 n1 → n2 elected (epoch 1→2) → logs identical → writes and guesses work → n1 restarts, follows n2 and catches up |

## Architecture

```
node_message/
  messages.py     the 6 message types + JSON-line serialization
  framing.py      TCP stream framing (handles partial / merged reads)
  config.py       topology, quorum, timing parameters
  server.py       NodeServer        — accepts connections, dispatches PROPOSE/ACK/COMMIT/HEARTBEAT/ELECTION/FORWARD
  transport.py    Peer              — outbound connection to another node (one send queue and thread per peer)
  logstore.py     LogStore          — persistent log (sqlite3): append / read / commit / latest committed entry
  election.py     ElectionManager   — heartbeat-timeout detection, elections
  coordinator.py  LeaderCoordinator — assigns (epoch, seq), PROPOSE, waits for quorum, COMMIT, sync, guess rounds
  node.py         Node              — wires the components; follower-side and client-side logic
  game.py         game rules: closest guess, rounds, current target
  client.py       ClientApp         — connects to any node, Write / Guess / Read, live target display
run_local.py      starts 5 processes, demos the 3 scenarios; interactive admin mode
tests/            24 tests
```

### Roles

| Role | Responsibilities |
|---|---|
| Leader | assigns (epoch, seq), sends PROPOSE, collects ACKs, broadcasts COMMIT, sends heartbeats; collects and settles guess rounds |
| Follower | logs proposals (PROPOSED), sends ACKs, applies COMMITs (COMMITTED), forwards client Write / Guess requests, monitors heartbeats |
| Client | connects to any node, sends Write(x) / Guess(x), reads and displays the current target |

### Write flow

1. The client sends `FORWARD(op=WRITE, value=x)` to any node. A follower forwards it to the leader and **logs nothing until the PROPOSE arrives**.
2. The leader assigns `(epoch, seq)`, logs it as PROPOSED and sends `PROPOSE(epoch, seq, op, value)` to every follower.
3. Each follower logs it as PROPOSED (persisted) and replies `ACK(epoch, seq)`.
4. Once the leader has a quorum (**majority = 3, the leader counts as 1 vote**, i.e. 2 more ACKs) it broadcasts `COMMIT(epoch, seq)`.
5. Every node marks the entry COMMITTED, updates the target and pushes the COMMIT to the clients connected to it.

The leader commits in seq order. On ACK timeout (1 s) the PROPOSE is resent to followers that have not acknowledged it.

### Game rules

- The first client sets the target (0–100) with `write x`.
- Other clients `guess y`; followers forward guesses to the leader, which collects the round's guesses.
- A round is settled as soon as either: **every online client (except the author of the current target) has guessed**, or **5 seconds have passed since the round's first guess**.
- The leader picks the guess closest to the target (on a tie, the earliest guess wins), writes it as a GUESS through the full PROPOSE → ACK → COMMIT flow, and once committed it becomes the new target and a new round begins.
- Example: target 50, guesses {30, 62} → 62 → new target 62.
- If someone `write`s during an open round, the target changes and the guesses collected so far are discarded.

### Heartbeats and elections

- The leader broadcasts `HEARTBEAT(epoch)` every 0.3 s. A follower that hears none within a random 1–2 s becomes LOOKING and starts an election.
- Each candidate broadcasts its latest `(epoch, seq)`: `ELECTION(epoch, seq, candidate, round)`. Nodes switch their vote to a better candidate: **higher (epoch, seq) wins; on a tie, the lower node ID wins**.
- Once a candidate holds a majority (3 votes), nodes wait 0.4 s more for a better candidate, then the election ends.
- The new leader **increments the epoch and resets seq to 0** (the first entry of the new epoch is `(epoch, 1)`).
- Nodes that already have a leader do not vote; a single follower that times out by mistake is pulled back by the leader's heartbeat and does not disturb the cluster.

### Log synchronisation (recovery)

- After taking over, the new leader first sends its full log to every follower (`PROPOSE op=SYNC`). Followers replace their local log with it and reply `ACK(kind=SYNC)`.
- Once a majority is synchronised, the leader commits the PROPOSED entries left by the previous leader and only then starts handling client requests (requests received earlier are queued).
- The election picks the most up-to-date node, so the new leader always holds every committed entry.
- Nodes that join later (a restarted old leader, a follower that missed messages) send `ACK(kind=JOIN)` when they start following, and the leader sends them the full log again.

### Persistent log

One `data/<id>.db` (sqlite3) per node, one row per entry:

| Field | Type | Description |
|---|---|---|
| epoch | int | leader generation, incremented on each election |
| seq | int | proposal number within the epoch |
| op | string | WRITE or GUESS |
| value | int | proposed value |
| client | string | client that proposed the value (extra field) |
| status | string | PROPOSED or COMMITTED |

The highest epoch the node has accepted is persisted too. After a restart, the node uses it to reject stale leaders, and recovers the target from its committed log.

### Messages

| Message | Fields (suggested by the lab document) | Extra fields |
|---|---|---|
| PROPOSE | epoch, seq, op, value | client; with op=SYNC, value is the full log |
| ACK | epoch, seq, from | kind: PROPOSAL / JOIN / SYNC / REJECT / CLIENT |
| COMMIT | epoch, seq | op, value, client (when pushed to clients) |
| HEARTBEAT | epoch, from | leader_id |
| ELECTION | epoch, seq, from | candidate_id, round |
| FORWARD | op, value, from client | op also covers JOIN / LEAVE / READ / STATUS |

Every message also carries `type, sender_id (=from), epoch, msg_id, timestamp`. Example:

```
{"type":"PROPOSE","sender_id":"n1","epoch":1,"msg_id":"…","timestamp":1760687000.1,"seq":2,"op":"GUESS","value":62,"client":"carol"}
```

### Threads and timeouts

- NodeServer: an accept thread (with a timeout, so it can be stopped) plus one reader thread per incoming connection.
- One sender thread and queue per peer; connects and sends have timeouts. For a peer that just became unreachable, heartbeats and votes are dropped for one heartbeat interval, while PROPOSE / ACK / COMMIT are retried after that backoff.
- Messages to clients are sent, in order, by one dedicated thread.
- The timer thread is independent of all network I/O and uses `time.monotonic()` for heartbeat, ACK and round timeouts.
- All node state is changed under a single lock; while holding it a node only enqueues messages and writes to the local sqlite, never blocking on the network.

### Known limitations

- Guesses of an open round live only in the leader's memory: if the leader crashes before the round is settled, those guesses are lost and clients must guess again. Committed targets are not affected.
- Sync sends the full log (enough at lab scale); there is no incremental sync or snapshotting.
- A network-isolated old leader does not step down until it receives a message with a higher epoch; it cannot reach a quorum, though, so it cannot commit anything.
