<!--
Closes: https://github.com/ClickHouse/ClickHouse/issues/102707
-->

One unified set of `SYSTEM` commands to control the background activity of every engine that has it — the streaming engines (`Kafka`, `RabbitMQ`, `NATS`, `S3Queue`/`AzureQueue`) and refreshable materialized views (RMV). Previously RMV had its own `SYSTEM ... VIEW` commands and the streaming engines had no per-table SQL control at all.

```sql
SYSTEM STOP    [db.]table | ALL BACKGROUND   -- interrupt the running cycle AND block further ones
SYSTEM START   [db.]table | ALL BACKGROUND   -- resume (undo STOP / PAUSE)
SYSTEM PAUSE   [db.]table | ALL BACKGROUND   -- block further cycles, let the running one finish
SYSTEM CANCEL  [db.]table | ALL BACKGROUND   -- interrupt the running cycle only, keep going
SYSTEM REFRESH [db.]table | ALL BACKGROUND   -- run exactly one out-of-order cycle
```

On an RMV the verbs are **aliases** of the existing `SYSTEM ... VIEW` commands. `ALL BACKGROUND` applies the verb to every such table at once.

## How the verbs behave

A **cycle** = read a block → insert into the dependent MVs → reach the engine's **commit point**:

| Engine | Commit point |
|---|---|
| Kafka v1/v2 | offset commit (librdkafka / Keeper offset guard) |
| RabbitMQ | AMQP ack |
| NATS (JetStream) | JetStream ack |
| NATS (core) | *(none — at-most-once, see Limitations)* |
| S3Queue / AzureQueue | file marked `Processed` in Keeper |
| RMV | refresh result committed |

Each verb combines two flags — block future cycles, and abort the in-flight cycle before its commit point:

| verb | block future cycles? | abort in-flight cycle? | note |
|---|---|---|---|
| `STOP` | yes | yes | `= PAUSE + CANCEL` |
| `PAUSE` | yes | no | running cycle still commits |
| `CANCEL` | no | yes | keeps scheduling |
| `START` | unblock | — | undo `STOP` / `PAUSE` |
| `REFRESH` | — | — | one extra out-of-order cycle |

An aborted cycle is not committed, so it is redelivered and reprocessed later — except core NATS, which cannot replay and drops it.

`REFRESH` runs immediately on a streaming table (even while stopped, then re-blocks); on a stopped RMV it defers until `START`, like `SYSTEM REFRESH VIEW`.

## Design notes

- **AST**: scope is encoded in the `Type` enum (5 bare verbs + 5 `*_ALL_BACKGROUND`), not a bool — mirrors the existing `START_VIEW`/`START_VIEWS` pattern so the parser/formatter need no special-casing. The bare verbs are matched in a parser fallback *after* the specific forms so `SYSTEM STOP` can't shadow `STOP MERGES`/`STOP VIEW`.
- **Dispatch** (`InterpreterSystemQuery`): one place resolves the table and branches on the new `IStorage::isStreamingStorage()` predicate — streaming path vs. RMV-aliasing path. `isStreamingStorage()` is used instead of `isMessageQueue()` so `S3Queue`/`AzureQueue` join without inheriting `isMessageQueue()`'s unrelated INSERT kill-switch. `ALL BACKGROUND` fans out over both the RMV and streaming action locks, each skipping tables the caller can't access.
- **Streaming mechanism** (`StreamingBackgroundControl`, one per streaming storage): three signals — an `ActionBlocker` (block/release future cycles, integrates with the action-lock manager + fan-out), a monotonic `cancel_epoch` (every consumer — a streaming cycle *or* a direct SELECT — snapshots it when it starts and aborts its in-flight unit only if the epoch has advanced past the snapshot; there is no shared flag to reset, so a stale `STOP`/`CANCEL` cannot poison a later direct read and concurrent cycles cannot clear each other's request), and `refresh_once`. The control machinery is uniform across engines; **only the abort/boundary mechanism is engine-specific**: Kafka v1 `markDirty`+rewind, Kafka v2 return before the Keeper offset guard commits, RabbitMQ requeue (`reject` with `AMQP::requeue` — plain reject *drops*), S3Queue/AzureQueue cancel the *pipeline* (the slow part is the downstream insert, not the source) → files reset & reprocessed, NATS JetStream explicit ack after a successful insert — added because auto-ack-on-delivery put the durable boundary *before* the insert, leaving `STOP`/`CANCEL` nothing to abort without losing the block (core NATS has no ack/replay and stays lossy).
- **RBAC**: new `SYSTEM BACKGROUND` privilege (`TABLE` level). The required privilege is resolved **at runtime by engine**: RMV → `SYSTEM VIEWS` (same grant as the `... VIEW` alias), streaming → `SYSTEM BACKGROUND`. The generic verb is deliberately not pinned to one fixed privilege. `ALL BACKGROUND` never errors on access — it acts on what you may control and silently skips the rest.

## Known limitations / out of scope

- **`ON CLUSTER`** is not supported for these commands, matching the `SYSTEM ... VIEW` aliases; the access mapping is defensive only. The `getRequiredAccessForDDLOnCluster` mapping for these types is unreachable; if `ON CLUSTER` parsing is ever enabled, the mapping must first be made engine-aware (`SYSTEM BACKGROUND` for streaming tables).
- **Core NATS** `STOP`/`CANCEL` are lossy (no replay); JetStream is at-least-once.
- **Suggestion (out of scope):** gate the JetStream `ManualAck=true` change behind a new `nats_jetstream_manual_ack` setting, the same way `nats_wait_for_flush_interval` is gated. Default `false` would preserve the pre-PR behaviour (auto-ack on delivery → at-most-once, a direct `SELECT` permanently consumes); default `true` would match the other streaming engines (a direct `SELECT` does not consume, and nothing is lost to an ack landing before the insert). Left as a suggestion since it is tangential to the engine-agnostic `SYSTEM` controls; the default is a NATS-semantics call for the maintainers.
- The `PAUSE`-vs-`STOP` in-flight distinction is asserted deterministically for RMV and via dedicated in-flight tests for streaming; ordinary streaming tests only assert "future activity halts".
- `WindowView` (experimental) and an engine-agnostic `WAIT` are out of scope; `SYSTEM WAIT VIEW` stays RMV-only.

## Tests

- `04320_parser_system_background_controls` — parser round-trip.
- `04319_system_stop_all_background` — RMV: every verb (per-table + `ALL BACKGROUND`) + precise RBAC.
- `test_storage_{kafka,rabbitmq,nats,s3_queue}/test_system_stop.py` — every verb per engine incl. Kafka v2, `AzureQueue`, NATS JetStream; in-flight `STOP`-vs-`PAUSE`; `REFRESH`-runs-once; and RBAC proving `SYSTEM VIEWS` is *not* enough and `SYSTEM BACKGROUND` on the table *is* the required grant. The `test_storage_nats/` directory (previously removed from master) returns only as a home for this one test file plus the minimal broker/TLS harness it needs — none of the removed original NATS suites are re-introduced.

### Changelog category (leave one):
- New Feature

### Changelog entry (a [user-readable short description](https://github.com/ClickHouse/ClickHouse/blob/master/docs/changelog_entry_guidelines.md) of the changes that goes into CHANGELOG.md):

Added engine-agnostic `SYSTEM STOP`, `SYSTEM START`, `SYSTEM PAUSE`, `SYSTEM CANCEL`, and `SYSTEM REFRESH` commands — and their `... ALL BACKGROUND` server-wide forms — to control the background activity of `Kafka`, `RabbitMQ`, `NATS`, `S3Queue`/`AzureQueue` tables and refreshable materialized views through one unified interface. For refreshable materialized views they alias the existing `SYSTEM ... VIEW` commands. As part of supporting these controls on NATS, JetStream tables now acknowledge messages only after a successful insert (at-least-once; previously messages were auto-acknowledged on delivery and could be lost if the insert failed). A new `nats_wait_for_flush_interval` setting (default `false`, preserving the previous low-latency behaviour) optionally keeps a consumption cycle open for the whole flush interval instead of flushing as soon as the queue drains.

### Documentation entry for user-facing changes
- [x] Documented in `docs/en/sql-reference/statements/system.md` and `docs/en/sql-reference/statements/grant.md`.
