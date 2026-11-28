# Storage memory profiler

`clickhouse-examples storage_memory_profiler` measures live jemalloc allocations while executing a stateful sequence of SQL files.
Unlike `parser_memory_profiler`, which isolates AST allocations for individual queries, this profiler keeps one ClickHouse context alive and records a checkpoint after each storage scenario.

Run it with a release build compiled with jemalloc profiling:

```bash
MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:0,prof_unbias:false \
  ./clickhouse-examples storage_memory_profiler \
  --file utils/storage-memory-profiler/scenarios/01_create_table.sql \
  --file utils/storage-memory-profiler/scenarios/02_insert_data.sql \
  --output-dir profiles \
  --path data
```

The command writes a tab-separated checkpoint summary to standard output and one jemalloc heap dump for the initial state and after every SQL file. The CI job runs the whole sequence twice per binary and compares each checkpoint transition of the second pass only, so that one-time process initialization is not charged to a scenario. A scenario must therefore leave no object behind after `09_cleanup.sql`; if it does, the second pass fails and the profiler exits non-zero. Allocations made while a thread joins or leaves a thread group are excluded from the comparison: they are per-thread accounting state that lives until the pool thread exits, so a scenario would otherwise be charged for whichever background threads happened to serve it. Once nothing merges or mutates a table any more, the scenarios stop its merges, so that a background merge-selection pass running at a heap dump is not charged to one checkpoint and credited back at the next. The inserted timestamps are fixed, so that the partition layout of the scenarios does not depend on the date of the run. It reuses the parser-memory check's batch symbolization, stable stack canonicalization, cross-version diff, and flamegraph report helpers, but publishes a separate storage-scenario report.

Each checkpoint pauses the initialized schedule pools, waits for admitted MergeTree executor tasks and worker cleanup to finish, and keeps scheduling paused through the allocator statistics read and heap dump. Queued callbacks remain queued and resume afterward, including when capture throws an exception. The shared wait deadline is 30 seconds; a timeout fails the run. This requires foreground SQL to have finished and does not freeze unrelated asynchronous loaders or concurrent pool reconfiguration.

CI also publishes `storage_memory_raw_checkpoints.json` with all heap totals, including startup and retained changes after each pass. These jemalloc totals apply no allocation-stack exclusions and are checked against the sum of allocation stacks. They do not measure RSS, static storage, or direct `mmap` allocations. They are informational: retained memory can include initialization, caches, and residual storage state, and default worker participation can vary. They are separate from the filtered second-pass scenario comparison and from jemalloc's `stats.allocated` printed by the profiler. With `lg_prof_sample:0`, `prof_unbias:false` disables sample-correction rounding so stack sizes add up exactly.

For controlled worker-population experiments, pass `--background-pool-size N` with a positive integer. This uses `N` merge/mutation workers and one worker in each other MergeTree executor, schedule pool, marks-loader pool, active-parts-loading pool, and parts-cleaning pool. These auxiliary pools retain their workers once real work starts them; checkpoints also drain their callbacks and propagate job exceptions. Their pool objects are initialized before the startup checkpoint. Even an empty new table uses the active-parts loader to scan its disks. Other auxiliary pools keep their existing configuration.

The merge/mutation task-to-worker ratio is rounded up to keep at least the default task capacity (currently 32); this preserves the default `MergeTreeSettings` validation for tables and projections. Task slots bound admitted work, not the number of workers. Add `--max-threads 1` to control foreground query parallelism independently; SQL can override this initial session setting. Omitting these options preserves the existing configuration. Start with `N = 1` when measuring lazy worker initialization: a larger fixed pool does not ensure that every worker handles a task in each pass.

The raw heap dumps retain worker allocations; the CI comparison currently still applies the exclusions described above. These exclusions can hide real changes in worker accounting and are not suitable as the sole memory-regression signal. The raw artifact provides visibility while the new controls are validated. Switching the blocking comparison requires a reference binary supporting the same controls, applied identically to both binaries, and removal of the exclusions from both totals and flamegraphs. Do not compare different worker configurations or treat first-pass retained memory as entirely initialization.

The scenarios are ordered by their numeric filename prefix and intentionally share state. They cover table creation, inserts, indexes, projections, dictionaries, multiple tables, many parts, system metadata, and cleanup.
