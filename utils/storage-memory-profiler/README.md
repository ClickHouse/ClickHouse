# Storage memory profiler

`clickhouse-examples storage_memory_profiler` measures live jemalloc allocations while executing a stateful sequence of SQL files.
Unlike `parser_memory_profiler`, which isolates AST allocations for individual queries, this profiler keeps one ClickHouse context alive and records a checkpoint after each storage scenario.

Run it with a release build compiled with jemalloc profiling:

```bash
MALLOC_CONF=prof:true,prof_active:true,lg_prof_sample:0 \
  ./clickhouse-examples storage_memory_profiler \
  --file utils/storage-memory-profiler/scenarios/01_create_table.sql \
  --file utils/storage-memory-profiler/scenarios/02_insert_data.sql \
  --output-dir profiles \
  --path data
```

The command writes a tab-separated checkpoint summary to standard output and one jemalloc heap dump for the initial state and after every SQL file. The CI job compares each checkpoint transition between master and the pull request. It reuses the parser-memory check's batch symbolization, stable stack canonicalization, cross-version diff, and flamegraph report helpers, but publishes a separate storage-scenario report.

The scenarios are ordered by their numeric filename prefix and intentionally share state. They cover table creation, inserts, indexes, projections, dictionaries, multiple tables, many parts, system metadata, and cleanup.
