# Running SQuaLity against ClickHouse

This adapter executes records parsed by [SQuaLity](https://github.com/suyZhong/SQuaLity) on a dedicated ClickHouse server. It accepts SQLite SQLLogicTest and DuckDB `.test` files, plus PostgreSQL regression `.sql` files with matching `.out` files. Each file gets a new database. The runner compares query results with the source suite's expected output and writes one JSON object per record plus a summary. For SQLite files it also runs each statement on SQLite, so a mismatch can be checked against the source engine.

From the ClickHouse repository root, prepare the upstream parser and suites in `tmp`:

```sh
mkdir -p tmp/squality
git clone --depth 1 https://github.com/suyZhong/SQuaLity.git tmp/squality/SQuaLity
git clone --depth 1 https://github.com/gregrahn/sqllogictest.git tmp/squality/sqllogictest
git clone --depth 1 --filter=blob:none --sparse https://github.com/duckdb/duckdb.git tmp/squality/duckdb
git -C tmp/squality/duckdb sparse-checkout set test/sql
git clone --depth 1 --filter=blob:none --sparse https://github.com/postgres/postgres.git tmp/squality/postgres
git -C tmp/squality/postgres sparse-checkout set src/test/regress/sql src/test/regress/expected
python3 -m pip install --target tmp/squality/dependencies pandas sqlparse
```

Start a dedicated server using a recent ClickHouse master binary. The server launcher writes its configuration and data only below the chosen directory and listens on localhost. Use a fresh directory for each run. Its default server memory limit is 4 GiB; set `--max-memory` in bytes for a larger full-corpus run when the host has room.

```sh
python3 utils/squality/server.py --binary /absolute/path/to/clickhouse --directory tmp/squality/server
```

The preset in [settings.json](settings.json) enables SQL behavior that covered more source queries in the experiment. Examples of bounded runs:

```sh
mkdir -p tmp/squality/build tmp/squality/results
PYTHONPATH=tmp/squality/dependencies python3 utils/squality/run.py \
  --squality tmp/squality/SQuaLity --suite sqlite \
  --tests tmp/squality/sqllogictest/test --glob 'select*.test' \
  --max-records 1000 --profile sql_standard \
  --output tmp/squality/results/sqlite_select.jsonl \
  > tmp/squality/build/test_sqlite_select.log 2>&1

PYTHONPATH=tmp/squality/dependencies python3 utils/squality/run.py \
  --squality tmp/squality/SQuaLity --suite duckdb \
  --tests tmp/squality/duckdb/test/sql --max-files 750 --max-records 50 \
  --seed 20261005 --profile sql_standard --set default_table_engine=MergeTree \
  --set allow_create_index_without_type=1 \
  --output tmp/squality/results/duckdb.jsonl \
  > tmp/squality/build/test_duckdb.log 2>&1

PYTHONPATH=tmp/squality/dependencies python3 utils/squality/run.py \
  --squality tmp/squality/SQuaLity --suite postgresql \
  --tests tmp/squality/postgres/src/test/regress/sql --glob '**/*.sql' \
  --profile sql_standard --set default_table_engine=MergeTree \
  --output tmp/squality/results/postgresql.jsonl \
  > tmp/squality/build/test_postgresql.log 2>&1
```

`--max-files` samples files deterministically from the sorted suite. `--max-records` stops before the given record ID in each file. Omitting both caps attempts every file and every parsed record. For a large corpus, run independent shards with `--shard-count N --shard-index I` for every index from 0 through N-1. Each shard needs a distinct `--output` path and redirected log. The runner writes a checkpoint after each file; repeat the same command with `--resume` after interruption. The ClickHouse version, selected files, and settings must match the checkpoint. A full SQLite corpus can take a long time and produce large JSONL output.

The JSONL distinguishes passed queries, result mismatches, SQL exceptions, expected errors, unsupported harness directives, blocked setup, result-encoding limitations, and parser errors. A mismatch is evidence to investigate, since the source suites exercise different SQL dialects. Check its `setup`, `expected`, `actual`, and SQLite `reference_actual` fields before reporting a ClickHouse bug. Unsupported `UNIQUE` indexes, database administration, DuckDB plan oracles, PostgreSQL `psql` commands, and uninstalled data fixtures are excluded rather than silently accepted. PostgreSQL's regression files depend on shared setup and fixtures that this isolated-file runner does not replay; only self-contained records give meaningful query coverage.

The preset uses `data_type_default_nullable=1` for source schemas with omitted columns and `join_use_nulls=1` for outer join results. It also uses `cast_keep_nullable=1`, `group_by_use_nulls=1`, `prefer_column_name_to_alias=1`, and `DISTINCT` defaults for `UNION`, `INTERSECT`, and `EXCEPT`. `aggregate_functions_null_for_empty=1` helps match some source aggregates; it can change the behavior of other aggregate cases, so compare both profiles for a suspected failure. These are test compatibility settings. Per `schema-types-avoid-nullable`, broad nullable columns cost memory and are not a production schema recommendation; per `query-join-null-handling`, `join_use_nulls=1` trades memory for SQL-style nulls in unmatched rows.

`default_table_engine=Memory` keeps SQLite query runs small and quick. Use `--set default_table_engine=MergeTree --set data_type_default_nullable=0` for SQLite files with `PRIMARY KEY` declarations. This nonnullable default changes the source schema semantics, so check it against the source case. The preset's `allow_create_index_without_type=1` lets ClickHouse ignore an index without an explicit type, as a compatibility setting. It does not enforce `UNIQUE` constraints. Most SQLite index files contain `UNIQUE` indexes and cannot provide a trustworthy query comparison without that semantics.

See [experiment.md](experiment.md) for the measured results and reproductions from 2026-10-05.
