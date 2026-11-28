#!/usr/bin/env bash
# Tags: long, no-fasttest
# no-fasttest: the fast test build has no Parquet support.
# long: thousands of queries over hundreds of files take more than the 180 s limit of the flaky check on sanitizer builds.

# Parquet files written by other writers than pyarrow: Spark 3.5 (parquet-java), DuckDB 0.9, 1.1, and 2.0,
# polars 0.20 and 2.1 (polars-parquet), delta-rs (arrow-rs), arrow-go, parquet-go, and fastparquet, with
# their own option combinations. For every file the content that ClickHouse reads is compared with what the
# writer itself read back from the file. See `data_lake_corpus/manifest.jsonl` for the provenance of every
# file and `utils/data-lake-corpus/generate.py` for how the corpus is generated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

"$CUR_DIR"/data_lake_corpus/run_checks.sh "$CLICKHOUSE_LOCAL" "$CUR_DIR"/data_lake_corpus/checks_writers.sql
