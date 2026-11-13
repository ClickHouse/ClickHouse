#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# Delta Lake tables written by Spark 4.1 with Delta Lake 4.4 and Spark 3.5 with Delta Lake 3.3, and by delta-rs 1.6:
# all types, partition values with special characters and nulls, deletion vectors (also with checkpoints, column
# mapping, MERGE, and RESTORE), column mapping in `id` and `name` modes with renamed and dropped (nested) columns,
# schema evolution, type widening (also of nested and partition columns), v2 checkpoints with JSON and Parquet
# top-level files and sidecars, multi-part checkpoints, row tracking, in-commit timestamps, liquid clustering,
# `icebergCompatV2`, identity, generated and default columns with CHECK constraints, and the change data feed with
# deletion vectors and column mapping. For every version the content that ClickHouse reads
# (`delta_lake_snapshot_version` for the old ones) is compared with what the writer read back, and the change data
# feed (`delta_lake_snapshot_start_version`) with the writer's `table_changes`. See `data_lake_corpus/manifest.jsonl`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

"$CUR_DIR"/data_lake_corpus/run_checks.sh "$CLICKHOUSE_LOCAL" "$CUR_DIR"/data_lake_corpus/checks_delta.sql
