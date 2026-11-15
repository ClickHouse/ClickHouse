#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Iceberg and Parquet support.

# Iceberg tables written by Spark 3.5 with the Iceberg runtime 1.12 and 1.4 (Hadoop catalog with
# `version-hint.text`) and by PyIceberg 0.12 (SQL catalog), in format versions 1, 2, and 3: all types,
# every partition transform, partition and schema evolution, position deletes, deletion vectors, copy-on-write
# rewrites, compaction, sort orders, gzip-compressed metadata, ORC, Avro and mixed data files, name mapping,
# and nanosecond timestamps. For every snapshot the content that ClickHouse reads (`iceberg_snapshot_id`
# for the old ones) is compared with what the writer read back. See `data_lake_corpus/manifest.jsonl`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

"$CUR_DIR"/data_lake_corpus/run_checks.sh "$CLICKHOUSE_LOCAL" "$CUR_DIR"/data_lake_corpus/checks_iceberg.sql
