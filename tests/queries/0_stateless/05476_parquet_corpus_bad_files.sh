#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the fast test build has no Parquet support.

# Files that cannot be read as written, which ClickHouse must reject with an error (not a crash or a
# `LOGICAL_ERROR`) or read: the fuzzer reproducers of https://github.com/apache/arrow-testing, files that
# writers produce out of the format specification, encrypted files, and files with types without a common
# reference. Every file is read completely; the output is the row count and a hash, or the error code name.
# See `data_lake_corpus/manifest.jsonl` for the provenance and notes of every file.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

"$CUR_DIR"/data_lake_corpus/run_checks.sh "$CLICKHOUSE_LOCAL" "$CUR_DIR"/data_lake_corpus/checks_smoke.sql --max_threads=1 --max_memory_usage=1G
