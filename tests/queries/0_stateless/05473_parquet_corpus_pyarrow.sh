#!/usr/bin/env bash
# Tags: long, no-fasttest
# no-fasttest: the fast test build has no Parquet support.
# long: thousands of queries over hundreds of files take more than the 180 s limit of the flaky check on sanitizer builds.

# Parquet files written by pyarrow 2.0, 8.0, 12.0, 17.0, and 26.0 with many combinations of writer options
# (format versions, data page v1/v2, codecs, encodings, page and row group sizes, statistics, page index,
# dictionary fallback, INT96, legacy list names, encryption). For every file the content that ClickHouse
# reads (all columns, with stress settings, each column and subcolumn alone, and with filters) is compared
# with what pyarrow itself read back from the file. See `data_lake_corpus/manifest.jsonl` for the
# provenance of every file and `utils/data-lake-corpus/generate.py` for how the corpus is generated.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

"$CUR_DIR"/data_lake_corpus/run_checks.sh "$CLICKHOUSE_LOCAL" "$CUR_DIR"/data_lake_corpus/checks_pyarrow.sql
