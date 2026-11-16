#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs Parquet

# A single local Parquet file, that would be read by several sources in parallel, is still renamed according to
# `rename_files_after_processing` when the query condition cache knows that none of its row groups match.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_DATABASE}_05452"
DATA_DIR="${USER_FILES_PATH:?}/${DIR}"
rm -rf "${DATA_DIR:?}"
mkdir -p "$DATA_DIR"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${DIR}/data.parquet', Parquet, 'x UInt64') SELECT number * 2 FROM numbers(8000)
    SETTINGS output_format_parquet_row_group_size = 1000, output_format_parquet_use_custom_encoder = 1"
# A settled version token: the query condition cache is used only for files modified a while ago.
touch -d '2020-01-01 00:00:00' "$DATA_DIR/data.parquet"

SETTINGS="max_threads = 4, parallelize_output_from_storages = 1, input_format_parquet_preserve_order = 0, use_query_condition_cache = 1"

# Fill the query condition cache: no row group has an odd value.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM file('${DIR}/data.parquet') WHERE x = 5 SETTINGS $SETTINGS"
# The same condition with the warm cache, the file must be renamed.
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM file('${DIR}/data.parquet') WHERE x = 5 SETTINGS $SETTINGS, rename_files_after_processing = 'processed_%a'"

ls "$DATA_DIR"
${CLICKHOUSE_CLIENT} --query "SELECT count(), sum(x) FROM file('${DIR}/processed_data.parquet')"

rm -rf "${DATA_DIR:?}"
