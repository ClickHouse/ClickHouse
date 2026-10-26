#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# `JSON` is a format that cannot be appended to, so a second insert with `engine_file_allow_create_multiple_files`
# steps aside from the non-empty file into a new numbered one. If creating the sink fails after that file has been
# opened - here, the compressing wrapper rejects `output_format_compression_zstd_window_log = 1` - the empty file is
# removed: otherwise a glob pattern over the directory would read it.

DIR="${CLICKHOUSE_TMP}/05448_split_on_write_failed_sink_construction"
rm -rf "${DIR}"
mkdir -p "${DIR}"

SETTINGS="engine_file_split_on_write_by_size_bytes = 1000000000, engine_file_allow_create_multiple_files = 1"

echo '--- Partitioned insert'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/p_{_partition_id}_part.json.zst', JSON, 'p UInt64, x UInt64', 'zstd') PARTITION BY p
        SELECT 0 AS p, 1 AS x SETTINGS ${SETTINGS};
"
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/p_{_partition_id}_part.json.zst', JSON, 'p UInt64, x UInt64', 'zstd') PARTITION BY p
        SELECT 0 AS p, 2 AS x SETTINGS ${SETTINGS}, output_format_compression_zstd_window_log = 1;
" 2>&1 | grep -o -m1 'ILLEGAL_CODEC_PARAMETER'
ls "${DIR}" | LC_ALL=C sort
${CLICKHOUSE_LOCAL} --query "SELECT p, x FROM file('${DIR}/p_*.json.zst', JSON, 'p UInt64, x UInt64') ORDER BY ALL"

echo '--- Non-partitioned insert'
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/data.json.zst', JSON, 'x UInt64', 'zstd') SELECT 1 AS x SETTINGS ${SETTINGS};
"
${CLICKHOUSE_LOCAL} --query "
    INSERT INTO FUNCTION file('${DIR}/data.json.zst', JSON, 'x UInt64', 'zstd') SELECT 2 AS x
        SETTINGS ${SETTINGS}, output_format_compression_zstd_window_log = 1;
" 2>&1 | grep -o -m1 'ILLEGAL_CODEC_PARAMETER'
ls "${DIR}" | LC_ALL=C sort
${CLICKHOUSE_LOCAL} --query "SELECT x FROM file('${DIR}/data*.json.zst', JSON, 'x UInt64') ORDER BY ALL"

rm -rf "${DIR}"
