#!/usr/bin/env bash

# `checksums.txt` of the oldest format lists no files. The size of a skip index of such a part used to be
# computed from the empty checksums anyway, which threw a `LOGICAL_ERROR` on the first query to the table.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORKING_DIR="${CLICKHOUSE_TMP}/05401_skip_index_size_of_part_with_empty_checksums"
rm -rf "${WORKING_DIR}"
mkdir -p "${WORKING_DIR}"

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" -q "
    CREATE TABLE t (id UInt64, v UInt64, INDEX idx v TYPE minmax) ENGINE = MergeTree ORDER BY id
    SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
    INSERT INTO t SELECT number, number % 10 FROM numbers(5000);
" </dev/null

PART_PATH=$(${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" -q "SELECT path FROM system.parts WHERE database = currentDatabase() AND table = 't'" </dev/null)
echo "checksums format version: 1" > "${PART_PATH}/checksums.txt"

${CLICKHOUSE_LOCAL} --path "${WORKING_DIR}" -q "
    SELECT count() FROM t;
    SELECT name, rows, secondary_indices_compressed_bytes FROM system.parts WHERE database = currentDatabase() AND table = 't';
" </dev/null

rm -rf "${WORKING_DIR}"
