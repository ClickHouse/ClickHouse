#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: ORC is not available in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Boundary cases that previously wrapped around an unsigned integer (which UBSan now reports).

echo "-- nullable floating-point columns of ORC"
$CLICKHOUSE_LOCAL -q "
SELECT if(number = 0, NULL, toFloat64(number)) AS x, if(number = 1, NULL, toFloat32(number)) AS y FROM numbers(3)
FORMAT ORC" | $CLICKHOUSE_LOCAL --input-format ORC -q "SELECT * FROM table"

echo "-- page_cache_block_size of UInt64 max"
CONFIG_FILE="${CLICKHOUSE_TMP}/page_cache_config_${CLICKHOUSE_DATABASE}.yaml"
> "${CONFIG_FILE}" echo "
page_cache_max_size: 134217728
"
$CLICKHOUSE_LOCAL --config-file "${CONFIG_FILE}" -q "
CREATE TABLE t (x UInt64) ENGINE = MergeTree ORDER BY x;
INSERT INTO t SELECT number FROM numbers(100);
SELECT sum(x) FROM t
SETTINGS use_reader_executor = 0, use_page_cache_for_local_disks = 1, local_filesystem_read_method = 'pread',
    page_cache_block_size = 18446744073709551615, page_cache_lookahead_blocks = 1,
    use_uncompressed_cache = 0, use_columns_cache = 0, use_statistics_for_sum_avg_aggregation = 0;" 2>&1 | grep -o -m1 -E '^4950$|INVALID_SETTING_VALUE'
rm "${CONFIG_FILE}"
