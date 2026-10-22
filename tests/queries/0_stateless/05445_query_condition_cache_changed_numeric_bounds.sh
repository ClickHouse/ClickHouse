#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: the query condition cache is populated per replica

# The query condition cache serves a condition whose numeric bounds changed since it was cached: a granule
# recorded as not matching an earlier variant of the condition is skipped unless the primary key shows that
# it may have rows admitted by the new bounds but not by the earlier ones. With narrower bounds, the primary
# key is not needed.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 123 granules, a single part. The only matching rows (`num = 1`) are in the granules of the rows 500999
# and 501000. Only the first one has a high `price`, `amount` and `qty`.
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS tab;
    CREATE TABLE tab (key UInt64, price Float64, amount Decimal(10, 2), qty UInt32, num UInt64) ENGINE = MergeTree ORDER BY key
        SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0;
    INSERT INTO tab SELECT number, (number % 1000) / 10, (number % 1000) / 10, number % 1000, number IN (500999, 501000)
    FROM numbers(1000000)
    SETTINGS max_insert_threads = 1, max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
"

# Run a query and print its result, whether its result is the same without the query condition cache,
# and whether the number of granules it read is in the expected range. With the condition in `WHERE`,
# a query reusing the cache reads a few granules around the matching rows, like a repetition of the same
# query does: not every granule without matching rows in a block with matching rows is recorded.
function run()
{
    local name="$1"
    local condition="$2"
    local min_granules="$3"
    local max_granules="$4"

    local query="SELECT count() FROM tab WHERE ${condition}"
    local result
    result=$(${CLICKHOUSE_CLIENT} --query "${query} SETTINGS use_query_condition_cache = 1, enable_analyzer = 1, merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0 -- ${name}")
    local expected
    expected=$(${CLICKHOUSE_CLIENT} --query "${query} SETTINGS use_query_condition_cache = 0")

    ${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
    local granules
    granules=$(${CLICKHOUSE_CLIENT} --query "
        SELECT intDiv(read_rows + 8191, 8192)
        FROM system.query_log
        WHERE event_date >= yesterday() AND event_time >= now() - 600
            AND type = 'QueryFinish'
            AND current_database = currentDatabase()
            AND endsWith(query, '-- ${name}')
        ORDER BY event_time_microseconds DESC
        LIMIT 1")

    local granules_as_expected=0
    if [ "${granules}" -ge "${min_granules}" ] && [ "${granules}" -le "${max_granules}" ]; then
        granules_as_expected=1
    fi
    if [ "${granules_as_expected}" == 0 ]; then
        echo "${name}: read ${granules} granules, expected from ${min_granules} to ${max_granules}"
    fi

    echo "${name}: result ${result}, same as without cache $([ "${result}" == "${expected}" ] && echo 1 || echo 0), granules as expected ${granules_as_expected}"
}

# Bounds on the primary key: 98 granules, then the bounds moved by 10 rows.
run "key prime" "key > 100000 AND key < 900000 AND num = 1" 98 98
run "key shifted" "key > 100010 AND key < 900010 AND num = 1" 1 12
# The band contains the matching rows.
run "key before matches" "key > 100000 AND key < 400000 AND num = 1" 1 12
run "key including matches" "key > 100000 AND key < 600000 AND num = 1" 25 40

# A threshold on a column outside of the primary key: only narrower thresholds reuse the cache.
run "float prime" "price > 50.5 AND num = 1" 123 123
run "float narrower" "price > 60.5 AND num = 1" 1 12
run "float wider" "price > 40 AND num = 1" 123 123

# The literals `100` and `300` have different types (`UInt8` and `UInt16`).
run "integer prime" "qty > 100 AND num = 1" 123 123
run "integer narrower" "qty > 300 AND num = 1" 1 12

run "decimal prime" "amount >= 50.25 AND num = 1" 123 123
run "decimal narrower" "amount >= 60.75 AND num = 1" 1 12

# A comparison with NaN is never true, so it is not a bound: it must not reuse the entries of other
# thresholds, nor provide entries for them.
run "nan prime" "price > 10 OR num = 1" 123 123
run "nan" "price > nan OR num = 1" 123 123
run "nan reversed prime" "price < nan OR num = 1" 123 123
run "nan reversed" "price < 0 OR num = 1" 123 123

${CLICKHOUSE_CLIENT} --query "DROP TABLE tab"
