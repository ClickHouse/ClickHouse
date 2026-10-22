#!/usr/bin/env bash
# Tags: no-parallel-replicas
# no-parallel-replicas: the query condition cache is populated per replica

# The query condition cache serves a condition whose time bounds changed since it was cached (issues #78502, #115504):
# a granule recorded as not matching an earlier variant of the condition is skipped unless the primary key
# shows that it may have rows admitted by the new bounds but not by the earlier ones.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# One row per millisecond from 2025-01-01 00:00:00 UTC (1735689600000 ms), 122 granules, a single part.
# The only matching rows are in the granule of the row at 500 seconds.
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS tab;
    CREATE TABLE tab (time DateTime64(3, 'UTC'), num UInt64) ENGINE = MergeTree ORDER BY time
        SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0;
    INSERT INTO tab SELECT fromUnixTimestamp64Milli(toInt64(1735689600000 + number), 'UTC'), number IN (500000, 500001)
    FROM numbers(1000000)
    SETTINGS max_insert_threads = 1, max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
"

function bound()
{
    echo "fromUnixTimestamp64Milli($((1735689600000 + $1)), 'UTC')"
}

# Run a query and print its result, whether its result is the same without the query condition cache,
# and whether the number of granules it read is in the expected range. With the condition in `WHERE`,
# a query reusing the cache reads a few granules around the matching rows, like a repetition of the same
# query does: not every granule without matching rows in a block with matching rows is recorded.
function run()
{
    local name="$1"
    local condition="$2"
    local settings="$3"
    local min_granules="$4"
    local max_granules="$5"

    local query="SELECT count() FROM tab WHERE ${condition}"
    local result
    result=$(${CLICKHOUSE_CLIENT} --query "${query} SETTINGS use_query_condition_cache = 1, enable_analyzer = 1, merge_tree_min_rows_for_seek = 0, merge_tree_min_bytes_for_seek = 0 ${settings} -- ${name}")
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

function scenarios()
{
    local prefix="$1"
    local settings="$2"

    # Prime the cache: the bounds select 98 granules, which are all read.
    run "${prefix} prime" "time >= $(bound 100000) AND time <= $(bound 900000) AND num = 1" "${settings}" 98 98
    # Both bounds moved by 3 ms: only the granule with matches and the granule at the new upper bound are read.
    run "${prefix} shifted" "time >= $(bound 100003) AND time <= $(bound 900003) AND num = 1" "${settings}" 1 12
    # Narrower bounds: only the granule with matches.
    run "${prefix} narrower" "time >= $(bound 150000) AND time <= $(bound 850000) AND num = 1" "${settings}" 1 12
    # Wider bounds: also the granules between the earlier and the new bounds.
    run "${prefix} wider" "time >= $(bound 80000) AND time <= $(bound 920000) AND num = 1" "${settings}" 1 35
    # The same bounds as a string, compared with the `DateTime64` column.
    run "${prefix} string" "time >= '2025-01-01 00:01:20.000' AND time <= '2025-01-01 00:15:20.000' AND num = 1" "${settings}" 104 104
    run "${prefix} string shifted" "time >= '2025-01-01 00:01:20.005' AND time <= '2025-01-01 00:15:20.005' AND num = 1" "${settings}" 1 12
    # Under NOT, the direction of the bounds flips.
    run "${prefix} not" "NOT (time < $(bound 100000) OR time > $(bound 900000) OR num != 1)" "${settings}" 98 98
    run "${prefix} not shifted" "NOT (time < $(bound 100003) OR time > $(bound 900003) OR num != 1)" "${settings}" 1 12
    # A granule recorded as not matching an earlier variant is read if it has rows in the band: the earlier
    # bounds exclude the matching rows, the new ones include them.
    run "${prefix} before matches" "time >= $(bound 100000) AND time <= $(bound 400000) AND num = 1" "${settings}" 1 12
    run "${prefix} including matches" "time >= $(bound 100000) AND time <= $(bound 600000) AND num = 1" "${settings}" 25 40
    # Other conditions do not share entries.
    run "${prefix} other condition" "time >= $(bound 100003) AND time <= $(bound 900003) AND num = 0" "${settings}" 98 98
}

scenarios "where" ", optimize_move_to_prewhere = 0"
${CLICKHOUSE_CLIENT} --query "TRUNCATE TABLE tab"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO tab SELECT fromUnixTimestamp64Milli(toInt64(1735689600000 + number), 'UTC'), number IN (500000, 500001)
    FROM numbers(1000000)
    SETTINGS max_insert_threads = 1, max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
"
scenarios "prewhere" ", optimize_move_to_prewhere = 1"

# Disabled: the shifted bounds do not use the entries of the earlier ones.
run "disabled" "time >= $(bound 100006) AND time <= $(bound 900006) AND num = 1" ", use_query_condition_cache_for_time_conditions = 0" 98 98

# A window relative to the current time moves on both sides with every query.
${CLICKHOUSE_CLIENT} --query "
    TRUNCATE TABLE tab;
    INSERT INTO tab SELECT now64(3, 'UTC') - toIntervalMillisecond(number), number IN (500000, 500001)
    FROM numbers(1000000)
    SETTINGS max_insert_threads = 1, max_block_size = 1000000, min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0;
"
run "now prime" "time >= now64(3, 'UTC') - INTERVAL 900 SECOND AND time <= now64(3, 'UTC') - INTERVAL 100 SECOND AND num = 1" "" 80 100
run "now moved" "time >= now64(3, 'UTC') - INTERVAL 900 SECOND AND time <= now64(3, 'UTC') - INTERVAL 100 SECOND AND num = 1" "" 1 12

${CLICKHOUSE_CLIENT} --query "DROP TABLE tab"
