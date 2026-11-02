#!/usr/bin/env bash
# Tags: no-ordinary-database, no-replicated-database
# Tag no-ordinary-database: the table needs a UUID
# Tag no-replicated-database: an explicit UUID is not allowed in a Replicated database

# A table created again with the same UUID names its parts like the dropped one did,
# and must not reuse the query condition cache entries of those parts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

function create()
{
    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS tab SYNC;
        CREATE TABLE tab UUID '$1' (a UInt64) ENGINE = MergeTree ORDER BY tuple()
            SETTINGS index_granularity = 8, index_granularity_bytes = '10Mi';"
}

# use_statistics_for_part_pruning = 0: the statistics would skip the part with no match before the cache records it.
# use_uncompressed_cache = 0: the uncompressed cache can return the blocks of the dropped table's files, which had the same paths.
function query()
{
    for prewhere in 0 1; do
        $CLICKHOUSE_CLIENT -q "SELECT count() FROM tab WHERE a < 50
            SETTINGS use_query_condition_cache = 1, use_statistics_for_part_pruning = 0, use_uncompressed_cache = 0,
                optimize_move_to_prewhere = $prewhere"
    done
}

uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
create "$uuid"
$CLICKHOUSE_CLIENT -q "INSERT INTO tab SELECT number + 100 FROM numbers(100)"
query
# The same number of granules, half of the rows match.
create "$uuid"
$CLICKHOUSE_CLIENT -q "INSERT INTO tab SELECT number FROM numbers(100)"
query

uuid=$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")
create "$uuid"
$CLICKHOUSE_CLIENT -q "INSERT INTO tab SELECT number + 100 FROM numbers(100)"
query
# More granules than the dropped part had.
create "$uuid"
$CLICKHOUSE_CLIENT -q "INSERT INTO tab SELECT number FROM numbers(1000)"
query

$CLICKHOUSE_CLIENT -q "DROP TABLE tab SYNC"
