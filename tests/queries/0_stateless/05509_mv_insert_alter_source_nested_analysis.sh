#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# ALTER of the source column type between two blocks of one INSERT, as in 05241. The view queries
# read the source table in a nested analysis, which swaps in the pushed block. So the nested
# analysis must use the types of the block, not the table's current metadata:
#  - `mv_merge`: an `IN` subquery in the child query of `merge`, which is analyzed again for each
#    child table because of the `Distributed` sibling.
#  - `mv_policy`: the row policy of an ordinary view, which is analyzed separately when the view is
#    inlined (`analyzer_inline_views`).

# `dist` makes `merge` analyze its child query again. Its row matches no set: depending on the
# settings, the shard runs the `IN` subquery on its own, empty `src` or gets the set of the block.
function run()
{
    local inline_views=$1

    $CLICKHOUSE_CLIENT -q "
        DROP TABLE IF EXISTS mv_merge;
        DROP TABLE IF EXISTS mv_policy;
        DROP ROW POLICY IF EXISTS p ON v;
        DROP TABLE IF EXISTS v;
        DROP TABLE IF EXISTS dst_merge;
        DROP TABLE IF EXISTS dst_policy;
        DROP TABLE IF EXISTS dist;
        DROP TABLE IF EXISTS child_local;
        DROP TABLE IF EXISTS child;
        DROP TABLE IF EXISTS src;

        CREATE TABLE src (arr Array(Tuple(a UInt32))) ENGINE = Null;
        CREATE TABLE child (k UInt64) ENGINE = MergeTree ORDER BY k;
        CREATE TABLE child_local (k UInt64) ENGINE = MergeTree ORDER BY k;
        CREATE TABLE dist (k UInt64) ENGINE = Distributed(test_shard_localhost, currentDatabase(), child_local);
        INSERT INTO child VALUES (1);
        INSERT INTO child_local VALUES (5);

        CREATE VIEW v AS SELECT k FROM child;
        CREATE ROW POLICY p ON v USING (SELECT any(toTypeName(arr)) FROM ${CLICKHOUSE_DATABASE}.src) NOT LIKE '%Nullable%' TO ALL;

        CREATE TABLE dst_merge (cnt UInt64) ENGINE = MergeTree ORDER BY tuple();
        CREATE TABLE dst_policy (cnt UInt64) ENGINE = MergeTree ORDER BY tuple();

        CREATE MATERIALIZED VIEW mv_merge TO dst_merge AS
        SELECT
            (SELECT count() FROM merge(currentDatabase(), '^child\$|^dist\$')
             WHERE k IN (SELECT count() FROM src WHERE arrayExists(x -> x.a >= 0, arr))) AS cnt
        FROM src;

        CREATE MATERIALIZED VIEW mv_policy TO dst_policy AS
        SELECT count() AS cnt FROM src CROSS JOIN v;
    "

    # One row per block, 0.2 s apart, so the INSERT lasts about 6 s.
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO src
        SELECT [tuple(toUInt32(number))]
        FROM numbers(30)
        WHERE NOT ignore(sleepEachRow(0.2))
        SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1, max_threads = 1, max_insert_threads = 1,
            analyzer_inline_views = $inline_views
    " &
    local insert_pid=$!

    # Wait until the first block has passed through the views, so the INSERT surely started with the old type.
    while [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM dst_policy") -lt 1 ]] && kill -0 $insert_pid 2>/dev/null
    do
        sleep 0.1
    done

    $CLICKHOUSE_CLIENT -q "ALTER TABLE src MODIFY COLUMN arr Array(Tuple(a UInt32, b Nullable(UInt32)))"
    wait

    # Each block matches the row of `child` and the row of `v`.
    $CLICKHOUSE_CLIENT -q "SELECT 'analyzer_inline_views = $inline_views', (SELECT count(), sum(cnt) FROM dst_merge), (SELECT count(), sum(cnt) FROM dst_policy)"
}

run 0
run 1

$CLICKHOUSE_CLIENT -q "
    DROP TABLE mv_merge;
    DROP TABLE mv_policy;
    DROP ROW POLICY p ON v;
    DROP TABLE v;
    DROP TABLE dst_merge;
    DROP TABLE dst_policy;
    DROP TABLE dist;
    DROP TABLE child_local;
    DROP TABLE child;
    DROP TABLE src;
"
