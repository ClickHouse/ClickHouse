#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 120440: with `use_iceberg_manifest_statistics`, the conditions of an Iceberg read (the filter above it, its
# `PREWHERE` and row policy) are estimated on the column statistics of its remaining data files, as for MergeTree tables
# with column statistics.
# - T1: filters that prune no file. Equality and `IN` on an integer divide by its counted NDV, `IS NULL` uses the NULL
#   count, a range interpolates the bounds, a string equality gets the default factor.
# - T2: filters that prune files: the selectivity applies to the remaining files.
# - T3: a filter on an identity-partitioned string keeps the rows of its partition.
# - T4: a filter on a 5-row table, which also reaches the table joined on its key, no longer hides the rows of the joins
#   above it: the plan and the rows put into hash tables, with the manifest statistics off and on.
# - T5: a filter above a `LIMIT` stays out of the read, and is still estimated, then capped by the `LIMIT`.
# - T6: a row policy prunes and is estimated.
# - T7: a filter right above the read is also the read's own filter, and is counted once.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The join order, the labels and the Iceberg file layout depend on these; most are randomized.
PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=auto
    --use_hash_table_stats_for_join_reordering=0 --collect_hash_table_stats_during_joins=0
    --enable_join_runtime_filters=0 --enable_parallel_replicas=0 --enable_join_transitive_predicates=0
    --query_plan_propagate_predicate_across_join=0 --use_statistics=1 --materialize_statistics_on_insert=1
    --explain_query_plan_default=legacy --max_insert_threads=1 --max_threads=1 --max_block_size=1000000
    --allow_insert_into_iceberg=1"
ON="--use_iceberg_manifest_statistics=1"
OFF="--use_iceberg_manifest_statistics=0"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"

# Prints the `Join:` line of the plan of a join of <relation> with `dim`, then the rows the relation returns.
# Usage: labels <relation> <key> <filter> [client flags].
labels()
{
    local relation="$1" key="$2" filter="$3"
    shift 3
    ${CLICKHOUSE_CLIENT} ${PINS} "$@" --query "
        SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1
            SELECT count() FROM ${relation} AS t JOIN dim ON t.${key} = dim.v WHERE ${filter})
        WHERE explain LIKE '%Join: %'"
    echo "rows: $(${CLICKHOUSE_CLIENT} ${PINS} --query "SELECT count() FROM ${relation} AS t WHERE ${filter}")"
}

# t: 4 data files of 2500 rows with disjoint k and d ranges; v has 5 values, n is 90% NULL, s is 'rare' in 2% of rows.
# t_rp: the files of t, read under a row policy. p: identity-partitioned by a string, one data file per partition
# (us: 3000 rows, eu: 7000).
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE t (k Int64, v Int32, s String, n Nullable(Int32), d Date) ENGINE = IcebergLocal('${LAKE}/t');
    INSERT INTO t SELECT number, number % 5, if(number % 50 = 0, 'rare', 'common'), if(number % 10 = 0, number, NULL),
        toDate('2024-01-01') + intDiv(number, 100) FROM numbers(0, 2500);
    INSERT INTO t SELECT number, number % 5, if(number % 50 = 0, 'rare', 'common'), if(number % 10 = 0, number, NULL),
        toDate('2024-01-01') + intDiv(number, 100) FROM numbers(2500, 2500);
    INSERT INTO t SELECT number, number % 5, if(number % 50 = 0, 'rare', 'common'), if(number % 10 = 0, number, NULL),
        toDate('2024-01-01') + intDiv(number, 100) FROM numbers(5000, 2500);
    INSERT INTO t SELECT number, number % 5, if(number % 50 = 0, 'rare', 'common'), if(number % 10 = 0, number, NULL),
        toDate('2024-01-01') + intDiv(number, 100) FROM numbers(7500, 2500);
    CREATE TABLE t_rp ENGINE = IcebergLocal('${LAKE}/t');
    CREATE ROW POLICY ${CLICKHOUSE_DATABASE}_rp ON ${CLICKHOUSE_DATABASE}.t_rp USING k < 1000 TO ALL;
    CREATE TABLE p (k Int64, r String) ENGINE = IcebergLocal('${LAKE}/p') PARTITION BY (r);
    INSERT INTO p SELECT number, if(number % 10 < 3, 'us', 'eu') FROM numbers(10000);
    CREATE TABLE dim (v Int32) ENGINE = MergeTree ORDER BY v SETTINGS auto_statistics_types = 'uniq';
    INSERT INTO dim SELECT number FROM numbers(5);
"

echo '--- fixture: data files and rows per Iceberg table'
${CLICKHOUSE_CLIENT} --query "
    SELECT table, content, count(), sum(record_count) FROM system.iceberg_files
    WHERE database = currentDatabase() AND table IN ('t', 'p') GROUP BY table, content ORDER BY table, content"

echo '--- T1: v = 3, every file overlaps'
echo 'Expect ~2000: v is 0..4, so its counted NDV is 5, and equality keeps 10000 / 5 rows.'
labels t v 't.v = 3' ${ON}
echo '--- T1: v IN (1, 2)'
echo 'Expect ~4000: 2 of the 5 values of v, so 10000 * 2 / 5.'
labels t v 't.v IN (1, 2)' ${ON}
echo '--- T1: n IS NULL'
echo 'Expect ~9000: the NULL counts of the manifests, 9000 of the 10000 rows.'
labels t v 't.n IS NULL' ${ON}
echo '--- T1: k BETWEEN 1000 AND 8999, every file overlaps'
echo 'Expect ~7999: interpolated between min 0 and max 9999, so about 8000 of the 10000 rows.'
labels t v 't.k BETWEEN 1000 AND 8999' ${ON}
echo "--- T1: s = 'rare'"
echo 'Expect ~100: a string has no counted NDV, so equality takes the default 1% of 10000 (true 200).'
labels t v "t.s = 'rare'" ${ON}

echo '--- T2: k < 1000, 1 of 4 files remains'
echo 'Expect ~999: the remaining file holds k 0..2499 in 2500 rows, interpolated to 2500 * 1000 / 2500.'
labels t v 't.k < 1000' ${ON}
echo "--- T2: d = '2024-01-05', 1 of 4 files remains"
echo 'Expect ~100: the remaining file holds 25 days in 2500 rows, so 2500 / 25.'
labels t v "t.d = '2024-01-05'" ${ON}

echo "--- T3: r = 'us', the eu partition is pruned"
echo 'Expect ~3000: the us partition holds 3000 rows and one partition value, so 3000 / 1.'
labels p k "t.r = 'us'" ${ON}

# T4 tables: l -> o -> c -> g, every key dense; g is filtered.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE l (lo Int64) ENGINE = IcebergLocal('${LAKE}/l');
    INSERT INTO l SELECT number % 10000 + 1 FROM numbers(30000);
    CREATE TABLE o (id Int64, oc Int64) ENGINE = IcebergLocal('${LAKE}/o');
    INSERT INTO o SELECT number + 1, number % 1000 + 1 FROM numbers(10000);
    CREATE TABLE c (id Int64, cg Int32) ENGINE = IcebergLocal('${LAKE}/c');
    INSERT INTO c SELECT number + 1, number % 5 FROM numbers(1000);
    CREATE TABLE g (id Int32) ENGINE = IcebergLocal('${LAKE}/g');
    INSERT INTO g SELECT number FROM numbers(5);
"
T4="SELECT count() FROM l JOIN o ON l.lo = o.id JOIN c ON o.oc = c.id JOIN g ON c.cg = g.id WHERE g.id = 2"
echo '--- T4: plan, manifest statistics off'
echo 'Expect no row estimates: without the manifest statistics the Iceberg reads have none, so neither have the joins above them.'
${CLICKHOUSE_CLIENT} ${PINS} ${OFF} --query "
    SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1 ${T4}) WHERE explain LIKE '%Join: %'"
echo '--- T4: plan, manifest statistics on'
echo 'Expect g ~1 (5 rows / 5 ids) and c ~200 (1000 rows / 5 values of cg), joined first.'
${CLICKHOUSE_CLIENT} ${PINS} ${ON} --query "
    SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1 ${T4}) WHERE explain LIKE '%Join: %'"
echo '--- T4: result, manifest statistics off and on'
echo 'Expect 6000 both times.'
${CLICKHOUSE_CLIENT} ${PINS} ${OFF} --query_id="${CLICKHOUSE_DATABASE}_t4_off" --query "${T4}"
${CLICKHOUSE_CLIENT} ${PINS} ${ON} --query_id="${CLICKHOUSE_DATABASE}_t4_on" --query "${T4}"
${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
echo '--- T4: JoinBuildTableRowCount, manifest statistics off and on'
echo 'Expect off: g (1) + o (10000) + l x o (30000) = 40001; on: g (1) + c x g (200) + o x c x g (2000) = 2201.'
${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(query_id, currentDatabase() || '_t4_', ''), ProfileEvents['JoinBuildTableRowCount']
    FROM system.query_log
    WHERE event_date >= yesterday() AND type = 'QueryFinish' AND current_database = currentDatabase()
        AND startsWith(query_id, currentDatabase() || '_t4_')
    ORDER BY query_id"

echo '--- T5: v = 3 above LIMIT 5000'
echo 'Expect ~2000: v = 3 on the 10000 rows, below the limit (true 1000: the limit takes 2 whole files first).'
labels '(SELECT * FROM t LIMIT 5000)' v 't.v = 3' ${ON}

echo '--- T6: row policy k < 1000, 1 of 4 files remains'
echo 'Expect ~999: as the filter k < 1000 in T2.'
labels t_rp v 't.v >= 0' ${ON}

echo '--- T7: k % 2 = 0, right above the read'
echo 'Expect ~100: the default 1% of 10000 rows for a condition that is not a range, once (true 5000).'
labels t v 't.k % 2 = 0' ${ON}

${CLICKHOUSE_CLIENT} --query "DROP ROW POLICY ${CLICKHOUSE_DATABASE}_rp ON ${CLICKHOUSE_DATABASE}.t_rp"
rm -rf "${LAKE}"
