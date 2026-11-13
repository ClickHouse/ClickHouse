#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: Iceberg needs Avro and Parquet, which the fasttest build lacks.

# Issue 129260: with `use_iceberg_table_statistics_ndv`, an Iceberg read takes the number of distinct values of a column
# that the manifests give none for from the `ndv` of its `apache-datasketches-theta-v1` blob in `metadata.json`, if the
# statistics belong to the snapshot that is read.
# Fixture written by Spark 4.0 / Iceberg 1.11: 3000 rows in one file, then `CALL system.compute_table_stats`. With
# id = 0..2999: k = id, g = (id % 10) * 10, s = 's' || id % 50, dec = -5 - id % 6 (Decimal(5, 2)), d = (id % 11) / 2
# (Float64), sid = id * 1000003, n = NULL for an even id, else 'n' || id % 7.
# - T1: the current snapshot has statistics: they give s, dec, d, sid and n; g and k keep the range of their bounds.
# - T2: T1 with the setting off: the estimator guesses 10% of the rows.
# - T3: after an append, the current snapshot has no statistics, so they are not used.
# - T4: time travel to the analyzed snapshot uses them again.
# - T5: an equality on a Decimal and a join on a String, with the setting on and off.
# Fixture r written by Trino 474: 300 rows (k = 0..299, s = 's' || k % 50, u = 'u' || k % 30) with statistics on write, then
# 20 rows (k = 300..319, s = 'x' || k, u = 'v' || k % 15 for an even k, else NULL) without, then `ANALYZE ... WITH (columns =
# ARRAY['u'])`, which keeps the blobs of k and s computed from the previous snapshot in the statistics file of the current one.
# - T6: only the blob of u is used for the current snapshot, clamped to the non-NULL rows of a pruned read; time travel to
#   the previous one uses all of its blobs.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

# The join order, the trace line, the labels and the Iceberg file layout depend on these; most are randomized.
PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=auto
    --use_hash_table_stats_for_join_reordering=0 --collect_hash_table_stats_during_joins=0
    --enable_join_runtime_filters=0 --enable_parallel_replicas=0 --enable_join_transitive_predicates=0
    --query_plan_propagate_predicate_across_join=0 --use_statistics=1 --materialize_statistics_on_insert=1
    --use_datalake_metadata_statistics=1 --explain_query_plan_default=legacy --max_insert_threads=1 --max_threads=1
    --max_block_size=1000000 --allow_insert_into_iceberg=1"
ON="--use_iceberg_table_statistics_ndv=1"
OFF="--use_iceberg_table_statistics_ndv=0"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"
cp -r "${CUR_DIR}/data_minio/table_statistics_db/ndv_table" "${LAKE}/ndv_table"
cp -r "${CUR_DIR}/data_minio/table_statistics_db/retained_table" "${LAKE}/retained_table"

# Prints the numbers of distinct values of the `Estimated statistics` trace line of a table. Usage: ndv <table> <columns>
# <filter> [client flags].
ndv()
{
    local table="$1" columns="$2" filter="$3"
    shift 3
    ${CLICKHOUSE_CLIENT_TRACE} ${PINS} "$@" --query "
        EXPLAIN SELECT ${columns} FROM ${table} JOIN dim ON ${table}.k = dim.k ${filter}" 2>&1 >/dev/null \
        | grep -oE "Estimated statistics for [A-Za-z]+ ${table}: .*" | head -n 1 \
        | grep -oE '__table1\.[a-z]+: [0-9]+( \[[^]]*\])?' | sed 's/__table1\.//' | sort
}
T_COLUMNS="t.k, t.g, t.s, t.dec, t.d, t.sid, t.n"

# Prints the `Join:` and `ResultRows:` lines of the logical plan. Usage: labels <query> [client flags].
labels()
{
    local query="$1"
    shift
    ${CLICKHOUSE_CLIENT} ${PINS} "$@" --query "
        SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1 ${query})
        WHERE explain LIKE '%Join: %' OR explain LIKE '%ResultRows: %'"
}

# `uniq` gives the dimensions an exact count and an NDV equal to their distinct values.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE t ENGINE = IcebergLocal('${LAKE}/ndv_table');
    CREATE TABLE dim (k Int64) ENGINE = MergeTree ORDER BY k SETTINGS auto_statistics_types = 'uniq';
    INSERT INTO dim SELECT number FROM numbers(10);
    CREATE TABLE dim_s (s String) ENGINE = MergeTree ORDER BY s SETTINGS auto_statistics_types = 'uniq';
    INSERT INTO dim_s SELECT concat('s', number) FROM numbers(10);"

echo '--- fixture: data files and rows; distinct values of s, dec, d, sid, n, g'
${CLICKHOUSE_CLIENT} --query "
    SELECT count(), sum(record_count) FROM system.iceberg_files WHERE database = currentDatabase() AND table = 't'"
${CLICKHOUSE_CLIENT} --query "SELECT uniqExact(s), uniqExact(dec), uniqExact(d), uniqExact(sid), uniqExact(n), uniqExact(g) FROM t"
echo '--- fixture: the statistics of metadata.json are of the current snapshot; field id and ndv of each blob'
${CLICKHOUSE_CLIENT} --query "
    SELECT JSONExtractUInt(json, 'statistics', 1, 'snapshot-id') = JSONExtractUInt(json, 'current-snapshot-id'),
        length(JSONExtractArrayRaw(json, 'statistics'))
    FROM file('${LAKE}/ndv_table/metadata/v3.metadata.json', 'JSONAsString')"
${CLICKHOUSE_CLIENT} --query "
    SELECT JSONExtractString(blob, 'type'), JSONExtractInt(blob, 'fields', 1), JSONExtractString(blob, 'properties', 'ndv')
    FROM file('${LAKE}/ndv_table/metadata/v3.metadata.json', 'JSONAsString')
    ARRAY JOIN JSONExtractArrayRaw(json, 'statistics', 1, 'blob-metadata') AS blob"

echo '--- T1: the current snapshot has statistics'
echo 'Expect the theta NDV for d 11, dec 6, n 7, s 50, sid 3000; g 91 and k 3000 from the range of their bounds (theta: g 10).'
ndv t "${T_COLUMNS}" "" ${ON}

echo '--- T2: T1 with the setting off'
echo 'Expect 300, 10% of the 3000 rows, for d, dec, n, s, sid; g 91 and k 3000 as in T1.'
ndv t "${T_COLUMNS}" "" ${OFF}

echo '--- T1 at the default setting'
echo 'Expect T1: the setting is on by default.'
ndv t "${T_COLUMNS}" ""

echo '--- T5: dec = -5'
echo 'Expect ~500: dec has 6 values, so 3000 / 6; off ~30, the default 1% (true 500).'
labels "SELECT count() FROM t JOIN dim ON t.k = dim.k WHERE t.dec = -5" ${ON} | grep 'Join: '
labels "SELECT count() FROM t JOIN dim ON t.k = dim.k WHERE t.dec = -5" ${OFF} | grep 'Join: '
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t WHERE dec = -5"

echo '--- T5: join on s with 10 of its values'
echo 'Expect 600: 3000 * 10 / max(50, 10); off 100, by the guessed 300 (true 600).'
labels "SELECT count() FROM t JOIN dim_s ON t.s = dim_s.s" ${ON}
labels "SELECT count() FROM t JOIN dim_s ON t.s = dim_s.s" ${OFF}
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t JOIN dim_s ON t.s = dim_s.s"

# A new snapshot of 1000 rows, without statistics; the statistics of the first one stay in metadata.json.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    INSERT INTO t SELECT number + 3000, ((number + 3000) % 10) * 10, concat('x', number % 200), -5, 0.25, number, NULL
    FROM numbers(1000)"

echo '--- T3: the current snapshot has no statistics'
echo 'Expect 400, 10% of the 4000 rows, for d, dec, n, s, sid: the statistics are of the previous snapshot.'
ndv t "${T_COLUMNS}" "" ${ON}

FIRST_SNAPSHOT=$(${CLICKHOUSE_CLIENT} --query "
    SELECT snapshot_id FROM system.iceberg_history WHERE database = currentDatabase() AND table = 't' AND parent_id = 0")

echo '--- T4: time travel to the analyzed snapshot'
echo 'Expect T1 again.'
ndv t "${T_COLUMNS}" "" ${ON} --iceberg_snapshot_id="${FIRST_SNAPSHOT}"

${CLICKHOUSE_CLIENT} ${PINS} --query "CREATE TABLE r ENGINE = IcebergLocal('${LAKE}/retained_table')"

echo '--- fixture r: rows, distinct values of s and u; blobs of the current statistics file: computed from the current snapshot, field id, ndv'
${CLICKHOUSE_CLIENT} --query "SELECT count(), uniqExact(s), uniqExact(u) FROM r"
${CLICKHOUSE_CLIENT} --query "
    SELECT JSONExtractUInt(blob, 'snapshot-id') = current, JSONExtractInt(blob, 'fields', 1), JSONExtractString(blob, 'properties', 'ndv')
    FROM
    (
        SELECT JSONExtractUInt(json, 'current-snapshot-id') AS current, arrayJoin(JSONExtractArrayRaw(json, 'statistics')) AS stats_file
        FROM file('${LAKE}/retained_table/metadata/00004-*.metadata.json', 'JSONAsString')
    )
    ARRAY JOIN JSONExtractArrayRaw(stats_file, 'blob-metadata') AS blob
    WHERE JSONExtractUInt(stats_file, 'snapshot-id') = current"

echo '--- T6: a statistics file with blobs of the previous snapshot'
echo 'Expect u 40 from its blob; s 32, 10% of the 320 rows, as its blob gives 50 of the previous snapshot (true 70); k 320 from its bounds.'
ndv r "r.k, r.s, r.u" "" ${ON}
echo 'k >= 300 prunes the read to the 20 rows of the second file, 10 of them with u: expect u 40 clamped to 10, s 2, k 20.'
ndv r "r.k, r.s, r.u" "WHERE r.k >= 300" ${ON} --use_iceberg_partition_pruning=1
echo 'Time travel to the previous snapshot: expect s 50 and u 30 from its blobs; k 300 from its bounds.'
ndv r "r.k, r.s, r.u" "" ${ON} --iceberg_snapshot_id=75667572448823849

rm -rf "${LAKE}"
