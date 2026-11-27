#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel-rs is not in fast test
# Tag no-msan: delta-kernel-rs is not built with MSan

# Issue 129513: with `use_datalake_metadata_statistics`, a Delta Lake read is estimated for join reordering from the
# `add` actions of the log: the rows from `numRecords` less the cardinality of the deletion vectors, and the column
# statistics from `minValues`, `maxValues`, `nullCount` and the partition values.
# The tables written by Spark are copies of corpus tables (see `05560_delta_lake_corpus`).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

CLICKHOUSE_CLIENT_TRACE=${CLICKHOUSE_CLIENT/"--send_logs_level=${CLICKHOUSE_CLIENT_SERVER_LOGS_LEVEL}"/"--send_logs_level=trace"}

# The join order and the labels depend on these; most are randomized.
PINS="--query_plan_optimize_join_order_randomize=0 --query_plan_optimize_join_order_limit=10
    --query_plan_optimize_join_order_algorithm=greedy --query_plan_join_swap_table=auto
    --use_hash_table_stats_for_join_reordering=0 --collect_hash_table_stats_during_joins=0
    --enable_join_runtime_filters=0 --enable_parallel_replicas=0 --enable_join_transitive_predicates=0
    --query_plan_propagate_predicate_across_join=0 --use_statistics=1 --materialize_statistics_on_insert=1
    --explain_query_plan_default=legacy --max_insert_threads=1 --max_threads=1
    --allow_experimental_delta_kernel_rs=1 --allow_delta_lake_writes=1 --allow_delta_lake_create_table=1"
ON="--use_datalake_metadata_statistics=1 --session_timezone=UTC"

LAKE="${CLICKHOUSE_USER_FILES_UNIQUE}"
rm -rf "${LAKE}"
mkdir -p "${LAKE}"
for table in deletion_vectors partitioned all_types column_mapping_id
do
    cp -r "$CUR_DIR/data_lake_corpus/delta/$table" "${LAKE}/$table"
done

# Prints the `Join:` line of the logical plan, then the rows the read returns.
# Usage: labels <settings> <table> [<filter>] [<join key>].
labels()
{
    local settings="$1"
    local table="$2"
    local filter="${3:-1}"
    local key="${4:-id}"
    ${CLICKHOUSE_CLIENT} ${PINS} ${settings} --query "
        SELECT trimLeft(explain) FROM (EXPLAIN keep_logical_steps = 1, actions = 1 SELECT count() FROM mt AS m JOIN ${table} AS t ON m.k = t.${key} WHERE ${filter})
        WHERE explain LIKE '%Join: %'"
    echo "live rows: $(${CLICKHOUSE_CLIENT} ${PINS} ${settings} --query "SELECT count() FROM ${table} AS t WHERE ${filter}")"
}

# Prints the rows and the number of distinct values of the join key that the optimizer gets for the read.
# Usage: distinct_values <table> <join key>.
distinct_values()
{
    ${CLICKHOUSE_CLIENT_TRACE} ${PINS} ${ON} --query "EXPLAIN SELECT count() FROM mt AS m JOIN $1 AS t ON m.k = t.$2" 2>&1 >/dev/null \
        | grep -oE 'Estimated statistics for [A-Za-z]+ t: .*' | head -n 1 | grep -oE "t: [0-9]+ rows|__table[0-9]+\.$2: [0-9]+"
}

# written: 2 data files written by ClickHouse, which gives only `numRecords`.
${CLICKHOUSE_CLIENT} ${PINS} --query "
    CREATE TABLE mt (k Int64, x Int64) ENGINE = MergeTree ORDER BY k
        SETTINGS index_granularity = 8192, auto_statistics_types = 'uniq';
    INSERT INTO mt SELECT number, number FROM numbers(1000);
    CREATE TABLE written (id Int64, v Int64) ENGINE = DeltaLakeLocal('${LAKE}/written', Parquet);
    INSERT INTO written SELECT number, number % 5 FROM numbers(100);
    INSERT INTO written SELECT number, number % 5 FROM numbers(100, 50);
"

# wide_decimal: 1 data file of 10 rows whose `d` bounds do not fit in 64 bits, with a hand-written log.
mkdir -p "${LAKE}/wide_decimal/_delta_log"
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${LAKE}/wide_decimal/part-0.parquet', Parquet, 'id Nullable(Int64), d Nullable(Decimal(38, 0))')
    SELECT number, toDecimal128('123456789012345678901234567890', 0) FROM numbers(10)"
WIDE_SIZE=$(wc -c < "${LAKE}/wide_decimal/part-0.parquet" | tr -d ' ')
cat > "${LAKE}/wide_decimal/_delta_log/00000000000000000000.json" <<EOF
{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}
{"metaData":{"id":"5fba94ed-9794-4965-ba6e-6ee3c0d22af9","format":{"provider":"parquet","options":{}},"schemaString":"{\\"type\\":\\"struct\\",\\"fields\\":[{\\"name\\":\\"id\\",\\"type\\":\\"long\\",\\"nullable\\":true,\\"metadata\\":{}},{\\"name\\":\\"d\\",\\"type\\":\\"decimal(38,0)\\",\\"nullable\\":true,\\"metadata\\":{}}]}","partitionColumns":[],"configuration":{},"createdTime":0}}
{"add":{"path":"part-0.parquet","partitionValues":{},"size":${WIDE_SIZE},"modificationTime":0,"dataChange":true,"stats":"{\\"numRecords\\":10,\\"minValues\\":{\\"id\\":0,\\"d\\":123456789012345678901234567890},\\"maxValues\\":{\\"id\\":9,\\"d\\":123456789012345678901234567890},\\"nullCount\\":{\\"id\\":0,\\"d\\":0}}"}}
EOF

echo '--- T1: the setting is off'
labels "" "deltaLakeLocal('${LAKE}/deletion_vectors')"
echo '--- T2: 3 data files of 100, 50 and 12 rows, deletion vectors of 39 and 11 rows'
labels "${ON}" "deltaLakeLocal('${LAKE}/deletion_vectors')"
echo '--- T2: WHERE id >= 100 skips the file with id 0..99'
labels "${ON}" "deltaLakeLocal('${LAKE}/deletion_vectors')" "t.id >= 100"
echo '--- T3: 30 data files of 1 row, partitioned'
labels "${ON}" "deltaLakeLocal('${LAKE}/partitioned')"
echo '--- T3: WHERE p_int = 1 keeps 8 files'
labels "${ON}" "deltaLakeLocal('${LAKE}/partitioned')" "t.p_int = 1"
echo '--- T3: WHERE p_int IS NULL keeps 6 files'
labels "${ON}" "deltaLakeLocal('${LAKE}/partitioned')" "t.p_int IS NULL"
echo '--- T3: planning builds no IN set, which would run throwIf'
${CLICKHOUSE_CLIENT} ${PINS} ${ON} --query "
    SELECT count() FROM (EXPLAIN keep_logical_steps = 1, actions = 1 SELECT count() FROM mt AS m JOIN deltaLakeLocal('${LAKE}/partitioned') AS t ON m.k = t.id
        WHERE t.p_int GLOBAL IN (SELECT throwIf(number = 0) FROM numbers(1)))
    WHERE explain LIKE '%Join: %'"
echo '--- T3: p_int has 3 partition values and NULL'
distinct_values "deltaLakeLocal('${LAKE}/partitioned')" p_int
echo '--- T4: bounds of each type, 2 data files of 20 and 15 rows'
labels "${ON}" "deltaLakeLocal('${LAKE}/all_types')" "t.i < 0"
labels "${ON}" "deltaLakeLocal('${LAKE}/all_types')" "t.by < -120"
labels "${ON}" "deltaLakeLocal('${LAKE}/all_types')" "t.f < 1"
labels "${ON}" "deltaLakeLocal('${LAKE}/all_types')" "t.d > 3e11"
labels "${ON}" "deltaLakeLocal('${LAKE}/all_types')" "t.dec < 30"
labels "${ON}" "deltaLakeLocal('${LAKE}/all_types')" "t.dt < '1970-01-01'"
labels "${ON}" "deltaLakeLocal('${LAKE}/all_types')" "t.ts < '1970-01-05 00:00:00'"
labels "${ON}" "deltaLakeLocal('${LAKE}/all_types')" "t.ts_ntz >= '1970-01-01 20:00:00'"
echo '--- T4: the bounds of ts_ntz do not depend on the session time zone'
labels "--use_datalake_metadata_statistics=1 --session_timezone=Asia/Tokyo" "deltaLakeLocal('${LAKE}/all_types')" "t.ts_ntz >= toDateTime64('1970-01-01 20:00:00', 6, 'UTC')"
echo '--- T4: s has 7 NULLs in 35 rows'
labels "${ON}" "deltaLakeLocal('${LAKE}/all_types')" "t.s IS NULL"
echo '--- T5: column mapping, the statistics are keyed by physical names'
labels "${ON}" "deltaLakeLocal('${LAKE}/column_mapping_id')"
labels "${ON}" "deltaLakeLocal('${LAKE}/column_mapping_id')" "t.id < 2"
echo '--- T6: written by ClickHouse, only the rows'
labels "${ON}" written
labels "${ON}" written "t.v = 1"
echo '--- T7: bounds of a Decimal(38, 0) wider than 64 bits keep the rows, with and without the column'
labels "${ON}" "deltaLakeLocal('${LAKE}/wide_decimal')"
labels "${ON}" "deltaLakeLocal('${LAKE}/wide_decimal')" "t.d IS NOT NULL"

rm -rf "${LAKE}"
