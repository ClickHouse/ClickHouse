#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel-replicas
# no-fasttest: Parquet
# no-parallel-replicas: checks the initiator's pipeline; with parallel replicas even max_threads = 1 merges the replicas' streams with a resize

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_sparse_out;
    CREATE TABLE t_sparse_out (id UInt64, u UInt64, s String) ENGINE = MergeTree ORDER BY id
    SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5, min_bytes_for_wide_part = 0, index_granularity = 1024;
    INSERT INTO t_sparse_out SELECT number, number % 100 = 0 ? number : 0, number % 100 = 0 ? toString(number) : '' FROM numbers(100000);
    SELECT column, groupUniqArray(serialization_kind) FROM system.parts_columns
    WHERE database = currentDatabase() AND table = 't_sparse_out' AND active GROUP BY column ORDER BY column;
    SELECT count(), sum(cityHash64(id, u, s)) FROM t_sparse_out;
"

SETTINGS="max_threads = 4, merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 1, log_processors_profiles = 1, use_query_cache = 0"

function export_query()
{
    local name=$1 query=$2
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&query_id=${CLICKHOUSE_DATABASE}_${name}" --data-binary "$query"
}

function read_back()
{
    local format=$1
    ${CLICKHOUSE_LOCAL} --input-format "$format" -q "SELECT count(), sum(cityHash64(id, u, s)) FROM table"
}

echo "native $(export_query native "SELECT * FROM t_sparse_out SETTINGS $SETTINGS FORMAT Native" | read_back Native)"
echo "parquet $(export_query parquet "SELECT * FROM t_sparse_out SETTINGS $SETTINGS FORMAT Parquet" | read_back Parquet)"
echo "expression $(export_query expression "SELECT id, u, s, id + 1 AS e FROM t_sparse_out SETTINGS $SETTINGS FORMAT Native" | read_back Native)"
echo "single_stream $(export_query single_stream "SELECT * FROM t_sparse_out SETTINGS $SETTINGS, max_threads = 1 FORMAT Native" | read_back Native)"

# The limit lies between the size of the result with sparse columns (~0.84 MB) and with full ones (~2.4 MB).
echo "limits $(export_query limits "SELECT * FROM t_sparse_out SETTINGS $SETTINGS, max_result_bytes = 1600000, result_overflow_mode = 'throw' FORMAT Native" \
    | ${CLICKHOUSE_LOCAL} --input-format Native -q "SELECT count() FROM table" 2>&1)"

# Same bound for the RESULT_BYTES quota. The quota DDL can log a benign "Re-chose quotas" warning under sanitizers.
QUOTA_CLIENT="$CLICKHOUSE_CLIENT --allow_repeated_settings --send_logs_level=fatal"
$QUOTA_CLIENT -q "
    DROP QUOTA IF EXISTS q_${CLICKHOUSE_DATABASE};
    DROP USER IF EXISTS u_${CLICKHOUSE_DATABASE};
    CREATE USER u_${CLICKHOUSE_DATABASE} IDENTIFIED WITH no_password;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.t_sparse_out TO u_${CLICKHOUSE_DATABASE};
    CREATE QUOTA q_${CLICKHOUSE_DATABASE} FOR INTERVAL 1 YEAR MAX result_bytes = 1600000 TO u_${CLICKHOUSE_DATABASE};
"
echo "quota $(${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=u_${CLICKHOUSE_DATABASE}&query_id=${CLICKHOUSE_DATABASE}_quota" \
    --data-binary "SELECT * FROM t_sparse_out SETTINGS max_threads = 4, merge_tree_min_rows_for_concurrent_read = 1024, merge_tree_min_bytes_for_concurrent_read = 1, use_query_cache = 0 FORMAT Native" \
    | ${CLICKHOUSE_LOCAL} --input-format Native -q "SELECT count() FROM table" 2>&1)"
$QUOTA_CLIENT -q "
    DROP QUOTA q_${CLICKHOUSE_DATABASE};
    DROP USER u_${CLICKHOUSE_DATABASE};
"

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS processors_profile_log"

for name in native parquet expression single_stream; do
    $CLICKHOUSE_CLIENT -q "
        SELECT '$name', countIf(name = 'Resize') > 0,
            sumIf(output_bytes, name = 'MaterializingTransform') > sumIf(input_bytes, name = 'MaterializingTransform')
        FROM system.processors_profile_log
        WHERE event_date >= yesterday() AND query_id = '${CLICKHOUSE_DATABASE}_${name}'"
done

$CLICKHOUSE_CLIENT -q "DROP TABLE t_sparse_out"
