#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Tag no-fasttest: delta-kernel pulls in extra dependencies.
# Tag no-msan: delta-kernel-rs (Rust) is not built under MSan, so `DeltaLakeLocal` is absent.

# A nullable Delta field read through a column that is not `Nullable` turns each NULL into the type default,
# so the engine predicate must not skip a file for its NULLs. Each case prints the count with the predicate,
# the count over a full scan (`countIf` without WHERE), and the number of scanned files.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
set -e

ROOT="${USER_FILES_PATH}/${CLICKHOUSE_DATABASE}_delta_null_default"
trap 'rm -rf "${ROOT}" 2>/dev/null' EXIT
rm -rf "${ROOT}"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION file('${ROOT}/str/mixed.parquet', Parquet, 's Nullable(String)') VALUES ('x'), (NULL);
    INSERT INTO FUNCTION file('${ROOT}/str/null.parquet', Parquet, 's Nullable(String)') VALUES (NULL);
    INSERT INTO FUNCTION file('${ROOT}/str/y.parquet', Parquet, 's Nullable(String)') VALUES ('y');
    INSERT INTO FUNCTION file('${ROOT}/cdf/mixed.parquet', Parquet, 's Nullable(String)') VALUES ('x'), (NULL);
    INSERT INTO FUNCTION file('${ROOT}/cdf/null.parquet', Parquet, 's Nullable(String)') VALUES (NULL);
    INSERT INTO FUNCTION file('${ROOT}/cdf/y.parquet', Parquet, 's Nullable(String)') VALUES ('y');
    INSERT INTO FUNCTION file('${ROOT}/int/mixed.parquet', Parquet, 'i Nullable(Int32)') VALUES (7), (NULL);
    INSERT INTO FUNCTION file('${ROOT}/int/nine.parquet', Parquet, 'i Nullable(Int32)') VALUES (9);
    INSERT INTO FUNCTION file('${ROOT}/date/mixed.parquet', Parquet, 'd Nullable(Date32)') VALUES ('2026-01-01'), (NULL);
    INSERT INTO FUNCTION file('${ROOT}/date/feb.parquet', Parquet, 'd Nullable(Date32)') VALUES ('2026-02-01');
    INSERT INTO FUNCTION file('${ROOT}/bool/null.parquet', Parquet, 'b Nullable(Bool)') VALUES (NULL);
    INSERT INTO FUNCTION file('${ROOT}/bool/t.parquet', Parquet, 'b Nullable(Bool)') VALUES (true);
    INSERT INTO FUNCTION file('${ROOT}/notnull/x.parquet', Parquet, 's String') VALUES ('x');
    INSERT INTO FUNCTION file('${ROOT}/notnull/y.parquet', Parquet, 's String') VALUES ('y');
" < /dev/null

# Hand-written Delta logs, because ClickHouse's Delta writer emits no per-column stats.
PROTOCOL='{"protocol":{"minReaderVersion":1,"minWriterVersion":2}}'
CDF_PROTOCOL='{"protocol":{"minReaderVersion":1,"minWriterVersion":4}}'

# metadata <id> <field> <type> <nullable> [<configuration>]: a one-field schema.
metadata() {
    printf '{"metaData":{"id":"%s","format":{"provider":"parquet","options":{}},"schemaString":"{\\"type\\":\\"struct\\",\\"fields\\":[{\\"name\\":\\"%s\\",\\"type\\":\\"%s\\",\\"nullable\\":%s,\\"metadata\\":{}}]}","partitionColumns":[],"configuration":{%s}}}' "$1" "$2" "$3" "$4" "${5:-}"
}

# log <table> <protocol> <metaData> <add>...: writes the table's first commit.
log() {
    mkdir -p "${ROOT}/$1/_delta_log"
    printf '%s\n' "$2" "$3" "${@:4}" > "${ROOT}/$1/_delta_log/00000000000000000000.json"
}

# add <table> <file> <stats>: an add action; `stats` is a JSON document embedded as a JSON string.
# The size must be real: Change Data Feed reads the files through delta-kernel.
add() {
    printf '{"add":{"path":"%s.parquet","partitionValues":{},"size":%s,"modificationTime":0,"dataChange":true,"stats":"%s"}}' \
        "$2" "$(stat -c %s "${ROOT}/$1/$2.parquet")" "${3//\"/\\\"}"
}

# str_files <table>: `mixed` = ['x', NULL], `null` = [NULL], `y` = ['y'].
str_files() {
    add "$1" mixed '{"numRecords":2,"minValues":{"s":"x"},"maxValues":{"s":"x"},"nullCount":{"s":1}}'; echo
    add "$1" null '{"numRecords":1,"nullCount":{"s":1}}'; echo
    add "$1" y '{"numRecords":1,"minValues":{"s":"y"},"maxValues":{"s":"y"},"nullCount":{"s":0}}'
}

log str "${PROTOCOL}" "$(metadata str s string true)" "$(str_files str)"
log cdf "${CDF_PROTOCOL}" "$(metadata cdf s string true '"delta.enableChangeDataFeed":"true"')" "$(str_files cdf)"
# `cdf_pruned` lists `y.parquet` but does not have it: the query below succeeds only if the predicate skips that file.
cp -r "${ROOT}/cdf" "${ROOT}/cdf_pruned" && rm "${ROOT}/cdf_pruned/y.parquet"
log int "${PROTOCOL}" "$(metadata int i integer true)" \
    "$(add int mixed '{"numRecords":2,"minValues":{"i":7},"maxValues":{"i":7},"nullCount":{"i":1}}')" \
    "$(add int nine '{"numRecords":1,"minValues":{"i":9},"maxValues":{"i":9},"nullCount":{"i":0}}')"
log date "${PROTOCOL}" "$(metadata date d date true)" \
    "$(add date mixed '{"numRecords":2,"minValues":{"d":"2026-01-01"},"maxValues":{"d":"2026-01-01"},"nullCount":{"d":1}}')" \
    "$(add date feb '{"numRecords":1,"minValues":{"d":"2026-02-01"},"maxValues":{"d":"2026-02-01"},"nullCount":{"d":0}}')"
log bool "${PROTOCOL}" "$(metadata bool b boolean true)" \
    "$(add bool null '{"numRecords":1,"nullCount":{"b":1}}')" \
    "$(add bool t '{"numRecords":1,"minValues":{"b":true},"maxValues":{"b":true},"nullCount":{"b":0}}')"
log notnull "${PROTOCOL}" "$(metadata notnull s string false)" \
    "$(add notnull x '{"numRecords":1,"minValues":{"s":"x"},"maxValues":{"s":"x"},"nullCount":{"s":0}}')" \
    "$(add notnull y '{"numRecords":1,"minValues":{"s":"y"},"maxValues":{"s":"y"},"nullCount":{"s":0}}')"

# check <table> <condition> [<settings>]: the count under WHERE, then the same condition counted over a full scan.
check() {
    local settings="${3:+ SETTINGS $3}"
    echo "INSERT INTO results SELECT queryID(), count() FROM $1 WHERE $2${settings};"
    echo "INSERT INTO results SELECT queryID(), countIf($2) FROM $1${settings};"
}

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE results (query_id String, n UInt64) ENGINE = Memory;
    CREATE TABLE t_str (s String) ENGINE = DeltaLakeLocal('${ROOT}/str', Parquet);
    CREATE TABLE t_cdf (s String) ENGINE = DeltaLakeLocal('${ROOT}/cdf', Parquet);
    CREATE TABLE t_cdf_pruned (s String) ENGINE = DeltaLakeLocal('${ROOT}/cdf_pruned', Parquet);
    CREATE TABLE t_int (i Int32) ENGINE = DeltaLakeLocal('${ROOT}/int', Parquet);
    CREATE TABLE t_date (d Date32) ENGINE = DeltaLakeLocal('${ROOT}/date', Parquet);
    CREATE TABLE t_bool (b Bool) ENGINE = DeltaLakeLocal('${ROOT}/bool', Parquet);
    CREATE TABLE t_notnull (s String) ENGINE = DeltaLakeLocal('${ROOT}/notnull', Parquet);

    SET delta_lake_throw_on_engine_predicate_error = 1, optimize_move_to_prewhere = 0, convert_query_to_cnf = 0, log_queries = 1;

    $(check t_str "s != 'x'")
    $(check t_str "s < 'x'")
    $(check t_str "NOT (s = 'x')")
    $(check t_str "s = 'y'")
    $(check t_str "s = 'z'")
    $(check t_int "i != 7")
    $(check t_int "i < 5")
    $(check t_int "i = 9")
    $(check t_date "d < '2000-01-01'")
    $(check t_date "toDate(d) < '2000-01-01'")
    $(check t_date "d = '2026-02-01'")
    $(check t_bool "NOT b")
    $(check t_notnull "s = 'x'")
    -- Change Data Feed scans count no \`DeltaLakeScannedFiles\`.
    $(check t_cdf "s < 'x'" "delta_lake_snapshot_start_version = 0")
    $(check t_cdf "s != 'x'" "delta_lake_snapshot_start_version = 0")

    SYSTEM FLUSH LOGS query_log;
    CREATE VIEW report AS SELECT
        event_time_microseconds,
        extract(query, 'FROM (t_[a-z]+)') AS table,
        position(query, 'countIf(') > 0 AS full_scan,
        if(full_scan, concat(extract(query, 'countIf[(](.+)[)] FROM'), extract(query, '( SETTINGS [^;]*)')), extract(query, ' WHERE ([^;]*)')) AS condition,
        n,
        ProfileEvents['DeltaLakeScannedFiles'] AS scanned_files
    FROM system.query_log JOIN results USING (query_id)
    WHERE current_database = currentDatabase() AND event_date >= yesterday()
        AND query_kind = 'Insert' AND type = 'QueryFinish' AND query LIKE '%INSERT INTO results %';

    SELECT table, condition, anyIf(n, NOT full_scan) AS with_predicate, anyIf(n, full_scan) AS full_scan_count,
        anyIf(scanned_files, NOT full_scan) AS scanned
    FROM report GROUP BY table, condition ORDER BY min(event_time_microseconds) FORMAT TSVRaw;

    SELECT 't_cdf_pruned', count() FROM t_cdf_pruned WHERE s < 'x' SETTINGS delta_lake_snapshot_start_version = 0;
" < /dev/null
