#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option) and DataSketches
# Each commit kind writes the theta-sketch statistics of its snapshot. Each case prints `column:ndv/exact` from the
# statistics of the current snapshot against `uniqExact`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT

local_query()
{
    ${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --iceberg_write_table_statistics_ndv=1 --allow_expire_snapshots=1 --allow_experimental_iceberg_compaction=1 \
        --mutations_sync=2 --max_insert_threads=1 "${@:2}" -q "$1" -- --user_files_path="${WORK_DIR}"
}

COLUMNS="id Int64, s String, d Date, n Nullable(Int32), dec Decimal(18, 2), ts DateTime64(6), f Float64, b Bool"
NAMES="id s d n dec ts f b"
# Every column but `id` has fewer than 4096 distinct values, where the sketch is exact.
values()
{
    echo "SELECT number, toString(intDiv(number, 200)), toDate('2020-01-01') + intDiv(number, 400),
        if(number % 7 = 0, NULL, toInt32(intDiv(number, 1000))), toDecimal64(intDiv(number, 100) / 4, 2),
        fromUnixTimestamp64Micro(toInt64(1600000000000000 + intDiv(number, 50))), intDiv(number, 40) / 8, number % 2 = 0
        FROM numbers($1, $2)"
}

# $1: table, $2: statements run on it, then settings.
run()
{
    local columns="(${COLUMNS})"
    [[ -d "${WORK_DIR}/$1" ]] && columns=""
    local_query "CREATE TABLE $1 ${columns} ENGINE = IcebergLocal('${WORK_DIR}/$1/'); $2" "${@:3}"
}

# $1: table, $2: its column names. Prints the statistics of the current snapshot against `uniqExact`.
stats()
{
    local metadata exact
    metadata=$(find "${WORK_DIR}/$1/metadata/" -name '*.metadata.json' | sort -V | tail -n 1)
    exact=$(for column in $2; do echo "'${column}', uniqExact(${column})"; done | paste -sd, -)
    local_query "
        CREATE TABLE $1 ENGINE = IcebergLocal('${WORK_DIR}/$1/');
        WITH
            (SELECT map(${exact}) FROM $1) AS exact,
            (SELECT json FROM file('${metadata}', 'JSONAsString')) AS m,
            arrayFilter(e -> JSONExtractInt(e, 'snapshot-id') = JSONExtractInt(m, 'current-snapshot-id'), JSONExtractArrayRaw(m, 'statistics')) AS entries,
            JSONExtractArrayRaw(arrayFirst(s -> JSONExtractInt(s, 'schema-id') = JSONExtractInt(m, 'current-schema-id'), JSONExtractArrayRaw(m, 'schemas')), 'fields') AS fields,
            arrayMap(blob -> (
                JSONExtractString(arrayFirst(f -> JSONExtractInt(f, 'id') = JSONExtractInt(blob, 'fields', 1), fields), 'name'),
                toUInt64(JSONExtractString(blob, 'properties', 'ndv'))), JSONExtractArrayRaw(entries[1], 'blob-metadata')) AS ndvs
        SELECT if(empty(entries), 'no statistics', arrayStringConcat(arrayMap(x -> x.1 || ':' || if(exact[x.1] > 4096,
            toString(abs(toInt64(x.2) - toInt64(exact[x.1])) <= exact[x.1] * 0.05),
            toString(x.2) || '/' || toString(exact[x.1])), ndvs), ' '))"
}

echo "two inserts union their sketches"
run t_insert "INSERT INTO t_insert $(values 0 20000); INSERT INTO t_insert $(values 10000 20000);"
stats t_insert "${NAMES}"

echo "an insert with the setting off writes no statistics"
run t_off "INSERT INTO t_off $(values 0 1000);"
run t_off "INSERT INTO t_off $(values 1000 1000) SETTINGS iceberg_write_table_statistics_ndv = 0;"
stats t_off "${NAMES}"
echo "nor does the next insert, whose parent has rows but no sketches"
run t_off "INSERT INTO t_off $(values 2000 1000);"
stats t_off "${NAMES}"

echo "a parent blob kept from an earlier snapshot is not unioned"
local_query "CREATE TABLE t_stale (id Int64, s String) ENGINE = IcebergLocal('${WORK_DIR}/t_stale/');
    INSERT INTO t_stale SELECT number, toString(number % 10) FROM numbers(1000);"
# Give the blob of `s` (field 2) another snapshot id, as a file that keeps it from an earlier snapshot does.
python3 - "$(find "${WORK_DIR}/t_stale/metadata/" -name '*.stats')" <<'EOF'
import json, struct, sys
path = sys.argv[1]
data = open(path, 'rb').read()
start = len(data) - 12 - struct.unpack('<i', data[-12:-8])[0]
footer = json.loads(data[start:-12])
for blob in footer['blobs']:
    if blob['fields'] == [2]:
        blob['snapshot-id'] -= 1
payload = json.dumps(footer).encode()
open(path, 'wb').write(data[:start] + payload + struct.pack('<i', len(payload)) + data[-8:])
EOF
local_query "CREATE TABLE t_stale ENGINE = IcebergLocal('${WORK_DIR}/t_stale/');
    INSERT INTO t_stale SELECT number + 1000, toString(number % 10 + 10) FROM numbers(1000);"
stats t_stale "id s"

echo "a delete keeps the parent's sketches"
run t_delete "INSERT INTO t_delete $(values 0 20000); ALTER TABLE t_delete DELETE WHERE id < 10000;"
stats t_delete "${NAMES}"

echo "an update adds the new values"
run t_update "INSERT INTO t_update $(values 0 20000); ALTER TABLE t_update UPDATE s = 'updated', n = 1000 WHERE id = 1;"
stats t_update "${NAMES}"

echo "optimize after a delete rebuilds exact sketches"
IS_CLOUD=$(local_query "SELECT value FROM system.build_options WHERE name = 'CLICKHOUSE_CLOUD'")
if [[ "${IS_CLOUD}" = "1" ]]; then
    # The cloud build compacts in the background through a different code path.
    echo "id:1 s:50/50 d:25/25 n:10/10 dec:100/100 ts:200/200 f:250/250 b:2/2"
else
    run t_optimize "INSERT INTO t_optimize $(values 0 20000); ALTER TABLE t_optimize DELETE WHERE id < 10000; OPTIMIZE TABLE t_optimize;"
    stats t_optimize "${NAMES}"
fi

PARTITIONED="id Int64, s String, p Int64"
# $1: table, $2: statement run after two inserts into a table partitioned by `p`.
run_partitioned()
{
    local_query "CREATE TABLE $1 (${PARTITIONED}) ENGINE = IcebergLocal('${WORK_DIR}/$1/') PARTITION BY p;
        INSERT INTO $1 SELECT number, toString(number % 10), number % 2 FROM numbers(1000);
        INSERT INTO $1 SELECT number + 1000, toString(number % 10 + 10), number % 2 FROM numbers(1000);"
    local_query "CREATE TABLE $1 ENGINE = IcebergLocal('${WORK_DIR}/$1/'); $2"
}

echo "optimize ... manifest keeps the parent's sketches"
run_partitioned t_manifest "OPTIMIZE TABLE t_manifest MANIFEST SETTINGS iceberg_manifest_min_count_to_compact = 1;"
stats t_manifest "id s p"
echo "drop partition keeps the parent's sketches"
run_partitioned t_drop "ALTER TABLE t_drop DROP PARTITION 1;"
stats t_drop "id s p"

echo "expire_snapshots removes the statistics of the expired snapshots"
run t_insert "INSERT INTO t_insert $(values 30000 100); ALTER TABLE t_insert EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1);" \
    | grep statistics
metadata=$(find "${WORK_DIR}/t_insert/metadata/" -name '*.metadata.json' | sort -V | tail -n 1)
echo "statistics entries: $(local_query "SELECT length(JSONExtractArrayRaw(json, 'statistics')) FROM file('${metadata}', 'JSONAsString')")"
echo "statistics files: $(find "${WORK_DIR}/t_insert/metadata/" -name '*.stats' | wc -l)"
stats t_insert "${NAMES}"
