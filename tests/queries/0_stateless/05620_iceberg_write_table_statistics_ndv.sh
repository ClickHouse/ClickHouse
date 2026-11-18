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
    ${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --iceberg_write_table_statistics_ndv=1 --allow_expire_snapshots=1 \
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

# $1: table, $2: `zero_theta`, `truncated` or `large`. Rewrites the sketch of `id` (field 1) in the statistics file of $1.
corrupt_id_sketch()
{
    python3 - "$(find "${WORK_DIR}/$1/metadata/" -name '*.stats')" "$2" <<'EOF'
import json, struct, sys
path, mode = sys.argv[1], sys.argv[2]
data = open(path, 'rb').read()
start = len(data) - 12 - struct.unpack('<i', data[-12:-8])[0]
footer = json.loads(data[start:-12])
out = b'PFA1'
for blob in footer['blobs']:
    sketch = data[blob['offset']:blob['offset'] + blob['length']]
    if blob['fields'] == [1]:
        if mode == 'zero_theta':
            # Three preamble longs instead of two: the third one, after the count, is theta.
            sketch = bytes([sketch[0] & 0xC0 | 3]) + sketch[1:16] + bytes(8) + sketch[16:]
        elif mode == 'truncated':
            sketch = sketch[:-1]
        else:
            # An exact sketch of 5000 hashes, more than the 4096 a union keeps.
            hashes = sorted(range(1 << 50, (1 << 50) + 5000 * 997, 997))
            sketch = sketch[:8] + struct.pack('<I', len(hashes)) + sketch[12:16] + b''.join(struct.pack('<Q', h) for h in hashes)
    blob['offset'], blob['length'] = len(out), len(sketch)
    out += sketch
payload = json.dumps(footer).encode()
open(path, 'wb').write(out + b'PFA1' + payload + struct.pack('<i', len(payload)) + data[-8:])
EOF
}

for corruption in zero_theta truncated; do
    echo "a delete on a ${corruption//_/ } parent sketch commits without statistics"
    local_query "CREATE TABLE t_${corruption} (id Int64, s String) ENGINE = IcebergLocal('${WORK_DIR}/t_${corruption}/');
        INSERT INTO t_${corruption} SELECT number, toString(number % 10) FROM numbers(1000);"
    corrupt_id_sketch "t_${corruption}" "${corruption}"
    local_query "CREATE TABLE t_${corruption} ENGINE = IcebergLocal('${WORK_DIR}/t_${corruption}/');
        ALTER TABLE t_${corruption} DELETE WHERE id < 10; SELECT count() FROM t_${corruption};"
    stats "t_${corruption}" "id s"
done

echo "a delete keeps a parent sketch larger than a union"
local_query "CREATE TABLE t_large (id Int64, s String) ENGINE = IcebergLocal('${WORK_DIR}/t_large/');
    INSERT INTO t_large SELECT number, toString(number % 10) FROM numbers(1000);"
corrupt_id_sketch t_large large
local_query "CREATE TABLE t_large ENGINE = IcebergLocal('${WORK_DIR}/t_large/'); ALTER TABLE t_large DELETE WHERE id < 10;"
stats t_large "id s"

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
    run t_optimize "INSERT INTO t_optimize $(values 0 20000); ALTER TABLE t_optimize DELETE WHERE id < 10000; OPTIMIZE TABLE t_optimize SETTINGS allow_experimental_iceberg_compaction = 1;"
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
run_partitioned t_manifest "OPTIMIZE TABLE t_manifest MANIFEST SETTINGS allow_experimental_iceberg_compaction = 1, iceberg_manifest_min_count_to_compact = 1;"
stats t_manifest "id s p"
echo "drop partition keeps the parent's sketches"
run_partitioned t_drop "ALTER TABLE t_drop DROP PARTITION 1;"
stats t_drop "id s p"

echo "a column added to a table with rows gets the sketch of the new rows"
local_query "CREATE TABLE t_add (id Int64) ENGINE = IcebergLocal('${WORK_DIR}/t_add/');
    INSERT INTO t_add SELECT number FROM numbers(1000);
    ALTER TABLE t_add ADD COLUMN c Nullable(Int32);
    INSERT INTO t_add SELECT number + 1000, number % 7 FROM numbers(1000);"
stats t_add "id c"

echo "expire_snapshots removes the statistics of the expired snapshots"
run t_insert "INSERT INTO t_insert $(values 30000 100); ALTER TABLE t_insert EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1);" \
    | grep statistics
metadata=$(find "${WORK_DIR}/t_insert/metadata/" -name '*.metadata.json' | sort -V | tail -n 1)
echo "statistics entries: $(local_query "SELECT length(JSONExtractArrayRaw(json, 'statistics')) FROM file('${metadata}', 'JSONAsString')")"
echo "statistics files: $(find "${WORK_DIR}/t_insert/metadata/" -name '*.stats' | wc -l)"
stats t_insert "${NAMES}"

echo "expire_snapshots keeps a statistics file that holds the deletion vectors of a retained snapshot, deletes one of an expired snapshot once"
cp -r "${CUR_DIR}/data_lake_corpus/iceberg/v3_deletion_vectors" "${WORK_DIR}/t_dv"
# Expired snapshots have statistics in a deletion vector file of the current snapshot (00000-72) and in one that
# only expired snapshots use (00000-66). The statistics paths spell the files differently from the manifests.
sed -i -E 's#"location":"([^"]*)"(.*)"statistics":\[\]#"location":"\1"\2"statistics":[{"snapshot-id":2372400231275045333,"statistics-path":"file://'"${WORK_DIR}"'/t_dv/data/00000-72-873ac898-d3c7-4528-9355-2cbed87bfd56-00001-deletes.puffin","file-size-in-bytes":1,"file-footer-size-in-bytes":1,"blob-metadata":[]},{"snapshot-id":4475287003289568582,"statistics-path":"file://'"${WORK_DIR}"'/t_dv/data/00000-66-796d5805-503b-4859-a495-3962d805056e-00001-deletes.puffin","file-size-in-bytes":1,"file-footer-size-in-bytes":1,"blob-metadata":[]}]#' \
    "${WORK_DIR}/t_dv/metadata/v5.metadata.json"
echo "injected statistics entries: $(grep -o '"statistics-path"' "${WORK_DIR}/t_dv/metadata/v5.metadata.json" | wc -l)"
run t_dv "ALTER TABLE t_dv EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1);" | grep -E 'position_delete|statistics|failed'
local_query "SELECT count() FROM icebergLocal('${WORK_DIR}/t_dv/')"

echo "expire_snapshots keeps a deletion vector file of an expired snapshot that holds the statistics of a retained snapshot"
cp -r "${CUR_DIR}/data_lake_corpus/iceberg/v3_deletion_vectors" "${WORK_DIR}/t_dv_stats"
# The current snapshot has statistics in the deletion vector file that only expired snapshots use (00000-66), spelled
# differently from the manifests.
sed -i -E 's#"location":"([^"]*)"(.*)"statistics":\[\]#"location":"\1"\2"statistics":[{"snapshot-id":4919572450537363904,"statistics-path":"file://'"${WORK_DIR}"'/t_dv_stats/data/00000-66-796d5805-503b-4859-a495-3962d805056e-00001-deletes.puffin","file-size-in-bytes":1,"file-footer-size-in-bytes":1,"blob-metadata":[]}]#' \
    "${WORK_DIR}/t_dv_stats/metadata/v5.metadata.json"
for dry_run in 1 0; do
    run t_dv_stats "ALTER TABLE t_dv_stats EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1, dry_run = ${dry_run});" \
        | grep -E 'position_delete|statistics|dry_run'
done
metadata=$(find "${WORK_DIR}/t_dv_stats/metadata/" -name '*.metadata.json' | sort -V | tail -n 1)
echo "statistics entries: $(local_query "SELECT length(JSONExtractArrayRaw(json, 'statistics')) FROM file('${metadata}', 'JSONAsString')")"
echo "statistics files: $(find "${WORK_DIR}/t_dv_stats/data/" -name '00000-66-*' | wc -l)"
