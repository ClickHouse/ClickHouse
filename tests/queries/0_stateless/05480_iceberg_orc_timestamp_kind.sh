#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option) and python3 pyarrow.
#
# The Iceberg spec maps `timestamptz` to the ORC `timestamp with local time zone` kind
# (`TIMESTAMP_INSTANT`) and `timestamp` to the ORC `timestamp` kind, and Java Iceberg rejects a data
# file whose ORC kind differs from the schema. ClickHouse declares `DateTime64` columns as
# `timestamptz`, so their ORC data files must use `TIMESTAMP_INSTANT`, while a `timestamp` column of
# a table written by an older ClickHouse version must keep `TIMESTAMP`. Both must hold the same UTC
# instant.
#
# Every step runs in a separate clickhouse-local process, so no step can see metadata cached by an
# earlier one. The `timestamp` column is produced by rewriting the metadata file before any data is
# written, so the manifest files never carry a conflicting `timestamptz` copy of the schema.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TABLE_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${TABLE_DIR}"
mkdir -p "${TABLE_DIR}/tz/" "${TABLE_DIR}/legacy/"
trap 'rm -rf "${TABLE_DIR}" 2>/dev/null' EXIT

LOCAL="${CLICKHOUSE_LOCAL} --session_timezone UTC --allow_insert_into_iceberg=1 --async_insert=0"

read -r -d '' PROBE <<'PY'
import glob, json, os, sys
import pyarrow as pa
import pyarrow.orc as orc

table_path = sys.argv[1]
metadata_file = sorted(glob.glob(os.path.join(table_path, 'metadata', 'v*.metadata.json')),
                       key=lambda p: int(os.path.basename(p)[1:].split('.')[0]))[-1]
metadata = json.load(open(metadata_file))
schema = next(s for s in metadata['schemas'] if s['schema-id'] == metadata['current-schema-id'])
print('iceberg type: %s' % next(f['type'] for f in schema['fields'] if f['name'] == 'ts'))

data_files = sorted(glob.glob(os.path.join(table_path, 'data', '**', '*.orc'), recursive=True))
if len(data_files) != 1:
    raise SystemExit('expected one ORC data file, found %d' % len(data_files))
orc_file = orc.ORCFile(data_files[0])
print('orc type: %s' % orc_file.schema.field('ts').type)
print('orc epoch nanoseconds: %s' % orc_file.read().column('ts').cast(pa.int64()).to_pylist())
PY

run_arm()
{
    local name=$1 table_path=$2 rewrite=$3
    echo "--- ${name} ---"
    ${LOCAL} --query "CREATE TABLE t (k Int64, ts DateTime64(6)) ENGINE = IcebergLocal('${table_path}', 'ORC')"
    if [ "${rewrite}" = 1 ]; then
        # Make the column a `timestamp`, as a table written by an older ClickHouse version has it.
        sed -i 's/"timestamptz"/"timestamp"/g' "${table_path}"metadata/*.metadata.json
    fi
    ${LOCAL} --query "
        CREATE TABLE t ENGINE = IcebergLocal('${table_path}', 'ORC');
        INSERT INTO t VALUES (1, '2024-01-02 03:04:05.123456');
    "
    ${LOCAL} --query "
        CREATE TABLE t ENGINE = IcebergLocal('${table_path}', 'ORC');
        SELECT k, ts, toTypeName(ts) FROM t;
    "
    python3 -c "${PROBE}" "${table_path}"
}

run_arm "timestamptz column" "${TABLE_DIR}/tz/" 0
run_arm "legacy timestamp column" "${TABLE_DIR}/legacy/" 1
