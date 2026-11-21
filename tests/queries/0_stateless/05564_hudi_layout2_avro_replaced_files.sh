#!/usr/bin/env bash
# Tags: no-fasttest
# Hudi and Avro are not available in the fast test build.

set -euo pipefail

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_TEST_UNIQUE_NAME}/layout2_avro"

# Generate small OCF instants without any external Python Avro package. Bytes and strings
# deliberately have identical wire data: only the original schema distinguishes them.
avro_instant()
{
    python3 - "$1" <<'PY'
import json
import sys

case = sys.argv[1]


def long(value):
    value = (value << 1) ^ (value >> 63)
    result = bytearray()
    while value > 127:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def string(value):
    value = value.encode()
    return long(len(value)) + value


items = "bytes" if case == "bytes" else "string"
if case == "uuid_string":
    items = {"type": "string", "logicalType": "uuid"}
array = {"type": "array", "items": items}
nullable_array = case in ("nullable_array", "null_value")
values = ["null", array] if nullable_array else array
field_type = ["null", {"type": "map", "values": values}]
fields = [{"name": "partitionToReplaceFileIds", "type": field_type}]
payload = long(1) + long(1) + string("")
if nullable_array:
    payload += long(0 if case == "null_value" else 1)
if case != "null_value":
    payload += long(1) + string("drop") + long(0)
payload += long(0)
if case == "null":
    payload = long(0)
if case == "absent":
    fields, payload = [], b""
schema = json.dumps({"type": "record", "name": "HudiReplace", "fields": fields})
sync = b"hudi-avro-test-1"
metadata = long(2) + string("avro.schema") + string(schema)
metadata += string("avro.codec") + string("null") + long(0)
sys.stdout.buffer.write(b"Obj\x01" + metadata + sync + long(1) + long(len(payload)) + payload + sync)
PY
}

for case in valid absent null nullable_array null_value bytes uuid_string; do
    path="${ROOT}/${case}"
    printf 'hoodie.timeline.layout.version=2\n' | ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION s3(s3_conn, filename='${path}/.hoodie/hoodie.properties', format='RawBLOB', structure='data String')
        SETTINGS s3_truncate_on_insert=1 FORMAT RawBLOB"
    for file_id in keep drop; do
        ${CLICKHOUSE_CLIENT} --query "
            INSERT INTO FUNCTION s3(s3_conn, filename='${path}/${file_id}_0_100.parquet', format='Parquet')
            SETTINGS s3_truncate_on_insert=1 SELECT '${file_id}' AS id"
    done
    avro_instant "$case" | ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION s3(s3_conn, filename='${path}/.hoodie/timeline/200_300.replacecommit', format='RawBLOB', structure='data String')
        SETTINGS s3_truncate_on_insert=1 FORMAT RawBLOB"
    query="SELECT arrayStringConcat(arraySort(groupArray(id)), ',') FROM hudi(s3_conn, filename='${path}/', structure='id String')"
    if [[ "$case" == null_value || "$case" == bytes ]]; then
        if error=$(${CLICKHOUSE_CLIENT} --query "$query" 2>&1); then
            echo "Unexpected success: ${case}"
            exit 1
        fi
        [[ "$error" == *INCORRECT_DATA* ]]
        echo "${case}: INCORRECT_DATA"
    else
        echo "${case}: $(${CLICKHOUSE_CLIENT} --query "$query")"
    fi
done
