#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).

# An INSERT planned before a column was dropped or renamed must be refused in every data file format,
# and must not commit anything. Reading the table through a remembered older metadata file gives the
# stale structure deterministically, which is the same divergence a concurrent ALTER produces.
# clickhouse-local because the write used to terminate the process: in the shared test server that
# trips the hung-check instead of failing this test.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_DIR="${CLICKHOUSE_USER_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${TABLE_DIR}"
mkdir -p "${TABLE_DIR}"

LOCAL="${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --async_insert=0"

attach() {
    echo "CREATE TABLE t ENGINE = IcebergLocal('${TABLE_DIR}/$1/', '$2')"
}

latest_metadata() {
    echo "metadata/$(basename "$(ls "${TABLE_DIR}/$1/metadata"/*.metadata.json | sort -V | tail -1)")"
}

table_files() {
    find "${TABLE_DIR}/$1" -type f | sort
}

# Prints `ok`, the error code name of a refused INSERT, or `terminated` when the process died.
insert_outcome() {
    local out rc code
    out=$(${LOCAL} --query "$1" 2>&1)
    rc=$?
    code=$(echo "${out}" | grep -m1 '^Code: ' | sed 's/, Stack trace.*//' \
               | grep -o '([A-Z][A-Z0-9_]*)' | tail -1 | tr -d '()')
    if [ "${rc}" -eq 0 ]; then
        echo "ok"
    elif echo "${out}" | grep -q 'Received signal'; then
        echo "terminated"
    elif [ -n "${code}" ]; then
        echo "${code}"
    elif [ "${rc}" -ge 128 ]; then
        echo "terminated"
    else
        echo "unexpected"
    fi
}

# $1 label, $2 format, then one ALTER per argument, each in its own invocation.
stale_insert() {
    local label=$1 format=$2 dir="$1_$2"
    shift 2
    mkdir -p "${TABLE_DIR}/${dir}"
    ${LOCAL} --query "
        CREATE TABLE t (a Int32, b Nullable(String), c Int64) ENGINE = IcebergLocal('${TABLE_DIR}/${dir}/', '${format}');
        INSERT INTO t SELECT toInt32(number), NULL, number * 100 FROM numbers(3);"
    local pin
    pin=$(latest_metadata "${dir}")
    local alter
    for alter in "$@"; do
        ${LOCAL} --query "$(attach "${dir}" "${format}"); ${alter}"
    done
    local before
    before=$(table_files "${dir}")
    local outcome
    outcome=$(insert_outcome "
        $(attach "${dir}" "${format}") SETTINGS iceberg_metadata_file_path = '${pin}';
        INSERT INTO t SELECT toInt32(number + 10), 'x', 5000 + number FROM numbers(3);")
    local files=files_changed
    [ "$(table_files "${dir}")" = "${before}" ] && files=files_unchanged
    echo -e "${label}\t${format}\t${outcome}\t${files}"
    ${LOCAL} --query "$(attach "${dir}" "${format}"); SELECT '${label}', '${format}', count() FROM t;"
}

for format in Parquet ORC Avro; do
    stale_insert drop "${format}" "ALTER TABLE t DROP COLUMN b"
done

# A column added after planning is simply not written by the stale INSERT, which is accepted.
stale_insert add ORC "ALTER TABLE t ADD COLUMN d Nullable(Int64)"

# Same width: the stale block's `c` must not be described under the id of the new column `d`.
stale_insert drop_add ORC "ALTER TABLE t DROP COLUMN b" "ALTER TABLE t ADD COLUMN d Nullable(Int64)"
${LOCAL} --query "
    $(attach drop_add_ORC ORC);
    SELECT 'drop_add', (SELECT count() FROM t WHERE d IS NULL) = (SELECT count() FROM t) SETTINGS use_iceberg_partition_pruning = 1;"

for format in ORC Avro; do
    stale_insert rename "${format}" "ALTER TABLE t RENAME COLUMN b TO b2"
done

# A rename inside a tuple keeps the id of the tuple itself, so only a check of every nested field
# refuses it. ClickHouse cannot rename a nested field, so another writer publishes the new version.
for format in ORC Avro; do
    dir="nested_rename_${format}"
    mkdir -p "${TABLE_DIR}/${dir}"
    ${LOCAL} --query "
        CREATE TABLE t (a Int32, s Tuple(x Int32)) ENGINE = IcebergLocal('${TABLE_DIR}/${dir}/', '${format}');
        INSERT INTO t SELECT toInt32(number), tuple(toInt32(number)) FROM numbers(3);"
    pin=$(latest_metadata "${dir}")
    python3 - "${TABLE_DIR}/${dir}/${pin}" <<'PY'
import json, os, re, sys
path = sys.argv[1]
m = json.load(open(path))
schema = next(s for s in m['schemas'] if s['schema-id'] == m['current-schema-id'])
new_schema = json.loads(json.dumps(schema))
new_schema['schema-id'] = max(s['schema-id'] for s in m['schemas']) + 1
for field in new_schema['fields']:
    if field['name'] == 's':
        for sub in field['type']['fields']:
            if sub['name'] == 'x':
                sub['name'] = 'y'
m['schemas'].append(new_schema)
m['current-schema-id'] = new_schema['schema-id']
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
version = int(re.match(r'v(\d+)\.metadata\.json$', os.path.basename(path)).group(1))
tmp = os.path.join(os.path.dirname(path), '.tmp_metadata')
json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(os.path.dirname(path), f'v{version + 1}.metadata.json'))
PY
    before=$(table_files "${dir}")
    outcome=$(insert_outcome "
        $(attach "${dir}" "${format}") SETTINGS iceberg_metadata_file_path = '${pin}';
        INSERT INTO t SELECT toInt32(number + 10), tuple(toInt32(number + 10)) FROM numbers(3);")
    files=files_changed
    [ "$(table_files "${dir}")" = "${before}" ] && files=files_unchanged
    echo -e "nested_rename\t${format}\t${outcome}\t${files}"
    ${LOCAL} --query "$(attach "${dir}" "${format}"); SELECT 'nested_rename', '${format}', count() FROM t;"
done

# A write with the current structure is still accepted after a refusal.
echo -e "control\tORC\t$(insert_outcome "
    $(attach drop_ORC ORC);
    INSERT INTO t SELECT toInt32(number + 20), 6000 + number FROM numbers(3);")"
${LOCAL} --query "$(attach drop_ORC ORC); SELECT 'control', 'ORC', count() FROM t;"

rm -rf "${TABLE_DIR}"
