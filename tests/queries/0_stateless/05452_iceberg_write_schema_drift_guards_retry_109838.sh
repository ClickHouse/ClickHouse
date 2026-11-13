#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option).
# - no-parallel: `iceberg_writes_pause_before_commit` is server-global and would pause concurrent Iceberg writes.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Part 6 of the Iceberg write schema-drift guard regression (#109838): the checks in the commit-conflict
# retry of the INSERT sink. An INSERT is paused after its data files and manifests are written, a competing
# metadata version is published, and the INSERT is resumed, so its commit conflicts and it retries against
# the refreshed metadata.

FAILPOINT=iceberg_writes_pause_before_commit
TABLE="t_${CLICKHOUSE_DATABASE}_retry"
TABLE_PATH="${USER_FILES_PATH}/${TABLE}/"
PAUSED_OUTPUT="${CLICKHOUSE_TMP}/${TABLE}_paused_insert.out"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}" "${PAUSED_OUTPUT}"
}
trap cleanup EXIT

reset() {
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "
CREATE TABLE ${TABLE} (c0 Int32, c1 String) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet') PARTITION BY (c0);
INSERT INTO ${TABLE} VALUES (1, 'a');
"
}

# Start an INSERT in the background and wait until it is paused right before its commit.
start_paused_insert() {
    ${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT ${FAILPOINT}"
    ${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 \
        --query "INSERT INTO ${TABLE} VALUES (2, 'b')" > "${PAUSED_OUTPUT}" 2>&1 &
    PAUSED_PID=$!
    ${CLICKHOUSE_CLIENT} --query "SYSTEM WAIT FAILPOINT ${FAILPOINT} PAUSE"
}

resume_paused_insert() {
    ${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT ${FAILPOINT}"
    wait "${PAUSED_PID}"
}

count_data_files() {
    find "${TABLE_PATH}/data" -name '*.parquet' | wc -l
}

# --- Competing commit rebinds the partition spec id: the retry is rejected and cleans up ----------
# The competing version keeps the spec id but partitions by `c1` instead of `c0`. The buffered data files
# were partitioned by `c0`, so committing them under the rebound spec would mislabel their partitions.
reset
start_paused_insert
python3 - "${TABLE_PATH}/metadata" <<'PY'
import glob, json, os, re, sys
md = sys.argv[1]
version = lambda p: int(re.search(r'v(\d+)', os.path.basename(p)).group(1))
latest = max(glob.glob(os.path.join(md, 'v*.metadata.json')), key=version)
m = json.load(open(latest))
for spec in m['partition-specs']:
    if spec['spec-id'] == m['default-spec-id']:
        spec['fields'] = [{'field-id': 1000, 'name': 'c1', 'source-id': 2, 'transform': 'identity'}]
m['last-updated-ms'] = m.get('last-updated-ms', 0) + 60000
tmp = os.path.join(md, '.tmp_next')
json.dump(m, open(tmp, 'w'))
os.rename(tmp, os.path.join(md, f'v{version(latest) + 1}.metadata.json'))
PY
resume_paused_insert
grep -oF "NOT_IMPLEMENTED" "${PAUSED_OUTPUT}" | head -1
# Only the data file of the first INSERT remains: the rejected one removed its files.
count_data_files

# --- Competing INSERT without drift: the retry succeeds -------------------------------------------
# The failpoint pauses only once, so the competing INSERT commits first.
reset
start_paused_insert
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --async_insert=0 --query "INSERT INTO ${TABLE} VALUES (3, 'c')"
resume_paused_insert
grep -qF "Exception" "${PAUSED_OUTPUT}" && cat "${PAUSED_OUTPUT}" || echo "INSERT_OK"
${CLICKHOUSE_CLIENT} --query "SELECT c0, c1 FROM icebergLocal('${TABLE_PATH}') ORDER BY c0"
count_data_files
