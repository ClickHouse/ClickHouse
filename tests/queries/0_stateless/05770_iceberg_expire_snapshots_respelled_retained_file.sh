#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# `expire_snapshots` must keep a file that a retained snapshot uses when an expired snapshot spells it differently.
# https://github.com/ClickHouse/ClickHouse/issues/131764

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

WORK_DIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}"
rm -rf "${WORK_DIR}"
mkdir -p "${WORK_DIR}"
trap 'rm -rf "${WORK_DIR}"' EXIT

local_query() { ${CLICKHOUSE_LOCAL} --allow_insert_into_iceberg=1 --allow_expire_snapshots=1 -q "$1" -- --user_files_path="${WORK_DIR}"; }

# $1: table name, $2: Python expression respelling `p`, the manifest list of the current snapshot.
check()
{
    local path="${WORK_DIR}/$1"
    local_query "
        CREATE TABLE t (x Int64) ENGINE = IcebergLocal('${path}/') SETTINGS allow_experimental_iceberg_compaction = 0;
        INSERT INTO t VALUES (1);
        INSERT INTO t VALUES (2);
        INSERT INTO t VALUES (3);"
    # The first snapshot names the manifest list of the current one, so only the second snapshot's own list is expired.
    python3 -c "
import glob, json, re
f = max(glob.glob('${path}/metadata/v*.metadata.json'), key=lambda f: int(re.search(r'/v(\d+)\.metadata', f).group(1)))
m = json.load(open(f))
p = next(s for s in m['snapshots'] if s['snapshot-id'] == m['current-snapshot-id'])['manifest-list']
min(m['snapshots'], key=lambda s: s['sequence-number'])['manifest-list'] = $2
json.dump(m, open(f, 'w'))"
    local_query "
        CREATE TABLE t ENGINE = IcebergLocal('${path}/');
        ALTER TABLE t EXECUTE expire_snapshots(retention_period = '1ms', retain_last = 1);"
    local_query "SELECT count() FROM icebergLocal('${path}/')"
}

echo "file:// prefix"
check t1 "'file://' + p"
echo "repeated slash"
check t2 "p.replace('/metadata/', '/metadata//')"
