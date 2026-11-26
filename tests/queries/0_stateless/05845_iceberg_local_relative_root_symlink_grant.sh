#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database
# - no-fasttest: requires `IcebergLocal` (USE_AVRO build option)
# - no-replicated-database: with a replicated access storage, the entity serialization round trip widens
#   `GRANT READ ON FILE(...)` to an unfiltered `READ, WRITE ON FILE` (see issue #111402), so nothing is denied.

# A table with a relative root reads data files through a symlink leaving its directory:
# the `READ ON FILE` grant is checked on the file that is opened below `user_files`, not on the key resolved against the working directory.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ROOT="${CLICKHOUSE_USER_FILES_UNIQUE:?}"
RELATIVE_ROOT="${ROOT#"${CLICKHOUSE_USER_FILES%/}"/}"
TABLE="t_${CLICKHOUSE_DATABASE}"
USER="u_${CLICKHOUSE_DATABASE}"
trap 'rm -rf "${ROOT}"; ${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${USER}"' EXIT

mkdir -p "${ROOT}/table"
$CLICKHOUSE_CLIENT -q "CREATE TABLE ${TABLE} (x Int32) ENGINE = IcebergLocal('${RELATIVE_ROOT}/table/')"
$CLICKHOUSE_CLIENT --allow_insert_into_iceberg=1 -q "INSERT INTO ${TABLE} VALUES (1)"

mv "${ROOT}/table/data" "${ROOT}/target"
ln -s ../target "${ROOT}/table/data"

$CLICKHOUSE_CLIENT -q "DROP USER IF EXISTS ${USER}"
$CLICKHOUSE_CLIENT -q "CREATE USER ${USER}"
$CLICKHOUSE_CLIENT -q "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.${TABLE} TO ${USER}"

# A local file outside the table directory is not dispatched to other replicas, so the table is read without parallel replicas.
SELECT_SETTINGS=(--enable_parallel_replicas 0)

echo "--- grant on the symlink spelling"
$CLICKHOUSE_CLIENT -q "GRANT READ ON FILE('${ROOT}/table/.*') TO ${USER}"
$CLICKHOUSE_CLIENT --user "${USER}" "${SELECT_SETTINGS[@]}" -q "SELECT x FROM ${CLICKHOUSE_DATABASE}.${TABLE}" 2>&1 | grep -o -m1 "ACCESS_DENIED"

echo "--- grant on the target"
$CLICKHOUSE_CLIENT -q "GRANT READ ON FILE('${ROOT}/target/.*') TO ${USER}"
$CLICKHOUSE_CLIENT --user "${USER}" "${SELECT_SETTINGS[@]}" -q "SELECT x FROM ${CLICKHOUSE_DATABASE}.${TABLE}"

$CLICKHOUSE_CLIENT -q "DROP TABLE ${TABLE}"
