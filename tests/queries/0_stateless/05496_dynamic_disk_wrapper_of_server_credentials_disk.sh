#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# Tag zookeeper: the disk type is read with `from_zk`.
# Tag no-fasttest: needs the `encrypted` disk, which requires SSL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A session without `s3_allow_server_credentials_in_user_queries` may not wrap, by name, a disk that a session with it
# created with the server's own credentials, nor a wrapper of such a disk. A `type` read with `from_zk` counts as a
# disk that may use server credentials; here it resolves to an `encrypted` disk, so no object storage is needed.

ZK_PATH="/clickhouse/${CLICKHOUSE_TEST_UNIQUE_NAME}"
KEY="1234567812345678"
SERVER_DISK="server_${CLICKHOUSE_DATABASE}"
WRAPPER_DISK="wrapper_${CLICKHOUSE_DATABASE}"
PLAIN_DISK="plain_${CLICKHOUSE_DATABASE}"

run()
{
    local out
    if out=$(${CLICKHOUSE_CLIENT} --dynamic_disk_allow_from_zk=1 "$@" 2>&1); then echo "OK"; else echo "$out" | grep -oE '\([A-Z_]+\)' | head -1; fi
}
create() { echo "CREATE TABLE $1 (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS disk = $2"; }
wrap() { echo "disk(type = encrypted, disk = '$1', path = '$2/', key = '${KEY}')"; }

OPT_IN="--s3_allow_server_credentials_in_user_queries=1"

$CLICKHOUSE_KEEPER_CLIENT -q "create '${ZK_PATH}' 'disk'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '${ZK_PATH}/type' 'encrypted'"

echo "with the opt-in: create a disk with server credentials: $(run ${OPT_IN} -q "$(create t_server "disk(name = '${SERVER_DISK}', type = 'from_zk ${ZK_PATH}/type', disk = 'default', path = 'disks/${CLICKHOUSE_DATABASE}_server/', key = '${KEY}')")")"
echo "with the opt-in: wrap it: $(run ${OPT_IN} -q "$(create t_wrapper "disk(name = '${WRAPPER_DISK}', type = encrypted, disk = '${SERVER_DISK}', path = 'wrapper/', key = '${KEY}')")")"
echo "with the opt-in: create a disk without server credentials: $(run ${OPT_IN} -q "$(create t_plain "disk(name = '${PLAIN_DISK}', type = encrypted, disk = 'default', path = 'disks/${CLICKHOUSE_DATABASE}_plain/', key = '${KEY}')")")"

echo "without the opt-in: wrap the disk with server credentials: $(run -q "$(create t1 "$(wrap "${SERVER_DISK}" t1)")")"
echo "without the opt-in: wrap its wrapper: $(run -q "$(create t2 "$(wrap "${WRAPPER_DISK}" t2)")")"
echo "without the opt-in: wrap the disk without server credentials: $(run -q "$(create t3 "$(wrap "${PLAIN_DISK}" t3)")")"
echo "with the opt-in: wrap the disk with server credentials: $(run ${OPT_IN} -q "$(create t4 "$(wrap "${SERVER_DISK}" t4)")")"

# The wrapped disk is read with `from_zk`: after the node changes, the same definition no longer resolves to the disk with
# server credentials, but it still names the disk created from it.
$CLICKHOUSE_KEEPER_CLIENT -q "create '${ZK_PATH}/wrapped' '${SERVER_DISK}'"
INDIRECT_WRAPPER="disk(type = encrypted, disk = 'from_zk ${ZK_PATH}/wrapped', path = 'indirect/', key = '${KEY}')"
echo "with the opt-in: wrap it with from_zk: $(run ${OPT_IN} -q "$(create t_indirect "${INDIRECT_WRAPPER}")")"
$CLICKHOUSE_KEEPER_CLIENT -q "set '${ZK_PATH}/wrapped' '${PLAIN_DISK}'"
echo "without the opt-in: the same definition after the node changed: $(run -q "$(create t5 "${INDIRECT_WRAPPER}")")"

# A table on a disk gets the storage policy `__<disk>`, which names the disk as well.
echo "without the opt-in: the storage policy of the disk with server credentials: $(run -q "CREATE TABLE t6 (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = '__${SERVER_DISK}'")"
echo "without the opt-in: the storage policy of the disk without server credentials: $(run -q "CREATE TABLE t7 (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = '__${PLAIN_DISK}'")"
echo "without the opt-in: change to the storage policy of the disk with server credentials: $(run -q "ALTER TABLE t7 MODIFY SETTING storage_policy = '__${SERVER_DISK}'")"

${CLICKHOUSE_CLIENT} -q "SELECT name FROM system.tables WHERE database = currentDatabase() ORDER BY name"

$CLICKHOUSE_KEEPER_CLIENT -q "rmr '${ZK_PATH}'"
