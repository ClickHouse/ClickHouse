#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest
# Tag zookeeper: the disk type is read with `from_zk`.
# Tag no-fasttest: needs the `encrypted` disk, which requires SSL.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The stress test fuzzes DDL and ignores some DROPs: a `__fuzz_N` copy or an undropped table would name the disks,
# policies and Keeper nodes removed below, and the server could not start again.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --ast_fuzzer_any_query=0 --ignore_drop_queries_probability=0"

# A session without `s3_allow_server_credentials_in_user_queries` may not wrap, by name, a disk that a session with it
# created with the server's own credentials, nor a wrapper of such a disk. A `type` read with `from_zk` counts as a
# disk that may use server credentials; here it resolves to an `encrypted` disk, so no object storage is needed.
# The `encrypted` disks wrap local disks under `custom_local_disks_base_directory`. A disk is released when the last
# table using it is dropped, so the tables defining the disks that are named later are kept until the end, and dropped
# before the test ends: a table naming a disk of another table could not be loaded after a restart. The other tables
# are dropped right away.

ZK_PATH="/clickhouse/${CLICKHOUSE_TEST_UNIQUE_NAME}"
KEY="1234567812345678"
SERVER_DISK="server_${CLICKHOUSE_DATABASE}"
WRAPPER_DISK="wrapper_${CLICKHOUSE_DATABASE}"
PLAIN_DISK="plain_${CLICKHOUSE_DATABASE}"

run()
{
    local out
    if out=$(${CLICKHOUSE_CLIENT} --dynamic_disk_allow_from_zk=1 "$@" 2>&1); then echo "OK"; else echo "$out" | grep -oE '\([A-Z_]+\)' | head -1; fi
    ${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS created SYNC"
}
create() { echo "CREATE TABLE $1 (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS disk = $2"; }
wrap() { echo "disk(type = encrypted, disk = '$1', path = '$2/', key = '${KEY}')"; }

OPT_IN="--s3_allow_server_credentials_in_user_queries=1"
local_disk() { echo "disk(type = local, path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}_$1/')"; }

$CLICKHOUSE_KEEPER_CLIENT -q "create '${ZK_PATH}' 'disk'"
$CLICKHOUSE_KEEPER_CLIENT -q "create '${ZK_PATH}/type' 'encrypted'"

echo "with the opt-in: create a disk with server credentials: $(run ${OPT_IN} -q "$(create t_server "disk(name = '${SERVER_DISK}', type = 'from_zk ${ZK_PATH}/type', disk = $(local_disk server), path = 'encrypted/', key = '${KEY}')")")"
echo "with the opt-in: wrap it: $(run ${OPT_IN} -q "$(create t_wrapper "disk(name = '${WRAPPER_DISK}', type = encrypted, disk = '${SERVER_DISK}', path = 'wrapper/', key = '${KEY}')")")"
echo "with the opt-in: create a disk without server credentials: $(run ${OPT_IN} -q "$(create t_plain "disk(name = '${PLAIN_DISK}', type = encrypted, disk = $(local_disk plain), path = 'encrypted/', key = '${KEY}')")")"

echo "without the opt-in: wrap the disk with server credentials: $(run -q "$(create created "$(wrap "${SERVER_DISK}" t1)")")"
echo "without the opt-in: wrap its wrapper: $(run -q "$(create created "$(wrap "${WRAPPER_DISK}" t2)")")"
echo "without the opt-in: wrap the disk without server credentials: $(run -q "$(create created "$(wrap "${PLAIN_DISK}" t3)")")"
echo "with the opt-in: wrap the disk with server credentials: $(run ${OPT_IN} -q "$(create created "$(wrap "${SERVER_DISK}" t4)")")"

# The wrapped disk is read with `from_zk`: after the node changes, the same definition no longer resolves to the disk with
# server credentials, but it still names the disk created from it, which its table keeps registered.
$CLICKHOUSE_KEEPER_CLIENT -q "create '${ZK_PATH}/wrapped' '${SERVER_DISK}'"
INDIRECT_WRAPPER="disk(type = encrypted, disk = 'from_zk ${ZK_PATH}/wrapped', path = 'indirect/', key = '${KEY}')"
echo "with the opt-in: wrap it with from_zk: $(run ${OPT_IN} -q "$(create t_indirect "${INDIRECT_WRAPPER}")")"
$CLICKHOUSE_KEEPER_CLIENT -q "set '${ZK_PATH}/wrapped' '${PLAIN_DISK}'"
echo "without the opt-in: the same definition after the node changed: $(run -q "$(create created "${INDIRECT_WRAPPER}")")"

# A table on a disk gets the storage policy `__<disk>`, which names the disk as well.
echo "without the opt-in: the storage policy of the disk with server credentials: $(run -q "CREATE TABLE created (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = '__${SERVER_DISK}'")"
echo "without the opt-in: the storage policy of the disk without server credentials: $(run -q "CREATE TABLE created (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = '__${PLAIN_DISK}'")"
echo "without the opt-in: change to the storage policy of the disk with server credentials: $(run -q "CREATE TABLE created (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = '__${PLAIN_DISK}'; ALTER TABLE created MODIFY SETTING storage_policy = '__${SERVER_DISK}'")"

# A table that already has the policy can still be altered otherwise.
echo "with the opt-in: the storage policy of the disk with server credentials: $(run ${OPT_IN} -q "CREATE TABLE t_policy (x UInt8) ENGINE = MergeTree ORDER BY x SETTINGS storage_policy = '__${SERVER_DISK}'")"
echo "without the opt-in: alter that table otherwise: $(run -q "ALTER TABLE t_policy ADD COLUMN y UInt8")"
echo "without the opt-in: change another setting of that table: $(run -q "ALTER TABLE t_policy MODIFY SETTING parts_to_throw_insert = 500")"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_policy SYNC"

# Other engines name a disk or a storage policy as well.
echo "without the opt-in: Log on the disk with server credentials: $(run -q "CREATE TABLE created (x UInt8) ENGINE = Log SETTINGS disk = '${SERVER_DISK}'")"
echo "without the opt-in: StripeLog on its storage policy: $(run -q "CREATE TABLE created (x UInt8) ENGINE = StripeLog SETTINGS storage_policy = '__${SERVER_DISK}'")"
echo "without the opt-in: Set on the disk: $(run -q "CREATE TABLE created (x UInt8) ENGINE = Set SETTINGS disk = '${SERVER_DISK}'")"
echo "without the opt-in: Join on the disk: $(run -q "CREATE TABLE created (x UInt8) ENGINE = Join(ANY, LEFT, x) SETTINGS disk = '${SERVER_DISK}'")"
echo "without the opt-in: Distributed on its storage policy: $(run -q "CREATE TABLE created (x UInt8) ENGINE = Distributed('test_shard_localhost', currentDatabase(), t_plain, rand(), '__${SERVER_DISK}')")"
echo "without the opt-in: Log on the disk without server credentials: $(run -q "CREATE TABLE created (x UInt8) ENGINE = Log SETTINGS disk = '${PLAIN_DISK}'")"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_indirect SYNC; DROP TABLE t_wrapper SYNC; DROP TABLE t_server SYNC; DROP TABLE t_plain SYNC"
$CLICKHOUSE_KEEPER_CLIENT -q "rmr '${ZK_PATH}'"
