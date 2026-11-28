#!/usr/bin/env bash
# Tags: zookeeper, no-fasttest, no-shared-merge-tree
# Tag no-fasttest: creates custom object-storage disks
# Tag no-shared-merge-tree: the parts found under the root are not the part set SharedMergeTree reads

# A table with `table_disk` keeps its data at the root of its disk, and a new table can be created on a root that holds
# only detached parts of another table. A CREATE of such a table that is refused for a column named like a virtual
# column must leave the root as it found it.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The stress test fuzzes DDL and ignores some DROPs: a fuzzed `DETACH` or an undropped table could leave two tables
# on one root, and the server could not load them both after a restart.
CLICKHOUSE_CLIENT="${CLICKHOUSE_CLIENT} --ast_fuzzer_any_query=0 --ignore_drop_queries_probability=0"

DB="${CLICKHOUSE_DATABASE}_td"
${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${DB} SYNC"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${DB} ENGINE = Atomic"

function run_case()
{
    local suffix=$1 engine=$2
    local src="${DB}.src_${suffix}"
    local disk="disk(name = '${CLICKHOUSE_TEST_UNIQUE_NAME}_${suffix}', type = object_storage, object_storage_type = local,
        metadata_type = plain_rewritable, path = '${CLICKHOUSE_DISKS_FILES}/${CLICKHOUSE_TEST_UNIQUE_NAME}_${suffix}/')"

    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${src} (k Int32) ENGINE = MergeTree ORDER BY k
        SETTINGS table_disk = 1, disk = ${disk}, old_parts_lifetime = 1, cleanup_delay_period = 1,
        cleanup_delay_period_random_add = 0, cleanup_thread_preferred_points_per_iteration = 0"
    ${CLICKHOUSE_CLIENT} --query "INSERT INTO ${src} VALUES (1), (2), (3)"
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${src} DETACH PARTITION tuple()"

    # The outdated part has to be removed from the root before another table can be created there.
    for _ in {1..120}; do
        [ "$(${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.parts WHERE database = '${DB}' AND table = 'src_${suffix}'")" = 0 ] && break
        sleep 0.5
    done
    ${CLICKHOUSE_CLIENT} --query "DETACH TABLE ${src} PERMANENTLY SYNC"

    # `_row_exists` is a persistent virtual column of MergeTree.
    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE ${DB}.r_${suffix} (k Int32, _row_exists UInt8) ENGINE = ${engine} ORDER BY k
        SETTINGS table_disk = 1, disk = ${disk}" 2>&1 | grep -cm1 ILLEGAL_COLUMN

    if [ "${suffix}" = rmt ]; then
        ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM system.zookeeper WHERE path = '/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/td_rmt/replicas'"
    fi

    ${CLICKHOUSE_CLIENT} --query "ATTACH TABLE ${src}"
    ${CLICKHOUSE_CLIENT} --query "SELECT groupArray(name) FROM system.detached_parts WHERE database = '${DB}' AND table = 'src_${suffix}'"
    ${CLICKHOUSE_CLIENT} --query "ALTER TABLE ${src} ATTACH PARTITION tuple()"
    ${CLICKHOUSE_CLIENT} --query "SELECT count() FROM ${src}"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE ${src} SYNC"
}

run_case mt "MergeTree"
run_case rmt "ReplicatedMergeTree('/clickhouse/tables/$CLICKHOUSE_TEST_ZOOKEEPER_PREFIX/td_rmt', 'r1')"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${DB} SYNC"
