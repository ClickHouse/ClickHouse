#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `min_bytes_to_rebalance_partition_over_jbod`, a merged part goes to the disk of the JBOD volume
# that holds the fewest bytes of big parts of the same partition. In a new partition every disk ties,
# and the tie is broken by the most unreserved space. Both disks are on one filesystem, so
# `keep_free_space_bytes` makes `jbod_full` report 1 GiB less unreserved space than `jbod_empty`.
# Each partition is merged once, so every merge starts from a tie, and every merged part must land on
# `jbod_empty`. A JBOD volume is built from the configuration only, hence `clickhouse-local`.

WORK="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_jbod_balancer"
CONFIG="${WORK}/config.xml"
mkdir -p "${WORK}"

cat > "${CONFIG}" <<XML
<clickhouse>
    <storage_configuration>
        <disks>
            <jbod_full>
                <type>local</type>
                <path>${WORK}/full/</path>
                <keep_free_space_bytes>1073741824</keep_free_space_bytes>
            </jbod_full>
            <jbod_empty>
                <type>local</type>
                <path>${WORK}/empty/</path>
            </jbod_empty>
        </disks>
        <policies>
            <jbod_policy>
                <volumes>
                    <jbod_volume>
                        <disk>jbod_full</disk>
                        <disk>jbod_empty</disk>
                    </jbod_volume>
                </volumes>
            </jbod_policy>
        </policies>
    </storage_configuration>
</clickhouse>
XML

${CLICKHOUSE_LOCAL} --config-file="${CONFIG}" --path="${WORK}/db" -q "
    -- min_bytes_to_rebalance_partition_over_jbod must be at least max_bytes_to_merge_at_max_space_in_pool / 1024.
    CREATE TABLE t (p UInt8, d String)
    ENGINE = MergeTree
    PARTITION BY p
    ORDER BY tuple()
    SETTINGS
        storage_policy = 'jbod_policy',
        min_bytes_to_rebalance_partition_over_jbod = 10240,
        max_bytes_to_merge_at_max_space_in_pool = 10485760;

    SYSTEM STOP MERGES t;
    INSERT INTO t SELECT number % 10, randomPrintableASCII(1000) FROM numbers(1000);
    INSERT INTO t SELECT number % 10, randomPrintableASCII(1000) FROM numbers(1000);
    SYSTEM START MERGES t;

    OPTIMIZE TABLE t FINAL;

    SELECT disk_name, count() FROM system.parts
    WHERE database = currentDatabase() AND table = 't' AND active AND level > 0
    GROUP BY disk_name;
"

rm -rf "${WORK}"
