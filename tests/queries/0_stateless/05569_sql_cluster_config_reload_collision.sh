#!/usr/bin/env bash
# Tags: no-parallel
# no-parallel: `SYSTEM RELOAD CONFIG` rewrites global cluster state, and the test
# creates a SQL-managed cluster whose name must not collide with other tests.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A rejected config reload must not publish a prefix of the new clusters.
# Names are sorted, so `aaa_*` would be installed, `foo_*` would collide with
# the SQL cluster, and `zzz_*` would be skipped unless the update is atomic.

aaa="t05569_aaa"
foo="t05569_foo"
zzz="t05569_zzz"
config="${CLICKHOUSE_CONFIG_DIR}/config.d/${CLICKHOUSE_TEST_UNIQUE_NAME}.xml"

reload_config()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" |& grep -v \
        -e 'Address already in use' \
        -e 'Cannot set max size of core file' || :
}

cleanup()
{
    $CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${foo}" >/dev/null 2>&1 || :
    rm -f "$config"
    reload_config
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${foo}"

$CLICKHOUSE_CLIENT --query "
CREATE CLUSTER ${foo} (
    SHARD (host = '127.0.0.1', port = 9000)
)
"

write_config()
{
    cat > "$config" <<EOF
<clickhouse>
    <remote_servers>
        <${aaa}>
            <shard>
                <replica>
                    <host>127.0.0.1</host>
                    <port>9000</port>
                </replica>
            </shard>
        </${aaa}>
        <${foo}>
            <shard>
                <replica>
                    <host>127.0.0.1</host>
                    <port>9000</port>
                </replica>
            </shard>
        </${foo}>
        <${zzz}>
            <shard>
                <replica>
                    <host>127.0.0.1</host>
                    <port>9000</port>
                </replica>
            </shard>
        </${zzz}>
    </remote_servers>
</clickhouse>
EOF
}

count_new_clusters()
{
    $CLICKHOUSE_CLIENT --query "
        SELECT count()
        FROM system.clusters
        WHERE cluster IN ('${aaa}', '${zzz}')
    "
}

echo "created"
$CLICKHOUSE_CLIENT --query "
    SELECT cluster, source
    FROM system.clusters
    WHERE cluster = '${foo}'
    ORDER BY replica_num
    LIMIT 1
"

write_config

echo "first_reload"
output=$($CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" 2>&1 || true)
if echo "$output" | grep -q 'SQL-managed cluster with the same name already exists'; then
    echo "collision"
else
    echo "no_collision"
    echo "$output"
fi
echo "after_first"
count_new_clusters

echo "second_reload"
output=$($CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" 2>&1 || true)
if echo "$output" | grep -q 'SQL-managed cluster with the same name already exists'; then
    echo "collision"
else
    echo "no_collision"
    echo "$output"
fi
echo "after_second"
count_new_clusters

echo "drop_dns_cache"
output=$($CLICKHOUSE_CLIENT --query "SYSTEM DROP DNS CACHE" 2>&1 || true)
if echo "$output" | grep -q 'already exists'; then
    echo "collision"
else
    echo "ok"
fi

$CLICKHOUSE_CLIENT --query "DROP CLUSTER ${foo}"

echo "after_drop"
reload_config
$CLICKHOUSE_CLIENT --query "
    SELECT cluster, source
    FROM system.clusters
    WHERE cluster IN ('${aaa}', '${foo}', '${zzz}')
    GROUP BY cluster, source
    ORDER BY cluster
"
