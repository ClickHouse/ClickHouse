#!/usr/bin/env bash
# Tags: no-parallel, no-fasttest
# no-parallel: `SYSTEM RELOAD CONFIG` rewrites global cluster state, and the test
# pauses `CREATE CLUSTER` while a config cluster of the same name is installed.
# no-fasttest: enables a server-global failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

foo="t05572_foo"
config="${CLICKHOUSE_CONFIG_DIR}/config.d/${CLICKHOUSE_TEST_UNIQUE_NAME}.xml"
create_log="${CLICKHOUSE_TMP}/${CLICKHOUSE_TEST_UNIQUE_NAME}.create.log"

reload_config()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM RELOAD CONFIG" |& grep -v \
        -e 'Address already in use' \
        -e 'Cannot set max size of core file' || :
}

cleanup()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM DISABLE FAILPOINT sql_cluster_create_pause_after_namespace_check" >/dev/null 2>&1 || :
    $CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${foo}" >/dev/null 2>&1 || :
    rm -f "$config" "$create_log"
    reload_config
}
trap cleanup EXIT

$CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${foo}"
$CLICKHOUSE_CLIENT --query "SYSTEM ENABLE FAILPOINT sql_cluster_create_pause_after_namespace_check"

$CLICKHOUSE_CLIENT --query "
CREATE CLUSTER ${foo} (
    SHARD (host = '127.0.0.1', port = 9000)
)
" >"$create_log" 2>&1 &
create_pid=$!

$CLICKHOUSE_CLIENT --query "SYSTEM WAIT FAILPOINT sql_cluster_create_pause_after_namespace_check PAUSE"

cat > "$config" <<EOF
<clickhouse>
    <remote_servers>
        <${foo}>
            <shard>
                <replica>
                    <host>127.0.0.1</host>
                    <port>9000</port>
                </replica>
            </shard>
        </${foo}>
    </remote_servers>
</clickhouse>
EOF

echo "reload"
reload_config

echo "after_reload"
$CLICKHOUSE_CLIENT --query "
    SELECT cluster, source
    FROM system.clusters
    WHERE cluster = '${foo}'
    ORDER BY replica_num
    LIMIT 1
"

$CLICKHOUSE_CLIENT --query "SYSTEM NOTIFY FAILPOINT sql_cluster_create_pause_after_namespace_check"
wait "$create_pid" || :

echo "create"
if grep -q 'already exists in server configuration' "$create_log"; then
    echo "rejected"
else
    echo "unexpected"
    cat "$create_log"
fi

echo "after_create"
$CLICKHOUSE_CLIENT --query "
    SELECT cluster, source
    FROM system.clusters
    WHERE cluster = '${foo}'
    ORDER BY replica_num
    LIMIT 1
"

$CLICKHOUSE_CLIENT --query "DROP CLUSTER IF EXISTS ${foo}"

echo "after_drop"
$CLICKHOUSE_CLIENT --query "
    SELECT cluster, source
    FROM system.clusters
    WHERE cluster = '${foo}'
    ORDER BY replica_num
    LIMIT 1
"
