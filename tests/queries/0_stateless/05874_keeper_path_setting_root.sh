#!/usr/bin/env bash
# A Keeper path setting equal to '/' is rejected with BAD_ARGUMENTS instead of aborting the process.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

TESTDIR="${CLICKHOUSE_TMP}/${CLICKHOUSE_DATABASE}_keeper_path_root"
trap 'rm -rf "$TESTDIR"' EXIT
rm -rf "$TESTDIR"
mkdir -p "$TESTDIR"

MESSAGE="ZooKeeper path must not be empty or '/'"

function check_local()
{
    echo "<clickhouse>$2</clickhouse>" > "$TESTDIR/$1.xml"
    timeout 120 $CLICKHOUSE_LOCAL --config-file "$TESTDIR/$1.xml" --query "SELECT 1" > "$TESTDIR/$1.out" 2>&1
    local rc=$?
    echo "$1: exit $rc, rejected $(grep -c -m1 -F "$MESSAGE" "$TESTDIR/$1.out")"
}

check_local udf "<user_defined_zookeeper_path>/</user_defined_zookeeper_path>"
check_local workload "<workload_zookeeper_path>/</workload_zookeeper_path>"
check_local udf_valid_path "<user_defined_zookeeper_path>/udf</user_defined_zookeeper_path>"

# clickhouse-local does not create the replicated access storage, so this case needs the server.
mkdir -p "$TESTDIR/server"
echo "<clickhouse><profiles><default/></profiles><users><default><password></password><profile>default</profile><quota>default</quota></default></users><quotas><default/></quotas></clickhouse>" > "$TESTDIR/users.xml"
cat > "$TESTDIR/server.xml" <<EOF
<clickhouse>
    <logger><console>1</console></logger>
    <path>$TESTDIR/server/</path>
    <tmp_path>$TESTDIR/server/tmp/</tmp_path>
    <user_files_path>$TESTDIR/server/user_files/</user_files_path>
    <user_directories>
        <users_xml><path>$TESTDIR/users.xml</path></users_xml>
        <replicated><zookeeper_path>/</zookeeper_path></replicated>
    </user_directories>
</clickhouse>
EOF
timeout 300 ${CLICKHOUSE_SERVER_BINARY} --config-file "$TESTDIR/server.xml" > "$TESTDIR/server.out" 2>&1
rc=$?
echo "replicated_access: exit $rc, rejected $(grep -c -m1 -F "$MESSAGE" "$TESTDIR/server.out")"
