#!/usr/bin/env bash
# Tags: no-parallel

# SYSTEM STOP RELOAD DICTIONARIES and SYSTEM START RELOAD DICTIONARIES, also ON CLUSTER, need the SYSTEM RELOAD DICTIONARY grant,
# and a denied one leaves dictionary loading as it was.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="u_${CLICKHOUSE_DATABASE}"

# Reloads are stopped server-wide: never leave them stopped, even when the test fails.
trap '$CLICKHOUSE_CLIENT --query "SYSTEM START RELOAD DICTIONARIES"; $CLICKHOUSE_CLIENT --query "DROP USER IF EXISTS ${user}"' EXIT

$CLICKHOUSE_CLIENT <<EOF
DROP USER IF EXISTS ${user};
CREATE USER ${user};
-- Enough to issue ON CLUSTER queries, so that their check of the dictionary grant is reached.
GRANT CLUSTER ON *.* TO ${user};

CREATE TABLE ${CLICKHOUSE_DATABASE}.src (id UInt64, val Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO ${CLICKHOUSE_DATABASE}.src VALUES (1, 100);

CREATE DICTIONARY ${CLICKHOUSE_DATABASE}.dict (id UInt64, val Int64)
PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'src' DB '${CLICKHOUSE_DATABASE}'))
LAYOUT(FLAT())
LIFETIME(0);
EOF

# A dictionary that is not loaded yet cannot be loaded while reloads are stopped.
function check_loading()
{
    $CLICKHOUSE_CLIENT --query "SYSTEM UNLOAD DICTIONARY ${CLICKHOUSE_DATABASE}.dict"
    if $CLICKHOUSE_CLIENT --query "SELECT dictGet('${CLICKHOUSE_DATABASE}.dict', 'val', toUInt64(1))" > /dev/null 2>&1; then
        echo "loading: running"
    else
        echo "loading: stopped"
    fi
}

function run_as_user()
{
    local out
    if out=$($CLICKHOUSE_CLIENT --user "${user}" --distributed_ddl_output_mode=none --send_logs_level=fatal --query "$1" 2>&1); then
        echo "$1: done"
    elif echo "$out" | grep -q "necessary to have the grant SYSTEM RELOAD DICTIONARY ON \*\.\*. (ACCESS_DENIED)"; then
        echo "$1: denied"
    else
        echo "$1: $out"
    fi
}

check_loading
run_as_user "SYSTEM STOP RELOAD DICTIONARIES"
run_as_user "SYSTEM STOP RELOAD DICTIONARIES ON CLUSTER test_shard_localhost"
check_loading

$CLICKHOUSE_CLIENT --query "SYSTEM STOP RELOAD DICTIONARIES"
check_loading
run_as_user "SYSTEM START RELOAD DICTIONARIES"
run_as_user "SYSTEM START RELOAD DICTIONARIES ON CLUSTER test_shard_localhost"
check_loading

$CLICKHOUSE_CLIENT --query "GRANT SYSTEM RELOAD DICTIONARY ON *.* TO ${user}"
run_as_user "SYSTEM START RELOAD DICTIONARIES"
check_loading
run_as_user "SYSTEM STOP RELOAD DICTIONARIES ON CLUSTER test_shard_localhost"
check_loading
run_as_user "SYSTEM START RELOAD DICTIONARIES ON CLUSTER test_shard_localhost"
check_loading
