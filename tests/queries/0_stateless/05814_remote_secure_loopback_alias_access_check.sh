#!/usr/bin/env bash
# Tags: no-fasttest
# The `RemoteSecure` database engine to an address of this server that is not classified local (e.g. `127.0.0.2`)
# must check the caller's own grants, like `Remote` does in 05396_remote_loopback_alias_access_check.
# Separate from that test because it needs a build with TLS support.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

svc="svc_${CLICKHOUSE_DATABASE}"
bob="bob_${CLICKHOUSE_DATABASE}"
db="${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "
    DROP USER IF EXISTS ${svc}, ${bob};
    CREATE TABLE ${db}.secret (id UInt32, s String) ENGINE = MergeTree ORDER BY id;
    INSERT INTO ${db}.secret VALUES (1, 'secret');
    CREATE USER ${svc} NOT IDENTIFIED HOST LOCAL;
    GRANT SELECT ON ${db}.secret TO ${svc};
    CREATE USER ${bob} IDENTIFIED WITH plaintext_password BY 'bob';
    GRANT REMOTE ON *.* TO ${bob};
"

function as_bob()
{
    local query="$1"
    local output
    output=$(${CLICKHOUSE_CLIENT} --user "${bob}" --password bob --query "${query}" 2>&1)
    if grep -q "ACCESS_DENIED" <<< "${output}"; then
        echo "ACCESS_DENIED"
    else
        echo "${output}"
    fi
}

# Checked before ${bob} gets any grant on ${db}: any table-level grant implies `SHOW TABLES`.
echo "--- without grants"
secure_db="${CLICKHOUSE_DATABASE}_proxy_secure"
${CLICKHOUSE_CLIENT} --query "CREATE DATABASE ${secure_db} ENGINE = RemoteSecure('127.0.0.2:${CLICKHOUSE_PORT_TCP_SECURE}', '${db}', '${svc}', '')"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${secure_db}.* TO ${bob}"
as_bob "SELECT s FROM ${secure_db}.secret"
as_bob "SHOW TABLES FROM ${secure_db}"
as_bob "EXISTS TABLE ${secure_db}.secret"

echo "--- with grants"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${db}.secret TO ${bob}"
as_bob "SELECT s FROM ${secure_db}.secret"
as_bob "SHOW TABLES FROM ${secure_db}"
as_bob "EXISTS TABLE ${secure_db}.secret"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${secure_db}"
${CLICKHOUSE_CLIENT} --query "DROP USER ${svc}, ${bob}"
