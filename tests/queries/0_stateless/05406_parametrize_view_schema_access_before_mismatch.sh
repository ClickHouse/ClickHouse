#!/usr/bin/env bash
# A parameterized view whose declared schema mismatches its actual output must not reveal the
# declared columns to a user who is not allowed to see them: the `TYPE_MISMATCH` exception is
# reported only after `SHOW COLUMNS` on the declared columns is checked, otherwise `ACCESS_DENIED`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

user="user_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
    DROP USER IF EXISTS ${user};
    CREATE USER ${user} IDENTIFIED WITH no_password;
    SET use_declared_schema_for_parameterized_views = 1;
    CREATE VIEW pv_denied_mismatch (confidential_name String) SQL SECURITY DEFINER AS SELECT toUInt64({p:UInt64}) AS n;
    CREATE VIEW pv_denied_match (n UInt64) SQL SECURITY DEFINER AS SELECT toUInt64({p:UInt64}) AS n;
    CREATE VIEW pv_granted_mismatch (confidential_name String) SQL SECURITY DEFINER AS SELECT toUInt64({p:UInt64}) AS n;
    GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv_granted_mismatch TO ${user};"

check()
{
    local output
    output=$($CLICKHOUSE_CLIENT --user "${user}" -q "$1" 2>&1)
    echo "$output" | grep -o -m1 -E 'ACCESS_DENIED|TYPE_MISMATCH'
    echo "$output" | grep -q 'confidential_name' && echo 'declared column shown' || echo 'declared column hidden'
}

echo '-- no grants, mismatching declared schema'
check "SELECT * FROM pv_denied_mismatch(p = 1)"
echo '-- no grants, matching declared schema'
check "SELECT * FROM pv_denied_match(p = 1)"
echo '-- granted, mismatching declared schema'
check "SELECT * FROM pv_granted_mismatch(p = 1)"

$CLICKHOUSE_CLIENT -q "
    DROP VIEW pv_denied_mismatch;
    DROP VIEW pv_denied_match;
    DROP VIEW pv_granted_mismatch;
    DROP USER ${user};"
