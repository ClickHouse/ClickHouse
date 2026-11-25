#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Accessing a table with the `Alias` engine requires the global `USE ALIAS TABLE` grant, for data
# (SELECT including the trivial count shortcut, INSERT, ALTER, OPTIMIZE, TRUNCATE) and for the
# target-derived metadata (DESCRIBE, system tables) alike. The grants on the alias and target
# tables themselves stay necessary: `USE ALIAS TABLE` is a gate, not a replacement.

user="u_${CLICKHOUSE_TEST_UNIQUE_NAME}"
db="${CLICKHOUSE_DATABASE}"

trap '${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"' EXIT

${CLICKHOUSE_CLIENT} -q "DROP USER IF EXISTS ${user}"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${db}.target (x Int32) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE ${db}.al ENGINE = Alias('${db}', 'target')"
${CLICKHOUSE_CLIENT} -q "INSERT INTO ${db}.target VALUES (1), (2)"
${CLICKHOUSE_CLIENT} -q "CREATE USER ${user}"
${CLICKHOUSE_CLIENT} -q "GRANT SELECT, INSERT, ALTER, OPTIMIZE, TRUNCATE, SHOW, CREATE TABLE, DROP TABLE ON ${db}.* TO ${user}"

run_as_user() { ${CLICKHOUSE_CLIENT} --user "${user}" -q "$1" 2>&1; }
# Prints OK on success, otherwise the error code from the server's message. The message text is not
# pinned: for SELECT the refusal comes from different places depending on randomized settings
# (the trivial count shortcut refuses in metadata resolution, a real read in `StorageAlias::read`).
outcome()
{
    local out
    if out=$(run_as_user "$1"); then
        echo "OK"
    else
        grep -oE '\(ACCESS_DENIED\)' <<< "${out}" | head -n1 | tr -d '()'
    fi
}

echo "-- without the grant"
echo -e "select\t$(outcome "SELECT * FROM ${db}.al ORDER BY x")"
echo -e "count\t$(outcome "SELECT count() FROM ${db}.al")"
echo -e "insert\t$(outcome "INSERT INTO ${db}.al VALUES (3)")"
echo -e "alter\t$(outcome "ALTER TABLE ${db}.al UPDATE x = x WHERE 0")"
echo -e "optimize\t$(outcome "OPTIMIZE TABLE ${db}.al")"
echo -e "truncate\t$(outcome "TRUNCATE TABLE ${db}.al")"
echo -e "describe\t$(outcome "DESCRIBE ${db}.al")"
# Creating an alias is a lifecycle operation on the alias's own catalog entry, not gated by the grant.
echo -e "create\t$(outcome "CREATE TABLE ${db}.al2 ENGINE = Alias('${db}', 'target')")"
# The INSERT path always refuses in `StorageAlias`, so there the message names the missing grant.
echo -e "insert_error_names_grant\t$(run_as_user "INSERT INTO ${db}.al VALUES (3)" | grep -q 'grant USE ALIAS TABLE ON' && echo 1 || echo 0)"
echo -e "system_columns\t$(run_as_user "SELECT count() FROM system.columns WHERE database = '${db}' AND table = 'al'")"
echo -e "system_tables_total_rows\t$(run_as_user "SELECT total_rows FROM system.tables WHERE database = '${db}' AND name = 'al'")"
# The direct table is untouched by the gate.
echo -e "select_target\t$(run_as_user "SELECT count() FROM ${db}.target")"

echo "-- with the grant"
${CLICKHOUSE_CLIENT} -q "GRANT USE ALIAS TABLE ON *.* TO ${user}"
echo -e "sum\t$(run_as_user "SELECT sum(x) FROM ${db}.al")"
echo -e "count\t$(run_as_user "SELECT count() FROM ${db}.al")"
echo -e "insert\t$(outcome "INSERT INTO ${db}.al VALUES (3)")"
echo -e "count_after_insert\t$(run_as_user "SELECT count() FROM ${db}.al")"
echo -e "columns\t$(run_as_user "SELECT name, type FROM system.columns WHERE database = '${db}' AND table = 'al' FORMAT TSV")"
echo -e "system_tables_total_rows\t$(run_as_user "SELECT total_rows FROM system.tables WHERE database = '${db}' AND name = 'al'")"

echo "-- the gate does not replace the per-table grants"
${CLICKHOUSE_CLIENT} -q "REVOKE SELECT ON ${db}.target FROM ${user}"
echo -e "select_revoked_target\t$(outcome "SELECT * FROM ${db}.al")"

echo "-- lifecycle of an existing alias works without the grant"
${CLICKHOUSE_CLIENT} -q "REVOKE USE ALIAS TABLE ON *.* FROM ${user}"
echo -e "rename\t$(outcome "RENAME TABLE ${db}.al2 TO ${db}.al3")"
echo -e "drop\t$(outcome "DROP TABLE ${db}.al3")"
