#!/usr/bin/env bash
# An `Alias` table shows the metadata of its target, so analyzing a query over it (`EXPLAIN QUERY TREE`,
# `EXPLAIN SYNTAX`, the error kind of a missing column) must require access to the target as well.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

db="${CLICKHOUSE_DATABASE}"
u_alias="u_alias_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_col="u_col_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_showcol="u_showcol_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_both="u_both_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_show="u_show_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_tmp="u_tmp_${CLICKHOUSE_TEST_UNIQUE_NAME}"
session="session_${CLICKHOUSE_TEST_UNIQUE_NAME}"

drop_users()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${u_alias}, ${u_col}, ${u_showcol}, ${u_both}, ${u_show}, ${u_tmp}"
}

drop_users
${CLICKHOUSE_CLIENT} -m --query "
    DROP TABLE IF EXISTS alias_t;
    DROP TABLE IF EXISTS chain_a1;
    DROP TABLE IF EXISTS chain_a2;
    DROP TABLE IF EXISTS alias_one;
    DROP TABLE IF EXISTS hidden_target;

    CREATE TABLE hidden_target (secret_column_1 UInt32, secret_column_2 String) ENGINE = MergeTree ORDER BY secret_column_1;
    INSERT INTO hidden_target VALUES (1, 'a');
    CREATE TABLE alias_t ENGINE = Alias(currentDatabase(), 'hidden_target');
    -- An Alias cannot be created over an existing Alias, so the chain is built from its first hop.
    CREATE TABLE chain_a1 ENGINE = Alias(currentDatabase(), 'chain_a2');
    CREATE TABLE chain_a2 ENGINE = Alias(currentDatabase(), 'hidden_target');
    CREATE TABLE alias_one ENGINE = Alias('system', 'one');

    CREATE USER ${u_alias} NOT IDENTIFIED;
    GRANT SELECT ON alias_t TO ${u_alias};
    GRANT SELECT ON chain_a1 TO ${u_alias};
    GRANT SELECT ON chain_a2 TO ${u_alias};
    GRANT SELECT ON alias_one TO ${u_alias};

    CREATE USER ${u_col} NOT IDENTIFIED;
    GRANT SELECT(secret_column_1) ON alias_t TO ${u_col};

    CREATE USER ${u_showcol} NOT IDENTIFIED;
    GRANT SHOW COLUMNS(secret_column_1) ON alias_t TO ${u_showcol};

    CREATE USER ${u_both} NOT IDENTIFIED;
    GRANT SELECT ON alias_t TO ${u_both};
    GRANT SELECT(secret_column_1) ON hidden_target TO ${u_both};

    CREATE USER ${u_show} NOT IDENTIFIED;
    GRANT SELECT ON alias_t TO ${u_show};
    GRANT SHOW COLUMNS ON hidden_target TO ${u_show};

    CREATE USER ${u_tmp} NOT IDENTIFIED;
    GRANT CREATE ARBITRARY TEMPORARY TABLE ON *.* TO ${u_tmp};
    GRANT TABLE ENGINE ON Alias TO ${u_tmp};
    GRANT SHOW COLUMNS ON hidden_target TO ${u_tmp};
"

# The error must be ACCESS_DENIED and must name neither a column nor the target. The probe query is
# echoed back by the client, so check only the exception text cut before the first parenthesis.
check_denied()
{
    local output=$1
    if [[ "${output}" != *"ACCESS_DENIED"* ]]; then
        echo "FAIL: expected ACCESS_DENIED: ${output}"
        return
    fi
    local messages
    messages=$(grep -oE "DB::Exception: [^(]*" <<< "${output}")
    if [[ "${messages}" == *"secret_column"* || "${messages}" == *"hidden_target"* ]]; then
        echo "FAIL: the error message leaks the target: ${output}"
    else
        echo "denied"
    fi
}

check_ok()
{
    local output=$1
    if [[ "${output}" == *"Exception"* ]]; then
        echo "FAIL: expected success: ${output}"
    else
        echo "ok"
    fi
}

as_user()
{
    ${CLICKHOUSE_CLIENT} --user="$1" --query "$2" 2>&1
}

session_query()
{
    ${CLICKHOUSE_CURL} -sS "${CLICKHOUSE_URL}&user=${u_tmp}&password=&session_id=${session}" -d "$1" 2>&1
}

echo "-- grants on the alias only"
check_denied "$(as_user "${u_alias}" "EXPLAIN QUERY TREE SELECT * FROM alias_t")"
check_denied "$(as_user "${u_alias}" "EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM alias_t")"
check_denied "$(as_user "${u_alias}" "EXPLAIN QUERY TREE SELECT * FROM chain_a1")"
check_denied "$(as_user "${u_alias}" "SELECT secret_column_1 FROM alias_t")"
check_denied "$(as_user "${u_alias}" "SELECT no_such_column FROM alias_t")"

echo "-- an alias over an implicitly granted system table"
check_ok "$(as_user "${u_alias}" "EXPLAIN QUERY TREE SELECT * FROM alias_one")"
as_user "${u_alias}" "SELECT * FROM alias_one"

echo "-- a column-level grant on the alias only"
check_denied "$(as_user "${u_col}" "EXPLAIN QUERY TREE SELECT * FROM alias_t")"
check_denied "$(as_user "${u_showcol}" "EXPLAIN QUERY TREE SELECT * FROM alias_t")"

echo "-- grants on the alias and on the target"
check_ok "$(as_user "${u_both}" "EXPLAIN QUERY TREE SELECT secret_column_1 FROM alias_t")"
as_user "${u_both}" "SELECT secret_column_1 FROM alias_t"
check_ok "$(as_user "${u_show}" "EXPLAIN QUERY TREE SELECT * FROM alias_t")"

echo "-- a temporary alias"
session_query "CREATE TEMPORARY TABLE t_alias ENGINE = Alias('${db}', 'hidden_target')"
check_ok "$(session_query "EXPLAIN QUERY TREE SELECT * FROM t_alias")"
${CLICKHOUSE_CLIENT} --query "REVOKE SHOW COLUMNS ON ${db}.hidden_target FROM ${u_tmp}"
check_denied "$(session_query "EXPLAIN QUERY TREE SELECT * FROM t_alias")"

drop_users
${CLICKHOUSE_CLIENT} -m --query "
    DROP TABLE alias_t;
    DROP TABLE chain_a1;
    DROP TABLE chain_a2;
    DROP TABLE alias_one;
    DROP TABLE hidden_target;
"
