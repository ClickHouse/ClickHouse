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
u_sub="u_sub_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_virt="u_virt_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_json="u_json_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_revoke="u_revoke_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_dyn="u_dyn_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_bogus="u_bogus_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_tmp="u_tmp_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_mvdef="u_mvdef_${CLICKHOUSE_TEST_UNIQUE_NAME}"
u_create="u_create_${CLICKHOUSE_TEST_UNIQUE_NAME}"
session="session_${CLICKHOUSE_TEST_UNIQUE_NAME}"
# `CREATE` in a `Replicated` database is analyzed without the user, so the view arms use an `Atomic` one.
db_atomic="${CLICKHOUSE_DATABASE}_atomic"

drop_users()
{
    ${CLICKHOUSE_CLIENT} --query "DROP USER IF EXISTS ${u_alias}, ${u_col}, ${u_showcol}, ${u_both}, ${u_show}, ${u_sub}, ${u_virt}, ${u_json}, ${u_revoke}, ${u_dyn}, ${u_bogus}, ${u_tmp}, ${u_mvdef}, ${u_create}"
}

${CLICKHOUSE_CLIENT} --query "DROP DATABASE IF EXISTS ${db_atomic}"
drop_users
${CLICKHOUSE_CLIENT} -m --query "
    DROP TABLE IF EXISTS alias_t;
    DROP TABLE IF EXISTS chain_a1;
    DROP TABLE IF EXISTS chain_a2;
    DROP TABLE IF EXISTS alias_one;
    DROP TABLE IF EXISTS alias_dynamic;
    DROP TABLE IF EXISTS hidden_target;
    DROP TABLE IF EXISTS dynamic_target;

    CREATE TABLE hidden_target (secret_column_1 UInt32, secret_column_2 Nullable(String), secret_column_3 JSON) ENGINE = MergeTree ORDER BY secret_column_1;
    INSERT INTO hidden_target VALUES (1, 'a', '{\"k\": 5}');
    CREATE TABLE alias_t ENGINE = Alias(currentDatabase(), 'hidden_target');
    -- An Alias cannot be created over an existing Alias, so the chain is built from its first hop.
    CREATE TABLE chain_a1 ENGINE = Alias(currentDatabase(), 'chain_a2');
    CREATE TABLE chain_a2 ENGINE = Alias(currentDatabase(), 'hidden_target');
    CREATE TABLE alias_one ENGINE = Alias('system', 'one');
    CREATE TABLE dynamic_target (j JSON) ENGINE = Memory;
    INSERT INTO dynamic_target VALUES ('{\"k\": 9}');
    CREATE TABLE alias_dynamic ENGINE = Alias(currentDatabase(), 'dynamic_target');

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

    CREATE USER ${u_sub} NOT IDENTIFIED;
    GRANT SELECT ON alias_t TO ${u_sub};
    GRANT SELECT(\`secret_column_2.null\`) ON hidden_target TO ${u_sub};

    CREATE USER ${u_virt} NOT IDENTIFIED;
    GRANT SELECT ON alias_t TO ${u_virt};
    GRANT SELECT(_part) ON hidden_target TO ${u_virt};

    CREATE USER ${u_json} NOT IDENTIFIED;
    GRANT SELECT ON alias_t TO ${u_json};
    GRANT SELECT(\`secret_column_3.k\`) ON hidden_target TO ${u_json};

    CREATE USER ${u_revoke} NOT IDENTIFIED;
    GRANT SELECT ON alias_t TO ${u_revoke};
    GRANT SELECT ON hidden_target TO ${u_revoke};
    REVOKE SELECT(secret_column_1, secret_column_2, secret_column_3) ON hidden_target FROM ${u_revoke};

    CREATE USER ${u_dyn} NOT IDENTIFIED;
    GRANT SELECT ON alias_dynamic TO ${u_dyn};
    GRANT SELECT ON dynamic_target TO ${u_dyn};
    REVOKE SELECT(j, _table, _database) ON dynamic_target FROM ${u_dyn};

    CREATE USER ${u_bogus} NOT IDENTIFIED;
    GRANT SELECT ON alias_t TO ${u_bogus};
    GRANT SELECT(\`secret_column_1.no_such_subcolumn\`) ON hidden_target TO ${u_bogus};

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

echo "-- grants on the alias and on a subcolumn or a virtual column of the target"
as_user "${u_sub}" "SELECT secret_column_2.null FROM alias_t"
as_user "${u_virt}" "SELECT notEmpty(_part) FROM alias_t"
as_user "${u_json}" "SELECT secret_column_3.k FROM alias_t"
as_user "${u_revoke}" "SELECT notEmpty(_part), secret_column_2.null FROM alias_t"
as_user "${u_dyn}" "SELECT j.k FROM alias_dynamic"
check_denied "$(as_user "${u_bogus}" "EXPLAIN QUERY TREE SELECT * FROM alias_t")"

echo "-- a temporary alias"
session_query "CREATE TEMPORARY TABLE t_alias ENGINE = Alias('${db}', 'hidden_target')"
check_ok "$(session_query "EXPLAIN QUERY TREE SELECT * FROM t_alias")"
${CLICKHOUSE_CLIENT} --query "REVOKE SHOW COLUMNS ON ${db}.hidden_target FROM ${u_tmp}"
check_denied "$(session_query "EXPLAIN QUERY TREE SELECT * FROM t_alias")"

${CLICKHOUSE_CLIENT} -m --query "
    CREATE DATABASE ${db_atomic} ENGINE = Atomic;
    CREATE TABLE ${db_atomic}.hidden_target (secret_column_1 UInt32, secret_column_2 String) ENGINE = MergeTree ORDER BY secret_column_1;
    CREATE TABLE ${db_atomic}.alias_t ENGINE = Alias('${db_atomic}', 'hidden_target');
    CREATE TABLE ${db_atomic}.mv_dst (secret_column_1 UInt32) ENGINE = MergeTree ORDER BY tuple();

    CREATE USER ${u_mvdef} NOT IDENTIFIED;
    GRANT SELECT ON ${db_atomic}.alias_t TO ${u_mvdef};
    GRANT INSERT ON ${db_atomic}.mv_dst TO ${u_mvdef};

    CREATE USER ${u_create} NOT IDENTIFIED;
    GRANT SELECT ON ${db_atomic}.alias_t TO ${u_create};
    GRANT CREATE VIEW ON ${db_atomic}.* TO ${u_create};
    GRANT SELECT, INSERT ON ${db_atomic}.mv_dst TO ${u_create};
"

echo "-- a materialized view over the alias, defined by a user granted on the alias only"
${CLICKHOUSE_CLIENT} --query "CREATE MATERIALIZED VIEW ${db_atomic}.mv TO ${db_atomic}.mv_dst DEFINER = ${u_mvdef} SQL SECURITY DEFINER AS SELECT secret_column_1 FROM ${db_atomic}.alias_t"
${CLICKHOUSE_CLIENT} --query "INSERT INTO ${db_atomic}.alias_t SETTINGS async_insert = 0 VALUES (7, 'x')"
${CLICKHOUSE_CLIENT} --query "SELECT secret_column_1 FROM ${db_atomic}.mv_dst"

echo "-- a view and a materialized view over the alias, created by a user granted on the alias only"
check_denied "$(as_user "${u_create}" "CREATE VIEW ${db_atomic}.v AS SELECT * FROM ${db_atomic}.alias_t")"
check_denied "$(as_user "${u_create}" "CREATE MATERIALIZED VIEW ${db_atomic}.mv_create TO ${db_atomic}.mv_dst AS SELECT secret_column_1 FROM ${db_atomic}.alias_t")"

${CLICKHOUSE_CLIENT} --query "DROP DATABASE ${db_atomic}"
drop_users
${CLICKHOUSE_CLIENT} -m --query "
    DROP TABLE alias_t;
    DROP TABLE chain_a1;
    DROP TABLE chain_a2;
    DROP TABLE alias_one;
    DROP TABLE alias_dynamic;
    DROP TABLE hidden_target;
    DROP TABLE dynamic_target;
"
