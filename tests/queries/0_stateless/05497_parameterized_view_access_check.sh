#!/usr/bin/env bash
# A parameterized view must not expose its definition, columns, parameters or the tables behind it to a user
# who cannot see the view, including through `EXPLAIN QUERY TREE` (https://github.com/ClickHouse/ClickHouse/issues/78938).

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

username="user_${CLICKHOUSE_TEST_UNIQUE_NAME}"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP VIEW IF EXISTS v_over_pv;
    DROP VIEW IF EXISTS pv_definer;
    DROP VIEW IF EXISTS pv_invoker;
    DROP VIEW IF EXISTS pv_sorted;
    DROP VIEW IF EXISTS numbers;
    DROP TABLE IF EXISTS alias_base;
    DROP TABLE IF EXISTS secret_base;
    DROP TABLE IF EXISTS sink_dist;
    DROP TABLE IF EXISTS sink_local;

    CREATE TABLE secret_base (x UInt32, s String) ENGINE = MergeTree ORDER BY x;
    INSERT INTO secret_base VALUES (1, 'a'), (2, 'b'), (5, 'c');
    CREATE TABLE alias_base ENGINE = Alias('secret_base');
    CREATE VIEW pv_definer SQL SECURITY DEFINER AS SELECT x AS secret_column_1, s AS secret_column_2 FROM secret_base WHERE x < {secret_param:UInt32};
    CREATE VIEW pv_invoker SQL SECURITY INVOKER AS SELECT x AS secret_column_1, s AS secret_column_2 FROM secret_base WHERE x < {secret_param:UInt32};
    CREATE VIEW pv_sorted SQL SECURITY INVOKER AS SELECT x AS secret_column_1, s AS secret_column_2 FROM secret_base WHERE x < {secret_param:UInt32} ORDER BY {secret_sort:Identifier};
    CREATE VIEW numbers AS SELECT {n:UInt64} AS a, 'x' AS b;
    CREATE VIEW v_over_pv SQL SECURITY DEFINER AS SELECT * FROM pv_definer(secret_param = 3);
    CREATE TABLE sink_local (a UInt32, b String) ENGINE = MergeTree ORDER BY a;
    CREATE TABLE sink_dist AS sink_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), sink_local);

    CREATE USER ${username} NOT IDENTIFIED;
"

as_user()
{
    ${CLICKHOUSE_CLIENT} --user="${username}" --query "$1" 2>&1
}

# The error must be ACCESS_DENIED and must not disclose anything from the view definition. The probe
# query itself is echoed back by the client, so check only the exception text before the first parenthesis.
expect_denied()
{
    local output
    output=$(as_user "$1")
    if [[ "${output}" != *"ACCESS_DENIED"* ]]; then
        echo "FAIL: expected ACCESS_DENIED: ${output}"
        return
    fi
    local messages
    messages=$(grep -oE "DB::Exception: [^(]*" <<< "${output}")
    if [[ "${messages}" == *"secret"* ]]; then
        echo "FAIL: the error message leaks the view definition: ${output}"
    else
        echo "denied"
    fi
}

expect_ok()
{
    local output
    output=$(as_user "$1")
    if [[ "${output}" == *"Exception"* ]]; then
        echo "FAIL: expected success: ${output}"
    else
        echo "ok"
    fi
}

echo "-- no grants"
expect_denied "EXPLAIN QUERY TREE SELECT * FROM pv_definer(secret_param = 3)"
expect_denied "EXPLAIN QUERY TREE SELECT count() FROM pv_definer(secret_param = 3)"
expect_denied "EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM ${CLICKHOUSE_DATABASE}.pv_definer(secret_param = 3)"
expect_denied "EXPLAIN QUERY TREE SELECT 1 WHERE 1 IN (SELECT secret_column_1 FROM pv_definer(secret_param = 3))"

echo "-- no grants: errors must not reveal columns, parameters or the tables behind the view"
expect_denied "SELECT no_such_column FROM pv_definer(secret_param = 3)"
expect_denied "SELECT * FROM pv_definer()"
expect_denied "SELECT * FROM pv_invoker(secret_param = 3)"

echo "-- INSERT SELECT into a Distributed table resolves the view before the analyzer"
${CLICKHOUSE_CLIENT} --query "GRANT INSERT ON ${CLICKHOUSE_DATABASE}.sink_dist, INSERT ON ${CLICKHOUSE_DATABASE}.sink_local TO ${username}"
expect_denied "INSERT INTO sink_dist SETTINGS parallel_distributed_insert_select = 2 SELECT * FROM pv_definer()"
expect_denied "INSERT INTO sink_dist SETTINGS parallel_distributed_insert_select = 2 SELECT * FROM pv_invoker(secret_param = 3)"
echo "-- a view named like a table function does not hide the table function"
expect_ok "INSERT INTO sink_dist SETTINGS parallel_distributed_insert_select = 2, distributed_foreground_insert = 1 SELECT number, toString(number) FROM numbers(3)"
${CLICKHOUSE_CLIENT} --query "REVOKE INSERT ON ${CLICKHOUSE_DATABASE}.sink_dist, INSERT ON ${CLICKHOUSE_DATABASE}.sink_local FROM ${username}"

echo "-- SELECT on the table behind the view, no grant on the view"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.secret_base TO ${username}"
expect_denied "EXPLAIN QUERY TREE SELECT * FROM pv_invoker(secret_param = 3)"
echo "-- an invalid Identifier parameter: the analysis error must not quote the definition"
expect_denied "EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT * FROM pv_sorted(secret_param = 3, secret_sort = 'missing')"
expect_denied "EXPLAIN AST optimize = 1 SELECT * FROM pv_sorted(secret_param = 3, secret_sort = 'missing')"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.secret_base FROM ${username}"

echo "-- SHOW TABLES on the view does not make its columns visible"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW TABLES ON ${CLICKHOUSE_DATABASE}.pv_definer TO ${username}"
expect_denied "EXPLAIN QUERY TREE SELECT * FROM pv_definer(secret_param = 3)"
${CLICKHOUSE_CLIENT} --query "REVOKE SHOW TABLES ON ${CLICKHOUSE_DATABASE}.pv_definer FROM ${username}"

echo "-- a grant on a single column makes the view structure visible"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(secret_column_1) ON ${CLICKHOUSE_DATABASE}.pv_definer TO ${username}"
as_user "SELECT secret_column_1 FROM pv_definer(secret_param = 3) ORDER BY 1"
as_user "SELECT count() FROM pv_definer(secret_param = 3)"
expect_ok "EXPLAIN QUERY TREE SELECT * FROM pv_definer(secret_param = 3)"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(secret_column_1) ON ${CLICKHOUSE_DATABASE}.pv_definer FROM ${username}"

echo "-- a grant on a virtual column alone"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(_table) ON ${CLICKHOUSE_DATABASE}.pv_definer TO ${username}"
as_user "SELECT DISTINCT _table FROM pv_definer(secret_param = 3)"
expect_ok "EXPLAIN QUERY TREE SELECT _table FROM pv_definer(secret_param = 3)"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(_table) ON ${CLICKHOUSE_DATABASE}.pv_definer FROM ${username}"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(_table) ON ${CLICKHOUSE_DATABASE}.secret_base TO ${username}"
as_user "SELECT DISTINCT _table FROM secret_base"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(_table) ON ${CLICKHOUSE_DATABASE}.secret_base FROM ${username}"
echo "-- a grant on a virtual column of an Alias also needs the target"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(_table) ON ${CLICKHOUSE_DATABASE}.alias_base TO ${username}"
expect_denied "EXPLAIN QUERY TREE SELECT * FROM alias_base"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT(_table) ON ${CLICKHOUSE_DATABASE}.secret_base TO ${username}"
expect_ok "SELECT _table FROM alias_base"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(_table) ON ${CLICKHOUSE_DATABASE}.alias_base FROM ${username}"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT(_table) ON ${CLICKHOUSE_DATABASE}.secret_base FROM ${username}"

echo "-- SHOW COLUMNS also makes the structure visible, but reading is still denied"
${CLICKHOUSE_CLIENT} --query "GRANT SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.pv_definer TO ${username}"
expect_ok "EXPLAIN QUERY TREE SELECT * FROM pv_definer(secret_param = 3)"
as_user "SELECT * FROM pv_definer(secret_param = 3)" | grep -o "ACCESS_DENIED" | uniq
${CLICKHOUSE_CLIENT} --query "REVOKE SHOW COLUMNS ON ${CLICKHOUSE_DATABASE}.pv_definer FROM ${username}"

echo "-- SELECT on the view"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.pv_definer TO ${username}"
as_user "SELECT * FROM pv_definer(secret_param = 3) ORDER BY 1 FORMAT TSV"
${CLICKHOUSE_CLIENT} --query "REVOKE SELECT ON ${CLICKHOUSE_DATABASE}.pv_definer FROM ${username}"

echo "-- a SQL SECURITY DEFINER view reads the parameterized view as its definer"
${CLICKHOUSE_CLIENT} --query "GRANT SELECT ON ${CLICKHOUSE_DATABASE}.v_over_pv TO ${username}"
as_user "SELECT count() FROM v_over_pv"
expect_ok "EXPLAIN QUERY TREE SELECT * FROM v_over_pv"

${CLICKHOUSE_CLIENT} -m --query "
    DROP USER IF EXISTS ${username};
    DROP VIEW IF EXISTS v_over_pv;
    DROP VIEW IF EXISTS pv_definer;
    DROP VIEW IF EXISTS pv_invoker;
    DROP VIEW IF EXISTS pv_sorted;
    DROP VIEW IF EXISTS numbers;
    DROP TABLE IF EXISTS alias_base;
    DROP TABLE IF EXISTS secret_base;
    DROP TABLE IF EXISTS sink_dist;
    DROP TABLE IF EXISTS sink_local;
"
