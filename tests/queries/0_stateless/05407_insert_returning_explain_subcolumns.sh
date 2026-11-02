#!/usr/bin/env bash
# EXPLAIN PIPELINE of INSERT ... RETURNING does not insert, the inserted data cannot be read during the analysis,
# subcolumns of the inserted data can be read, and the destinations which read from other tables are rejected.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_returning_explain;
    CREATE TABLE t_returning_explain (x UInt64) ENGINE = Memory;
"

echo "EXPLAIN PIPELINE does not insert"
for returning in \
    "SELECT (SELECT sum(x) FROM t_returning_explain)" \
    "SELECT 1 WHERE EXISTS (SELECT x FROM t_returning_explain)" \
    "SELECT sum(x) FROM t_returning_explain"
do
    ${CLICKHOUSE_CLIENT} --query "EXPLAIN PIPELINE INSERT INTO t_returning_explain RETURNING ($returning) SELECT number FROM numbers(3)" > /dev/null
done
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_returning_explain"

echo "The inserted data cannot be read during the analysis"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_returning_explain RETURNING (SELECT (SELECT sum(x) FROM t_returning_explain)) SELECT number FROM numbers(3)" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_returning_explain RETURNING (SELECT 1 WHERE EXISTS (SELECT x FROM t_returning_explain)) SELECT number FROM numbers(3)" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_returning_explain"

echo "Subcolumns"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_returning_subcolumns;
    CREATE TABLE t_returning_subcolumns (j JSON, t Tuple(a UInt64, b String), arr Array(UInt64), n Nullable(UInt64)) ENGINE = Memory;
    INSERT INTO t_returning_subcolumns RETURNING (SELECT j.x, t.a, t.b, arr.size0, n.null, arr FROM t_returning_subcolumns)
    VALUES ('{\"x\":1}', (2, 'b'), [3, 4], NULL);
"

echo "Alias"
${CLICKHOUSE_CLIENT} --query "
    DROP TABLE IF EXISTS t_returning_alias;
    DROP TABLE IF EXISTS t_returning_alias_target;
    CREATE TABLE t_returning_alias_target (x UInt64) ENGINE = Memory;
    CREATE TABLE t_returning_alias ENGINE = Alias('t_returning_alias_target') SETTINGS allow_experimental_alias_table_engine = 1;
"
${CLICKHOUSE_CLIENT} --query "INSERT INTO t_returning_alias RETURNING (SELECT x FROM t_returning_alias) SELECT 7" 2>&1 | grep -o -m1 "NOT_IMPLEMENTED"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM t_returning_alias_target"

${CLICKHOUSE_CLIENT} --query "
    DROP TABLE t_returning_alias;
    DROP TABLE t_returning_alias_target;
    DROP TABLE t_returning_subcolumns;
    DROP TABLE t_returning_explain;
"
