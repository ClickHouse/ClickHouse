#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Cases of `standard` name matching that go through paths other than plain column lookup.
CLIENT_STANDARD="${CLICKHOUSE_CLIENT} --database_and_table_name_matching=standard --column_and_query_name_matching=standard"

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE Join_Review_Cases (k UInt64, v String) ENGINE = Join(ANY, LEFT, k);
    INSERT INTO Join_Review_Cases VALUES (1, 'a');
    CREATE TABLE T_Review_Cases (Col Int32, Items Nested(Name String), Data Tuple(Name String)) ENGINE = Memory;
    INSERT INTO T_Review_Cases VALUES (1, ['x'], tuple('y'));"

echo '--- joinGet keeps a double-quoted table name exact'
${CLIENT_STANDARD} --query "SELECT joinGet(join_review_cases, 'v', toUInt64(1))"
${CLIENT_STANDARD} --query "SELECT joinGet(\"Join_Review_Cases\", 'v', toUInt64(1))"
${CLIENT_STANDARD} --query "SELECT joinGet(\"join_review_cases\", 'v', toUInt64(1))" 2>&1 | grep -oF 'ILLEGAL_TYPE_OF_ARGUMENT' | uniq

echo '--- a double-quoted alias of a table is pinned'
${CLIENT_STANDARD} --query 'SELECT a.col FROM t_review_cases AS A'
${CLIENT_STANDARD} --query 'SELECT "A".col FROM t_review_cases AS "A"'
${CLIENT_STANDARD} --query 'SELECT a.col FROM t_review_cases AS "A"' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq

echo '--- temporary tables'
${CLIENT_STANDARD} --query "
    CREATE TEMPORARY TABLE Tmp_Review_Cases (x Int32);
    INSERT INTO Tmp_Review_Cases VALUES (5);
    SELECT * FROM tmp_review_cases;
    SELECT * FROM \"Tmp_Review_Cases\";"
${CLIENT_STANDARD} --query "
    CREATE TEMPORARY TABLE Tmp_Review_Cases (x Int32);
    SELECT * FROM \"tmp_review_cases\"" 2>&1 | grep -oF 'UNKNOWN_TABLE' | uniq
${CLIENT_STANDARD} --query "
    CREATE TEMPORARY TABLE Tmp_Review_Cases (x Int32);
    CREATE TEMPORARY TABLE \"tmp_review_cases\" (x Int32);
    SELECT * FROM TMP_REVIEW_CASES" 2>&1 | grep -oF 'AMBIGUOUS_IDENTIFIER' | uniq

echo '--- Nested prefix'
${CLIENT_STANDARD} --query 'SELECT items FROM t_review_cases'
${CLIENT_STANDARD} --query 'SELECT "items" FROM t_review_cases' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq

echo '--- subcolumns of a prefix bound to an ARRAY JOIN alias, a lambda argument or an alias'
${CLIENT_STANDARD} --query 'SELECT x.name FROM t_review_cases ARRAY JOIN [data] AS X'
${CLIENT_STANDARD} --query 'SELECT arrayMap(D -> d.name, [data]) FROM t_review_cases'
${CLIENT_STANDARD} --query 'SELECT data AS D, d.name FROM t_review_cases'
${CLIENT_STANDARD} --query 'SELECT x."name" FROM t_review_cases ARRAY JOIN [data] AS X' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq

echo '--- qualified matchers keep the quotes of the qualifier'
${CLIENT_STANDARD} --query 'SELECT "T".* FROM (SELECT 1 AS x) AS "T"'
${CLIENT_STANDARD} --query 'SELECT t.* FROM (SELECT 1 AS x) AS "T"' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
