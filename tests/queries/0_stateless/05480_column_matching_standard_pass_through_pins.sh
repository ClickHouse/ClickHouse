#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Under `standard` name matching a projection that passes a column through without an alias
# keeps the double-quote pin of the source name, so folded outer references must not match it.
CLIENT_STANDARD="${CLICKHOUSE_CLIENT} --column_and_query_name_matching=standard"

echo '--- direct'
${CLIENT_STANDARD} --query 'SELECT foo FROM (SELECT 1 AS "Foo")' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query 'SELECT "Foo" FROM (SELECT 1 AS "Foo")'

echo '--- through an asterisk'
${CLIENT_STANDARD} --query 'SELECT foo FROM (SELECT * FROM (SELECT 1 AS "Foo"))' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query 'SELECT foo FROM (SELECT * FROM (SELECT * FROM (SELECT 1 AS "Foo")))' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query 'SELECT "Foo" FROM (SELECT * FROM (SELECT * FROM (SELECT 1 AS "Foo")))'

echo '--- through an explicit column reference'
${CLIENT_STANDARD} --query 'SELECT foo FROM (SELECT "Foo" FROM (SELECT 1 AS "Foo"))' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --query 'SELECT "Foo" FROM (SELECT "Foo" FROM (SELECT 1 AS "Foo"))'

echo '--- through a CTE'
${CLIENT_STANDARD} --query 'WITH c AS (SELECT 1 AS "Foo") SELECT foo FROM (SELECT * FROM c)' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq

echo '--- an alias defines a new name'
${CLIENT_STANDARD} --query 'SELECT foo FROM (SELECT "Foo" AS foo FROM (SELECT 1 AS "Foo"))'
${CLIENT_STANDARD} --query 'SELECT FOO FROM (SELECT "Foo" AS Bar, "Foo" + 1 AS FOO FROM (SELECT 1 AS "Foo"))'

echo '--- unquoted definitions stay foldable'
${CLIENT_STANDARD} --query 'SELECT foo FROM (SELECT * FROM (SELECT 1 AS Foo))'

echo '--- a query parameter keeps the quotes of its alias'
${CLIENT_STANDARD} --param_p=1 --query 'SELECT {p:UInt8} AS "Foo", foo' 2>&1 | grep -oF 'UNKNOWN_IDENTIFIER' | uniq
${CLIENT_STANDARD} --param_p=1 --query 'SELECT {p:UInt8} AS "Foo", "Foo"'
${CLIENT_STANDARD} --param_p=1 --query 'SELECT {p:UInt8} AS Foo, foo'
