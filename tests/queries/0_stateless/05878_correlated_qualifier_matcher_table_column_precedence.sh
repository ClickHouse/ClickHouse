#!/usr/bin/env bash
# Under `analyzer_alias_hides_table_name`, the probe of an enclosing query tries the readings of one table expression
# in the order of the lookup, and the first one that binds the qualifier decides: the whole identifier as a column
# first, and the identifier without the alias of the table expression afterwards, or the opposite with
# `analyzer_compatibility_prefer_alias_over_subcolumn`. A scalar column that the lookup binds keeps the name of the
# inner table visible, even if the other reading is a `Tuple` that the matcher could expand.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --enable_analyzer 1 --analyzer_alias_hides_table_name 1 --multiquery "
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS scalar_first;
DROP TABLE IF EXISTS tuple_first;

CREATE TABLE t (val UInt64) ENGINE = Memory;
INSERT INTO t VALUES (10), (20), (30);

CREATE TABLE scalar_first (\`${CLICKHOUSE_DATABASE}.t\` UInt64, t Tuple(val UInt64)) ENGINE = Memory;
INSERT INTO scalar_first VALUES (5, (7));

CREATE TABLE tuple_first (\`${CLICKHOUSE_DATABASE}.t\` Tuple(val UInt64), t UInt64) ENGINE = Memory;
INSERT INTO tuple_first VALUES ((7), 5);

SELECT 'the whole identifier is a scalar column, the rest is a tuple';
SELECT (SELECT \`${CLICKHOUSE_DATABASE}\`.t.* FROM t AS c ORDER BY val LIMIT 1) FROM scalar_first AS \`${CLICKHOUSE_DATABASE}\`;
SELECT (SELECT \`${CLICKHOUSE_DATABASE}\`.t.* FROM t AS c ORDER BY val LIMIT 1) FROM scalar_first AS \`${CLICKHOUSE_DATABASE}\`
SETTINGS analyzer_alias_hides_table_name = 0;
SELECT (SELECT \`${CLICKHOUSE_DATABASE}\`.t.* FROM t AS c ORDER BY val LIMIT 1) FROM scalar_first AS \`${CLICKHOUSE_DATABASE}\`
SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;

SELECT 'the whole identifier is a tuple column, the rest is a scalar';
SELECT (SELECT \`${CLICKHOUSE_DATABASE}\`.t.* FROM t AS c ORDER BY val LIMIT 1) FROM tuple_first AS \`${CLICKHOUSE_DATABASE}\`;
SELECT (SELECT \`${CLICKHOUSE_DATABASE}\`.t.* FROM t AS c ORDER BY val LIMIT 1) FROM tuple_first AS \`${CLICKHOUSE_DATABASE}\`
SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1;
SELECT (SELECT \`${CLICKHOUSE_DATABASE}\`.t.* FROM t AS c ORDER BY val LIMIT 1) FROM tuple_first AS \`${CLICKHOUSE_DATABASE}\`
SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1, analyzer_alias_hides_table_name = 0;

DROP TABLE t;
DROP TABLE scalar_first;
DROP TABLE tuple_first;
"
