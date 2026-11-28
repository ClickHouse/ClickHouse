#!/usr/bin/env bash
# Under `analyzer_alias_hides_table_name`, the probe of an enclosing query binds an `ARRAY JOIN` output as the lookup
# does: an output column can be an equated key of an enclosing `INNER JOIN`, and the first output that binds the
# identifier decides, even if a later output could be expanded by the matcher.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT --enable_analyzer 1 --analyzer_alias_hides_table_name 1 --multiquery "
DROP TABLE IF EXISTS t;

CREATE TABLE t (val UInt64) ENGINE = Memory;
INSERT INTO t VALUES (10), (20), (30);

SELECT 'ARRAY JOIN output equated with the other side of an INNER JOIN';
WITH 1 AS t
SELECT a.t, b.t, (SELECT t.* FROM t AS c ORDER BY val LIMIT 1)
FROM (SELECT [tuple(7)] AS t) AS a ARRAY JOIN t
INNER JOIN (SELECT tuple(7) AS t) AS b ON a.t = b.t
SETTINGS prefer_column_name_to_alias = 1, single_join_prefer_left_table = 0, analyzer_compatibility_prefer_alias_over_subcolumn = 1;

SELECT 'control: renamed';
WITH 1 AS x
SELECT a.x, b.x, (SELECT x.* FROM t AS c ORDER BY val LIMIT 1)
FROM (SELECT [tuple(7)] AS x) AS a ARRAY JOIN x
INNER JOIN (SELECT tuple(7) AS x) AS b ON a.x = b.x
SETTINGS prefer_column_name_to_alias = 1, single_join_prefer_left_table = 0, analyzer_compatibility_prefer_alias_over_subcolumn = 1;

SELECT 'the first ARRAY JOIN output that binds the qualifier decides';
SELECT (SELECT \`${CLICKHOUSE_DATABASE}\`.t.* FROM t AS c ORDER BY val LIMIT 1)
FROM numbers(1)
ARRAY JOIN [CAST(tuple(1), 'Tuple(t UInt64)')] AS \`${CLICKHOUSE_DATABASE}\`, [tuple(7)] AS \`${CLICKHOUSE_DATABASE}.t\`;

SELECT 'control: not hidden';
SELECT (SELECT \`${CLICKHOUSE_DATABASE}\`.t.* FROM t AS c ORDER BY val LIMIT 1)
FROM numbers(1)
ARRAY JOIN [CAST(tuple(1), 'Tuple(t UInt64)')] AS \`${CLICKHOUSE_DATABASE}\`, [tuple(7)] AS \`${CLICKHOUSE_DATABASE}.t\`
SETTINGS analyzer_alias_hides_table_name = 0;

DROP TABLE t;
"
