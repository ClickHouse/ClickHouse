#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A lambda or SQL UDF call with a GROUP BY key as argument and an aggregate function in its body, with
# `group_by_use_nulls`. Each lambda query is followed by its inline twin where the result is not obvious.

$CLICKHOUSE_CLIENT -q "
DROP FUNCTION IF EXISTS ${CLICKHOUSE_DATABASE}_sum;
DROP FUNCTION IF EXISTS ${CLICKHOUSE_DATABASE}_grouping;
DROP FUNCTION IF EXISTS ${CLICKHOUSE_DATABASE}_identity;
CREATE FUNCTION ${CLICKHOUSE_DATABASE}_sum AS x -> sum(x);
CREATE FUNCTION ${CLICKHOUSE_DATABASE}_grouping AS x -> grouping(x);
CREATE FUNCTION ${CLICKHOUSE_DATABASE}_identity AS x -> x;

SELECT 'issue';
WITH (x -> sum(x)) AS f
SELECT f(number) FROM numbers(3)
GROUP BY number WITH ROLLUP
ORDER BY ALL
SETTINGS group_by_use_nulls = 1;

SET group_by_use_nulls = 1;

SELECT sum(number) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
WITH (x -> sum(x)) AS f SELECT f(number) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;

SELECT 'cube, grouping sets';
WITH (x -> sum(x)) AS f SELECT f(number) FROM numbers(3) GROUP BY number WITH CUBE ORDER BY ALL;
WITH (x -> sum(x)) AS f SELECT f(number) FROM numbers(3) GROUP BY GROUPING SETS ((number), ()) ORDER BY ALL;

SELECT 'grouping';
WITH (x -> grouping(x)) AS f SELECT number, f(number) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT 'compound argument, mixed body';
WITH (x -> (x, sum(x))) AS f SELECT f(number + 1) AS t, toTypeName(t) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
WITH (x -> (sum(x), x)) AS f SELECT f(number + 1) AS t, toTypeName(t) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT (number + 1, sum(number + 1)) AS t, toTypeName(t) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT 'no aggregate over the argument';
WITH (x -> x + 1) AS f SELECT f(number) AS t, toTypeName(t) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
WITH (x -> sum(number) + x) AS f SELECT f(number + 1) AS t, toTypeName(t) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;

SELECT 'argument form inside an aggregate that is not used';
WITH (x -> x) AS f SELECT f(toUInt64(if(isNullable(number), '1', 'invalid'))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT ${CLICKHOUSE_DATABASE}_identity(toUInt64(if(isNullable(number), '1', 'invalid'))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
WITH (x -> sum(number) + x) AS f SELECT f(toUInt64(if(isNullable(number), '1', 'invalid'))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
WITH ((x, y) -> sum(x) + y) AS f SELECT f(number, toUInt64(if(isNullable(number), '1', 'invalid'))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT 'nested lambda';
WITH (y -> sum(y)) AS g, (x -> g(x)) AS f SELECT f(number) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;

SELECT 'having';
WITH (x -> sum(x)) AS f SELECT number, f(number) FROM numbers(3) GROUP BY number WITH ROLLUP HAVING f(number) > 0 ORDER BY f(number), number;

SELECT 'sql udf';
SELECT ${CLICKHOUSE_DATABASE}_sum(number) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT number, ${CLICKHOUSE_DATABASE}_grouping(number) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT 'apply';
WITH (x -> sum(x)) AS f SELECT * APPLY f FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
WITH (x -> sum(x)) AS f SELECT * APPLY x -> f(x) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT * APPLY ${CLICKHOUSE_DATABASE}_sum FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
WITH (x -> sum(x)) AS f SELECT f(*) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
WITH ((x, y, z) -> (sum(x), y, sum(z))) AS f SELECT f(*, a) AS t, toTypeName(t) FROM (SELECT number AS a, number + 10 AS b FROM numbers(3)) GROUP BY a, b WITH ROLLUP ORDER BY ALL;
SELECT (sum(a), b, sum(a)) AS t, toTypeName(t) FROM (SELECT number AS a, number + 10 AS b FROM numbers(3)) GROUP BY a, b WITH ROLLUP ORDER BY ALL;
WITH (x -> x + 1) AS f SELECT * APPLY f FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT * APPLY x -> x + 1 FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
WITH (x -> x + 1) AS f SELECT * APPLY sum, * APPLY f FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT * APPLY sum, * APPLY x -> x + 1 FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT 'lambda named like an aggregate function';
WITH (x -> (x, avg(x))) AS sum SELECT sum(number) AS t, toTypeName(t) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT (number, avg(number)) AS t, toTypeName(t) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT 'replace in a subquery of the argument';
WITH (x -> sum(x)) AS f SELECT f(number + (SELECT count() FROM (SELECT * REPLACE (c + 1 AS c) FROM (SELECT number AS c FROM numbers(3)) WHERE c > 1))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;
SELECT sum(number + (SELECT count() FROM (SELECT * REPLACE (c + 1 AS c) FROM (SELECT number AS c FROM numbers(3)) WHERE c > 1))) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY 1;

SELECT 'subquery in the body';
SELECT t, toTypeName(t) FROM (WITH (x -> (sum(x), (SELECT sum(x)))) AS f SELECT f(number) AS t FROM numbers(3) GROUP BY number WITH ROLLUP) ORDER BY ALL;
SELECT t, toTypeName(t) FROM (SELECT (sum(number), (SELECT sum(number))) AS t FROM numbers(3) GROUP BY number WITH ROLLUP) ORDER BY ALL;
SELECT t, toTypeName(t) FROM (WITH (x -> ((SELECT sum(x)), sum(x))) AS f SELECT f(number) AS t FROM numbers(3) GROUP BY number WITH ROLLUP) ORDER BY ALL;
SELECT t, toTypeName(t) FROM (SELECT ((SELECT sum(number)), sum(number)) AS t FROM numbers(3) GROUP BY number WITH ROLLUP) ORDER BY ALL;

SELECT 'alias';
WITH (x -> sum(x)) AS f SELECT f(number) AS s, s + 1 FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT sum(number) AS s, s + 1 FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

SELECT 'rollup subquery';
WITH (x -> sum(x)) AS f SELECT f(number), (SELECT count() FROM (SELECT number FROM numbers(2) GROUP BY number WITH ROLLUP)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;
SELECT sum(number), (SELECT count() FROM (SELECT number FROM numbers(2) GROUP BY number WITH ROLLUP)) FROM numbers(3) GROUP BY number WITH ROLLUP ORDER BY ALL;

DROP FUNCTION ${CLICKHOUSE_DATABASE}_sum;
DROP FUNCTION ${CLICKHOUSE_DATABASE}_grouping;
DROP FUNCTION ${CLICKHOUSE_DATABASE}_identity;
"
