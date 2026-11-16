#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SQL UDFs are global: the names carry the test database, which is stripped from the output.
f="${CLICKHOUSE_DATABASE}_untuple_value"
g="${CLICKHOUSE_DATABASE}_untuple_value_nested"
fr="${CLICKHOUSE_DATABASE}_format_row"
um="${CLICKHOUSE_DATABASE}_untuple_map"
m="${CLICKHOUSE_DATABASE}_map"

$CLICKHOUSE_CLIENT -q "
CREATE FUNCTION $f AS (x) -> untuple(tuple(x));
CREATE FUNCTION $g AS (y) -> $f(y);
CREATE FUNCTION $fr AS (x) -> formatRowNoNewline('JSONEachRow', x);
CREATE FUNCTION $um AS (fn, xs) -> untuple(tuple(arrayMap(fn, xs)));
CREATE FUNCTION $m AS (fn, xs) -> arrayMap(fn, xs);

SELECT '-- https://github.com/ClickHouse/ClickHouse/issues/131545';
SELECT * FROM (SELECT $f(11), $f(22)) FORMAT TSVWithNames;
SELECT '-- the inner query alone';
SELECT $f(11), $f(22) FORMAT TSVWithNames;
SELECT '-- column arguments';
SELECT * FROM (SELECT $f(number), $f(number + 1) FROM numbers(2)) ORDER BY 1 FORMAT TSVWithNames;
SELECT '-- nested function';
SELECT * FROM (SELECT $g(11), $g(22)) FORMAT TSVWithNames;
SELECT '-- lambda arguments';
SELECT * FROM (SELECT $um(x -> x + 1, [1]), $um(x -> x + 2, [1])) FORMAT TSVWithNames;
SELECT * FROM (SELECT $m(x -> x + number, [1]), $m(x -> x * number, [1]) FROM numbers(2)) ORDER BY 1 FORMAT TSVWithNames;
SELECT '-- a WITH lambda keeps its name: stored view definitions keep WITH lambdas';
SELECT * FROM (WITH x -> untuple(tuple(x)) AS l SELECT l(11)) FORMAT TSVWithNames;
SELECT '-- an alias still names the columns';
SELECT * FROM (SELECT $f(11) AS a, $f(22) AS b) FORMAT TSVWithNames;
SELECT '-- view';
CREATE VIEW v AS SELECT $f(11), $f(22);
SELECT * FROM v FORMAT TSVWithNames;
SELECT '-- remote()';
SELECT * FROM (SELECT $f(dummy + 11), $f(dummy + 22) FROM remote('127.0.0.1', system.one)) FORMAT TSVWithNames;
SELECT '-- names inside the body keep the parameter name';
WITH x -> tupleElement(tuple(x), 'x') AS t SELECT t(1) SETTINGS enable_named_columns_in_function_tuple = 1;
SELECT $fr(number) FROM numbers(1);

DROP VIEW v;
DROP FUNCTION $f;
DROP FUNCTION $g;
DROP FUNCTION $fr;
DROP FUNCTION $um;
DROP FUNCTION $m;
" | sed "s/${CLICKHOUSE_DATABASE}_//g"
