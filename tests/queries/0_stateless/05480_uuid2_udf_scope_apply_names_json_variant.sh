#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# SQL user-defined functions are server-global rather than per-database, so their names embed the test database.
f_scope="${CLICKHOUSE_DATABASE}_05480_scope"
f_name="${CLICKHOUSE_DATABASE}_05480_name"

$CLICKHOUSE_CLIENT "
DROP FUNCTION IF EXISTS $f_scope;
DROP FUNCTION IF EXISTS $f_name;
CREATE FUNCTION $f_scope AS x -> x + 1;
CREATE FUNCTION $f_name AS x -> x + 1;

SELECT '-- A lambda declared in the query takes precedence over a SQL UDF of the same name in APPLY';
WITH (x -> x * 3) AS $f_scope SELECT * APPLY $f_scope FROM values('a UInt8', 1);
WITH (x -> x * 3) AS $f_scope SELECT * APPLY(x -> $f_scope(x)) FROM values('a UInt8', 1);

DROP TABLE IF EXISTS t_apply;
CREATE TABLE t_apply ENGINE = Memory AS
WITH (x -> x * 3) AS $f_scope SELECT * APPLY $f_scope FROM values('a UInt8', 1);
SELECT * FROM t_apply;
DROP TABLE t_apply;

CREATE TABLE t_apply ENGINE = Memory AS
WITH (x -> x * 3) AS $f_scope SELECT * APPLY(x -> $f_scope(x)) FROM values('a UInt8', 1);
SELECT * FROM t_apply;
DROP TABLE t_apply;

SELECT '-- Without a lambda of the same name, the UDF applies';
CREATE TABLE t_apply ENGINE = Memory AS SELECT * APPLY $f_scope FROM values('a UInt8', 1);
SELECT * FROM t_apply;
DROP TABLE t_apply;

SELECT '-- A callable formal parameter of a lambda takes precedence over a SQL UDF of the same name in APPLY';
SELECT * APPLY(x -> (
    WITH ($f_scope -> $f_scope(1)) AS invoke,
         (x -> x * 3) AS triple
    SELECT invoke(triple)))
FROM values('a UInt8', 1);

CREATE TABLE t_apply ENGINE = Memory AS
SELECT * APPLY(x -> (
    WITH ($f_scope -> $f_scope(1)) AS invoke,
         (x -> x * 3) AS triple
    SELECT invoke(triple)))
FROM values('a UInt8', 1);
SELECT * FROM t_apply;
DROP TABLE t_apply;

SELECT '-- A persisted named APPLY of a UDF keeps the names of the projections';
SELECT \`$f_name(a)\` FROM (SELECT * APPLY $f_name FROM values('a UInt8', 1));
CREATE TABLE t_apply ENGINE = Memory AS SELECT \`$f_name(a)\` FROM (SELECT * APPLY $f_name FROM values('a UInt8', 1));
SELECT * FROM t_apply;
DROP TABLE t_apply;
CREATE TABLE t_apply ENGINE = Memory AS SELECT * APPLY $f_name FROM values('a UInt8', 1);
SELECT name = '$f_name(a)' FROM system.columns WHERE database = currentDatabase() AND table = 't_apply';
DROP TABLE t_apply;

DROP FUNCTION $f_scope;
DROP FUNCTION $f_name;
"

$CLICKHOUSE_CLIENT "
SELECT '-- A typed JSON default value of an aggregate function keeps the identifier';
SELECT toJSONString(groupArrayInsertAt(
    CAST('{\"u\":\"00000000-0000-0000-0000-000000000001\"}' AS JSON(u UUID2)), 2
)(CAST('{\"u\":\"00000000-0000-0000-0000-000000000001\"}' AS JSON(u UUID2)), toUInt64(1)));

SELECT '-- A typed JSON value carried by a nested Variant keeps the identifier';
SET allow_suspicious_variant_types = 1;
SELECT j FROM values('j Array(JSON(u UUID1))',
    [CAST(CAST('{\"u\":\"00000000-0000-0000-0000-000000000001\"}' AS JSON(u UUID2)) AS Variant(JSON(u UUID2), String))]);
SELECT s FROM values('s String',
    [CAST(CAST('{\"u\":\"00000000-0000-0000-0000-000000000001\"}' AS JSON(u UUID2)) AS Variant(JSON(u UUID2), String))]);
"
