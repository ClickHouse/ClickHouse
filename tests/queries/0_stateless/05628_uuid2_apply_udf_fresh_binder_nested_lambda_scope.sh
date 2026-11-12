#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# With `uuid_type_version = 2`, `APPLY f` with a SQL UDF `f` is expanded to `APPLY(arg -> f(arg))`.
# 1. The synthesized argument of the lambda does not capture a free identifier of the body of `f`,
#    both in the query and in the body of another UDF.
# 2. The substitution of the arguments of a UDF respects the parameters of the lambdas nested in its body.
# The direct and the persisted results must agree.

# SQL user-defined functions are server-global rather than per-database, so the names have to embed the test database.
capture="${CLICKHOUSE_DATABASE}_05628_capture"
capture_outer="${CLICKHOUSE_DATABASE}_05628_capture_outer"
shadow="${CLICKHOUSE_DATABASE}_05628_shadow"

$CLICKHOUSE_CLIENT "
DROP FUNCTION IF EXISTS $capture_outer;
DROP FUNCTION IF EXISTS $capture;
DROP FUNCTION IF EXISTS $shadow;

SET uuid_type_version = 1;
CREATE FUNCTION $capture AS y -> tuple(y + x, CAST('00000000-0000-0000-0000-000000000001', 'UUID'));
CREATE FUNCTION $capture_outer AS z -> (WITH 10 AS x SELECT * APPLY $capture FROM values('a UInt8', z));
CREATE FUNCTION $shadow AS x -> (
    SELECT * APPLY(y -> tuple(arrayMap(x -> x + 1, [1]), CAST('00000000-0000-0000-0000-000000000001', 'UUID')))
    FROM values('a UInt8', 1));

SET uuid_type_version = 2;

DROP TABLE IF EXISTS t_capture;
DROP TABLE IF EXISTS t_capture_outer;
DROP TABLE IF EXISTS t_shadow;

WITH 10 AS x SELECT * APPLY $capture FROM values('a UInt8', 1);
CREATE TABLE t_capture ENGINE = Memory AS WITH 10 AS x SELECT * APPLY $capture FROM values('a UInt8', 1);
SELECT *, toTypeName(*) FROM t_capture;

SELECT $capture_outer(1);
CREATE TABLE t_capture_outer ENGINE = Memory AS SELECT $capture_outer(1) AS r;
SELECT *, toTypeName(*) FROM t_capture_outer;

SELECT $shadow(10);
CREATE TABLE t_shadow ENGINE = Memory AS SELECT $shadow(10) AS r;
SELECT *, toTypeName(*) FROM t_shadow;

DROP TABLE t_capture;
DROP TABLE t_capture_outer;
DROP TABLE t_shadow;
DROP FUNCTION $capture_outer;
DROP FUNCTION $capture;
DROP FUNCTION $shadow;
"
