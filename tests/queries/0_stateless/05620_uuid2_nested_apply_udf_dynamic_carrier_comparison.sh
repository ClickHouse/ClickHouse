#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 1. An `APPLY` transformer in the body of a SQL UDF expands the UDFs in its expressions before `uuid_type_version = 2`
#    is materialized, both for the named form `APPLY f` and for `APPLY(y -> f(y))`, like the same `APPLY` in the query.
# 2. Aggregate functions that keep their values in a `Field` reject a `Variant` with a `Dynamic` alternative and
#    a `UUID2` alternative: `Dynamic` reads a value inserted from a `Field` as a `UUID`.
# 3. The comparison of `UUID2` values uses their order.

# SQL user-defined functions are server-global rather than per-database, so the names have to embed the test database.
inner="${CLICKHOUSE_DATABASE}_05620_inner"
outer_named="${CLICKHOUSE_DATABASE}_05620_outer_named"
outer_lambda="${CLICKHOUSE_DATABASE}_05620_outer_lambda"
outer_param="${CLICKHOUSE_DATABASE}_05620_outer_param"

$CLICKHOUSE_CLIENT "
DROP FUNCTION IF EXISTS $outer_named;
DROP FUNCTION IF EXISTS $outer_lambda;
DROP FUNCTION IF EXISTS $outer_param;
DROP FUNCTION IF EXISTS $inner;

SET uuid_type_version = 1;
CREATE FUNCTION $inner AS x -> if(
    CAST('00000000-0000-0001-0000-000000000000', 'UUID')
        < CAST('00000000-0000-0000-0000-000000000001', 'UUID'), 0, 1);
CREATE FUNCTION $outer_named AS x -> (SELECT * APPLY $inner FROM values('a UInt8', 1));
CREATE FUNCTION $outer_lambda AS x -> (SELECT * APPLY(y -> $inner(y)) FROM values('a UInt8', 1));
-- The parameter of the outer UDF is substituted in the lambda, while the synthesized argument does not collide with it.
CREATE FUNCTION $outer_param AS x -> (SELECT * APPLY(y -> $inner(y) + x) FROM values('a UInt8', 1));

SET uuid_type_version = 2;
SELECT $inner(1), $outer_named(1), $outer_lambda(1), $outer_param(10);

DROP TABLE IF EXISTS t_direct;
DROP TABLE IF EXISTS t_nested;
CREATE TABLE t_direct ENGINE = Memory AS SELECT $inner(1) AS u;
CREATE TABLE t_nested ENGINE = Memory AS SELECT $outer_named(1) AS u, $outer_lambda(1) AS v;
SELECT * FROM t_direct;
SELECT * FROM t_nested;
DROP TABLE t_direct;
DROP TABLE t_nested;

-- With version 1, the bodies keep the historical type.
SET uuid_type_version = 1;
SELECT $inner(1), $outer_named(1), $outer_lambda(1);

DROP FUNCTION $outer_named;
DROP FUNCTION $outer_lambda;
DROP FUNCTION $outer_param;
DROP FUNCTION $inner;
"

$CLICKHOUSE_CLIENT "
SET allow_suspicious_variant_types = 1;
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT any_respect_nulls(CAST([toUUID2(s)] AS Variant(Array(Dynamic), Array(UUID2)))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT groupArrayInsertAt(CAST([toUUID2(s)] AS Variant(Array(Dynamic), Array(UUID2))), toUInt64(0)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT sumMap([CAST([toUUID2(s)] AS Variant(Array(Dynamic), Array(UUID2)))], [1]); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT any_respect_nulls(CAST([toUUID2(s)] AS Variant(Array(JSON), Array(UUID2)))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The column-backed state of \`groupArraySorted\` keeps the alternative.
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT toJSONString(groupArraySorted(1)(CAST([toUUID2(s)] AS Variant(Array(Dynamic), Array(UUID2)))));
-- \`Dynamic\` with the historical \`UUID\` reads the value in the same layout.
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT toJSONString(any_respect_nulls(CAST([toUUID(s)] AS Variant(Array(Dynamic), Array(UUID1)))));
"

$CLICKHOUSE_CLIENT "
DROP TABLE IF EXISTS t_cmp;
CREATE TABLE t_cmp (u UUID1, u2 UUID2) ENGINE = Memory;
INSERT INTO t_cmp SELECT toUUID(s), toUUID2(s) FROM (SELECT arrayJoin([
    '00000000-0000-0000-0000-000000000001',
    '00000000-0000-0001-0000-000000000000',
    '00000000-0000-0000-0000-000000000002']) AS s);
SELECT
    countIf(u < toUUID('00000000-0000-0000-0000-000000000002')),
    countIf(u2 < toUUID2('00000000-0000-0000-0000-000000000002')),
    countIf(u2 = toUUID2('00000000-0000-0001-0000-000000000000')),
    countIf(u2 > u2), countIf(u2 <= u2)
FROM t_cmp;
SELECT u2 FROM t_cmp WHERE u2 > toUUID2('00000000-0000-0000-0000-000000000001') ORDER BY u2;
DROP TABLE t_cmp;
"
