#!/usr/bin/env bash
# Tags: no-fasttest, use-rocksdb
# Tag no-fasttest: In fasttest, ENABLE_LIBRARIES=0, so the rocksdb engine is not enabled by default

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 1. A key-value point lookup by a tuple constant in a `Variant` does not lose the layout of its `UUID2` element.
# 2. A bloom filter probe of `hasAny` with an array constant in a `Dynamic` does not skip the matching granule.
# 3. `transform` and simple `CASE` keep a mapping from a `Dynamic` entry holding a `UUID2` value.
# 4. A lambda shadowing a function with a type name argument receives its ordinary string argument as is.
# 5. A SQL UDF in the parameters of an aggregate function of an `APPLY` transformer is materialized as well.

# SQL user-defined functions are server-global rather than per-database, so the name has to embed the test database.
function_name="${CLICKHOUSE_DATABASE}_05454_f"

$CLICKHOUSE_CLIENT "
DROP TABLE IF EXISTS t_kv;
DROP TABLE IF EXISTS t_bf;
DROP TABLE IF EXISTS t_lambda;
DROP TABLE IF EXISTS t_apply;
DROP FUNCTION IF EXISTS $function_name;

SELECT '-- kv';
CREATE TABLE t_kv (k UUID1, n UInt8, v UInt8) ENGINE = EmbeddedRocksDB PRIMARY KEY (k, n);
INSERT INTO t_kv VALUES ('00000000-0000-0000-0000-000000000001', 1, 7);
SELECT * FROM t_kv
WHERE (k, n) = CAST(tuple(toUUID2('00000000-0000-0000-0000-000000000001'), toUInt8(1)) AS Variant(Tuple(UUID2, UInt8), String));

SELECT '-- bloom filter';
CREATE TABLE t_bf (u Array(UUID2), INDEX bf u TYPE bloom_filter(0.001) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_bf SELECT [toUUID2('00000000-0000-0000-0000-000000000001')];
SELECT count() FROM t_bf WHERE hasAny(u, CAST([toUUID('00000000-0000-0000-0000-000000000001')] AS Dynamic)) SETTINGS use_skip_indexes = 1;
SELECT count() FROM t_bf WHERE hasAll(u, CAST([toUUID('00000000-0000-0000-0000-000000000001')] AS Variant(Array(UUID), String))) SETTINGS use_skip_indexes = 1;

SELECT '-- transform';
WITH '00000000-0000-0000-0000-000000000001' AS s
SELECT
    transform(toUUID2(s), [CAST(toUUID2(s) AS Dynamic)], [7], 0),
    transform(toUUID(s), [CAST(toUUID2(s) AS Dynamic)], [7], 0),
    transform(toUUID2(s), [CAST(toUUID(s) AS Dynamic)], [7], 0),
    CASE toUUID2(s) WHEN CAST(toUUID2(s) AS Dynamic) THEN 7 ELSE 0 END;

SELECT '-- lambda';
SET uuid_type_version = 2;
CREATE TABLE t_lambda ENGINE = Memory AS
WITH (x -> x) AS defaultValueOfTypeName
SELECT defaultValueOfTypeName('UUID') AS s, CAST('00000000-0000-0000-0000-000000000001', 'UUID') AS u;
SELECT s, toTypeName(u) FROM t_lambda;

SELECT '-- apply parameters';
SET uuid_type_version = 1;
CREATE FUNCTION $function_name AS () -> if(
    CAST('00000000-0000-0001-0000-000000000000', 'UUID') < CAST('00000000-0000-0000-0000-000000000001', 'UUID'), 0., 1.);
SET uuid_type_version = 2;
CREATE TABLE t_apply ENGINE = Memory AS SELECT * APPLY(quantileExact($function_name())) FROM values('x Float64', 1, 2);
SELECT * FROM t_apply;

DROP TABLE t_kv;
DROP TABLE t_bf;
DROP TABLE t_lambda;
DROP TABLE t_apply;
DROP FUNCTION $function_name;
"
