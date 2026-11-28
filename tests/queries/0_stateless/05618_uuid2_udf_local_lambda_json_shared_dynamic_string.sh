#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 1. A lambda declared in the body of a SQL UDF expanded inside `APPLY` takes precedence over a global UDF of the same
#    name, both in a direct query and in a table created with `CREATE TABLE ... AS SELECT`.
# 2. A `UUID2` value in the shared data of a `JSON` value keeps its identifier when it is extracted as a `Field`.
# 3. A textual UUID inside a `Dynamic` constant converted to `UUID2` keeps its identifier (in a partition operation).

# SQL user-defined functions are server-global rather than per-database, so the names have to embed the test database.
g="${CLICKHOUSE_DATABASE}_05618_g"
f="${CLICKHOUSE_DATABASE}_05618_f"

$CLICKHOUSE_CLIENT "
DROP FUNCTION IF EXISTS $f;
DROP FUNCTION IF EXISTS $g;
DROP TABLE IF EXISTS t_local_lambda;

SET uuid_type_version = 1;
CREATE FUNCTION $g AS y -> y + 1;
CREATE FUNCTION $f AS x -> (
    WITH (y -> y * 3) AS $g
    SELECT tuple($g(1), CAST('00000000-0000-0000-0000-000000000001', 'UUID')));
SET uuid_type_version = 2;

SELECT * APPLY $f FROM values('a UInt8', 1);
CREATE TABLE t_local_lambda ENGINE = Memory AS SELECT * APPLY $f FROM values('a UInt8', 1);
SELECT * FROM t_local_lambda;

DROP TABLE t_local_lambda;
DROP FUNCTION $f;
DROP FUNCTION $g;
"

$CLICKHOUSE_CLIENT "
SELECT toJSONString(j), dynamicType(j.u)
FROM format(RowBinary, 'j JSON(max_dynamic_paths=0)', unhex('0101753800000000000000000000000000000001'));

SELECT toJSONString(groupArrayInsertAt(j, toUInt64(0)))
FROM format(RowBinary, 'j JSON(max_dynamic_paths=0)', unhex('0101753800000000000000000000000000000001'));

SELECT toJSONString(groupArrayInsertAt(j::Dynamic, toUInt64(0)))
FROM format(RowBinary, 'j JSON(max_dynamic_paths=0)', unhex('0101753800000000000000000000000000000001'));
"

$CLICKHOUSE_CLIENT "
DROP TABLE IF EXISTS t_dynamic_string_partition;
CREATE TABLE t_dynamic_string_partition (u Array(UUID2)) ENGINE = MergeTree PARTITION BY u ORDER BY tuple();
INSERT INTO t_dynamic_string_partition VALUES (['00000000-0000-0000-0000-000000000001']), (['00000000-0000-0001-0000-000000000000']);
ALTER TABLE t_dynamic_string_partition DROP PARTITION tuple([CAST('00000000-0000-0000-0000-000000000001' AS Dynamic)]);
SELECT u FROM t_dynamic_string_partition;
DROP TABLE t_dynamic_string_partition;

CREATE TABLE t_dynamic_string_partition (u Array(UUID2)) ENGINE = MergeTree PARTITION BY u ORDER BY tuple();
INSERT INTO t_dynamic_string_partition VALUES (['00000000-0000-0000-0000-000000000001']), (['00000000-0000-0001-0000-000000000000']);
ALTER TABLE t_dynamic_string_partition DROP PARTITION tuple([CAST('00000000-0000-0000-0000-000000000001' AS Variant(String, UInt64))]);
SELECT u FROM t_dynamic_string_partition;
DROP TABLE t_dynamic_string_partition;
"
