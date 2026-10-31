#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# 1. With `uuid_type_version = 2`, a bare `UUID` introduced by a SQL UDF called from an `APPLY` transformer (both the
#    lambda form and the named form) is materialized as `UUID2`, as for an ordinary call of the UDF.
# 2. The first argument of the two-argument `loop(database, table)` is a scalar database name, so a scalar `format`
#    there is not treated as a table function and its arguments are not rewritten.
# 3. The `values` table function keeps a value whose type is the destination type as is, including the active
#    alternatives of nested `Variant` values.

# SQL user-defined functions are server-global rather than per-database, so the name has to embed the test database.
function_name="${CLICKHOUSE_DATABASE}_05453_to_uuid"

$CLICKHOUSE_CLIENT "
DROP TABLE IF EXISTS t_apply_src;
DROP TABLE IF EXISTS t_apply_lambda;
DROP TABLE IF EXISTS t_apply_named;
DROP TABLE IF EXISTS t_apply_mv;
DROP TABLE IF EXISTS t_apply_mv_target;
DROP TABLE IF EXISTS t_loop_scalar_database;
DROP FUNCTION IF EXISTS $function_name;

CREATE FUNCTION $function_name AS s -> CAST(s, 'UUID');

SET uuid_type_version = 2;

CREATE TABLE t_apply_src (s String) ENGINE = Memory;
INSERT INTO t_apply_src VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0');

SELECT 'APPLY lambda';
CREATE TABLE t_apply_lambda ENGINE = Memory AS SELECT * APPLY(x -> ${function_name}(x)) FROM t_apply_src;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_apply_lambda';
SELECT * FROM t_apply_lambda;

SELECT 'APPLY named';
CREATE TABLE t_apply_named ENGINE = Memory AS SELECT * APPLY ${function_name} FROM t_apply_src;
SELECT type FROM system.columns WHERE database = currentDatabase() AND table = 't_apply_named';
SELECT * FROM t_apply_named;

SELECT 'APPLY in ALTER MODIFY QUERY';
CREATE TABLE t_apply_mv_target (s String) ENGINE = Memory;
CREATE MATERIALIZED VIEW t_apply_mv TO t_apply_mv_target AS SELECT s FROM t_apply_src;
ALTER TABLE t_apply_mv MODIFY QUERY SELECT * APPLY(x -> ${function_name}(x)) FROM t_apply_src;
SELECT replaceAll(as_select, currentDatabase() || '.', '') FROM system.tables WHERE database = currentDatabase() AND name = 't_apply_mv';

SELECT 'loop with a scalar database name';
CREATE TABLE t_loop_scalar_database (k UInt8) AS loop(format('{}{}', 'id UUID', 'db'), 'source');
SELECT position(create_table_query, 'UUID2') = 0 FROM system.tables WHERE database = currentDatabase() AND name = 't_loop_scalar_database';

SELECT 'values with the destination type';
SET allow_suspicious_variant_types = 1;
SELECT a, variantType(a[1]) FROM values('a Array(Variant(UUID1, UUID2))',
    [CAST(toUUID2('00000000-0000-0000-0000-000000000001') AS Variant(UUID1, UUID2))]);
SELECT a, variantType(a[1]) FROM values('a Array(Variant(UUID1, UUID2))',
    [CAST(toUUID('00000000-0000-0000-0000-000000000001') AS Variant(UUID1, UUID2))]);

DROP TABLE t_apply_src;
DROP TABLE t_apply_lambda;
DROP TABLE t_apply_named;
DROP TABLE t_apply_mv;
DROP TABLE t_apply_mv_target;
DROP TABLE t_loop_scalar_database;
DROP FUNCTION $function_name;
"
