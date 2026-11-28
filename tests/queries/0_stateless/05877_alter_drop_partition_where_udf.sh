#!/usr/bin/env bash

# A SQL UDF in the predicate of `DROP PARTITION WHERE` is expanded before the checks, so that it cannot hide
# a read of another table or an `arrayJoin`. The functions are global, so their names include the database.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

udf_in_subquery="udf_in_subquery_${CLICKHOUSE_DATABASE}"
udf_in_table="udf_in_table_${CLICKHOUSE_DATABASE}"
udf_scalar_subquery="udf_scalar_subquery_${CLICKHOUSE_DATABASE}"
udf_array_join="udf_array_join_${CLICKHOUSE_DATABASE}"
udf_allowed="udf_allowed_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -n -q "
    CREATE TABLE t_dpw_udf (p UInt32, x UInt32) ENGINE = MergeTree ORDER BY x PARTITION BY p;
    INSERT INTO t_dpw_udf VALUES (1, 1), (2, 2), (3, 3);
    CREATE TABLE t_dpw_udf_keys (k UInt32) ENGINE = Memory;
    INSERT INTO t_dpw_udf_keys VALUES (1);

    CREATE FUNCTION ${udf_in_subquery} AS (x) -> (x IN (SELECT k FROM t_dpw_udf_keys));
    CREATE FUNCTION ${udf_in_table} AS (x) -> (x IN t_dpw_udf_keys);
    CREATE FUNCTION ${udf_scalar_subquery} AS (x) -> (x = (SELECT max(k) FROM t_dpw_udf_keys));
    CREATE FUNCTION ${udf_array_join} AS (x) -> arrayJoin(if(x = 1, [1, 1], [0]));
    CREATE FUNCTION ${udf_allowed} AS (x) -> (x >= 2);
"

for udf in "$udf_in_subquery" "$udf_in_table" "$udf_scalar_subquery"; do
    $CLICKHOUSE_CLIENT -q "ALTER TABLE t_dpw_udf DROP PARTITION WHERE ${udf}(_partition_value.1)" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"
done
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dpw_udf DROP PARTITION WHERE ${udf_array_join}(_partition_value.1) = 1" 2>&1 | grep -o -m1 "BAD_ARGUMENTS"

# Nothing was dropped by the rejected queries.
$CLICKHOUSE_CLIENT -q "SELECT count() FROM t_dpw_udf"

# A UDF without table reads is allowed.
$CLICKHOUSE_CLIENT -q "ALTER TABLE t_dpw_udf DROP PARTITION WHERE ${udf_allowed}(_partition_value.1)"
$CLICKHOUSE_CLIENT -q "SELECT p, x FROM t_dpw_udf ORDER BY x"

$CLICKHOUSE_CLIENT -n -q "
    DROP FUNCTION ${udf_in_subquery};
    DROP FUNCTION ${udf_in_table};
    DROP FUNCTION ${udf_scalar_subquery};
    DROP FUNCTION ${udf_array_join};
    DROP FUNCTION ${udf_allowed};
    DROP TABLE t_dpw_udf;
    DROP TABLE t_dpw_udf_keys;
"
