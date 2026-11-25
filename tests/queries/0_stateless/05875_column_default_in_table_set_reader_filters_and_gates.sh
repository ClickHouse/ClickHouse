#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A default `p IN table` of a column missing in a part (or substituted for `NULL` by a conversion to a non-nullable
# type) reads `table` on behalf of the reading query. Two of its restrictions are checked here:
# - its `additional_table_filters` apply to `table`, as if the query read it directly;
# - the features that are enabled to resolve the stored default itself, whatever the settings of the reading query
#   (`enableAllExperimentalSettings`), are not enabled for a view that the default reads: here `allow_fuzz_query_functions`.

other_db="${CLICKHOUSE_DATABASE}_other"

$CLICKHOUSE_CLIENT -m -q "
    DROP DATABASE IF EXISTS ${other_db};
    CREATE DATABASE ${other_db};

    CREATE TABLE keys (p UInt8) ENGINE = MergeTree ORDER BY p;
    INSERT INTO keys VALUES (1), (2);

    SET allow_fuzz_query_functions = 1;
    CREATE VIEW rhs_view AS SELECT p FROM keys WHERE notEmpty(fuzzQuery('SELECT 1'));

    CREATE TABLE t (p UInt8) ENGINE = MergeTree ORDER BY p;
    INSERT INTO t VALUES (1), (2), (3);
    ALTER TABLE t ADD COLUMN in_keys UInt8 DEFAULT p IN keys;
    ALTER TABLE t ADD COLUMN in_view UInt8 DEFAULT p IN rhs_view;
    ALTER TABLE t ADD COLUMN gated UInt8 DEFAULT notEmpty(fuzzQuery('SELECT 1'));

    CREATE TABLE t_conversion (p UInt8, f Nullable(UInt8)) ENGINE = MergeTree ORDER BY p;
    INSERT INTO t_conversion VALUES (1, NULL);
    -- Keep the part with the nullable column: the mutation of the type is never executed.
    SYSTEM STOP MERGES t_conversion;
    ALTER TABLE t_conversion MODIFY COLUMN f UInt8 DEFAULT 0 SETTINGS alter_sync = 0;
    ALTER TABLE t_conversion MODIFY COLUMN f UInt8 DEFAULT toUInt8(2) IN keys SETTINGS alter_sync = 0;
"

echo 'no filters'
$CLICKHOUSE_CLIENT -q "SELECT sum(p IN keys), sum(in_keys) FROM t"
$CLICKHOUSE_CLIENT -q "SELECT f FROM t_conversion SETTINGS apply_mutations_on_fly = 0"

echo 'additional_table_filters'
$CLICKHOUSE_CLIENT -q "SELECT sum(p IN keys), sum(in_keys) FROM t SETTINGS additional_table_filters = {'keys': 'p = 1'}"
$CLICKHOUSE_CLIENT -q "SELECT sum(in_keys) FROM t SETTINGS additional_table_filters = {'${CLICKHOUSE_DATABASE}.keys': 'p = 1'}"
$CLICKHOUSE_CLIENT -q "SELECT toUInt8(2) IN keys, f FROM t_conversion SETTINGS apply_mutations_on_fly = 0, additional_table_filters = {'keys': 'p = 1'}"

echo 'additional_table_filters from another database'
# An unqualified name refers to a table of the current database of the reading query, not of the database of `t`.
$CLICKHOUSE_CLIENT -q "USE ${other_db}; SELECT sum(p IN ${CLICKHOUSE_DATABASE}.keys), sum(in_keys) FROM ${CLICKHOUSE_DATABASE}.t SETTINGS additional_table_filters = {'keys': 'p = 1'}"
$CLICKHOUSE_CLIENT -q "USE ${other_db}; SELECT sum(p IN ${CLICKHOUSE_DATABASE}.keys), sum(in_keys) FROM ${CLICKHOUSE_DATABASE}.t SETTINGS additional_table_filters = {'${CLICKHOUSE_DATABASE}.keys': 'p = 1'}"

echo 'allow_fuzz_query_functions = 1'
$CLICKHOUSE_CLIENT -q "SELECT sum(p IN rhs_view), sum(in_view), sum(gated) FROM t SETTINGS allow_fuzz_query_functions = 1"

echo 'allow_fuzz_query_functions = 0'
$CLICKHOUSE_CLIENT -q "SELECT sum(p IN rhs_view) FROM t SETTINGS allow_fuzz_query_functions = 0" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'
$CLICKHOUSE_CLIENT -q "SELECT sum(in_view) FROM t SETTINGS allow_fuzz_query_functions = 0" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'
$CLICKHOUSE_CLIENT --allow_fuzz_query_functions 0 --readonly 1 -q "SELECT sum(in_view) FROM t" 2>&1 | grep -o -m1 'SUPPORT_IS_DISABLED'
# The stored default itself is still evaluated, and so is a default that reads a plain table.
$CLICKHOUSE_CLIENT -q "SELECT sum(gated), sum(in_keys) FROM t SETTINGS allow_fuzz_query_functions = 0"

$CLICKHOUSE_CLIENT -m -q "
    DROP TABLE t_conversion;
    DROP TABLE t;
    DROP VIEW rhs_view;
    DROP TABLE keys;
    DROP DATABASE ${other_db};
"
