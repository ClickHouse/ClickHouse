#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test uses DETACH/ATTACH itself
# no-replicated-database: like the rest of the reattach suite

# A nested `SETTINGS` clause is applied one change at a time, each clamped to the constraints the preceding
# changes left: after `readonly = 1`, the change of `enable_global_with_statement` is dropped, so the nested
# name keeps resolving to the outer CTE and the physical table of the same name is never read.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CUR_DIR"/02461_reattach_tables.lib

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_ro_src"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_ro_src (x UInt8) ENGINE = MergeTree ORDER BY tuple()"
${CLICKHOUSE_CLIENT} -q "INSERT INTO t_reattach_ro_src VALUES (2)"

# The nested name reads the CTE, which returns 1, not the physical table, which has 2.
${CLICKHOUSE_CLIENT} -q "WITH t_reattach_ro_src AS (SELECT 1 AS x) SELECT * FROM (SELECT * FROM t_reattach_ro_src SETTINGS readonly = 1, enable_global_with_statement = 0)"
check_if_not_detached "WITH t_reattach_ro_src AS (SELECT 1 AS x) SELECT * FROM (SELECT * FROM t_reattach_ro_src SETTINGS readonly = 1, enable_global_with_statement = 0)" "t_reattach_ro_src"

# The controls: without `readonly`, or with it applied after the scope setting, the nested name reads the physical table.
${CLICKHOUSE_CLIENT} -q "WITH t_reattach_ro_src AS (SELECT 1 AS x) SELECT * FROM (SELECT * FROM t_reattach_ro_src SETTINGS enable_global_with_statement = 0, readonly = 1)"
check_if_detached "WITH t_reattach_ro_src AS (SELECT 1 AS x) SELECT * FROM (SELECT * FROM t_reattach_ro_src SETTINGS enable_global_with_statement = 0)" "t_reattach_ro_src"
check_if_detached "WITH t_reattach_ro_src AS (SELECT 1 AS x) SELECT * FROM (SELECT * FROM t_reattach_ro_src SETTINGS enable_global_with_statement = 0, readonly = 1)" "t_reattach_ro_src"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_ro_src"
