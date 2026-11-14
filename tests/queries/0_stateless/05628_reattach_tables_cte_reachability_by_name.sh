#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database
# no-random-detach: test uses DETACH/ATTACH itself
# no-replicated-database: like the rest of the reattach suite

# A CTE is referenced only by the full name of an identifier, so neither a function call with its name nor a
# compound identifier whose first part is its name makes an unused definition reachable. A scalar binding is
# referenced by both, which the controls check.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CUR_DIR"/02461_reattach_tables.lib

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_cr_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_cr_lhs"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_cr_src (a UInt64) ENGINE = MergeTree ORDER BY a"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_cr_lhs (a UInt64) ENGINE = MergeTree ORDER BY a"

# The builtin `count` does not reference the CTE named `count`.
check_if_not_detached "WITH count AS (SELECT * FROM t_reattach_cr_src) SELECT count()" "t_reattach_cr_src"
# `unused.a` is a column of the table aliased as `unused`, not of the CTE.
check_if_not_detached "WITH unused AS (SELECT * FROM t_reattach_cr_src) SELECT unused.a FROM t_reattach_cr_lhs AS unused" "t_reattach_cr_src"
# The control: the CTE is used.
check_if_detached "WITH count AS (SELECT * FROM t_reattach_cr_src) SELECT * FROM count" "t_reattach_cr_src"

# Scalar bindings are referenced by a function call and by the first part of a compound identifier.
check_if_detached "WITH a -> a + (SELECT count() FROM t_reattach_cr_src) AS f SELECT f(a) FROM t_reattach_cr_lhs" "t_reattach_cr_src"
check_if_detached "WITH (SELECT CAST(tuple(count()), 'Tuple(n UInt64)') FROM t_reattach_cr_src) AS stats SELECT stats.n FROM t_reattach_cr_lhs" "t_reattach_cr_src"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_cr_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_cr_lhs"
