#!/usr/bin/env bash
# Tags: no-random-detach, no-replicated-database, no-fasttest
# no-random-detach: test uses DETACH/ATTACH itself
# no-replicated-database: like the rest of the reattach suite
# no-fasttest: `predictXGBoost` is not built in the fast test

# `reattach_tables_before_query_execution` walks a scalar `WITH` binding used through a named-tuple field or
# called as a lambda, and skips a query calling a model function that reads a dictionary named in its arguments.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh
# shellcheck source=./02461_reattach_tables.lib
. "$CURDIR"/02461_reattach_tables.lib

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_sb_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS t_reattach_sb_dim"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_sb_src (x UInt64) ENGINE = MergeTree ORDER BY x"
${CLICKHOUSE_CLIENT} -q "CREATE TABLE t_reattach_sb_dim (y UInt64) ENGINE = MergeTree ORDER BY y"

# 1. A scalar binding used only through a field of the named tuple it returns is used, so its source is reattached.
check_if_detached "WITH (SELECT CAST(tuple(count()), 'Tuple(n UInt64)') FROM t_reattach_sb_dim) AS stats SELECT stats.n FROM t_reattach_sb_src" "t_reattach_sb_dim"

# 2. A lambda binding used only as a function call is used, so the source of the subquery in its body is reattached.
check_if_detached "WITH a -> a + (SELECT count() FROM t_reattach_sb_dim) AS f SELECT f(x) FROM t_reattach_sb_src" "t_reattach_sb_dim"

# 3. A scalar binding that is not used at all is still not walked.
check_if_not_detached "WITH (SELECT count() FROM t_reattach_sb_dim) AS unused SELECT x FROM t_reattach_sb_src" "t_reattach_sb_dim"

# 4. `predictXGBoost` resolves the dictionary named in its arguments only when it runs, so it skips the hook.
check_fails_kind_without_detach "SELECT predictXGBoost('${CLICKHOUSE_DATABASE}.no_such_model', x) FROM t_reattach_sb_src SETTINGS enable_xgboost = 1" "t_reattach_sb_src" "BAD_ARGUMENTS"

${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_sb_src"
${CLICKHOUSE_CLIENT} -q "DROP TABLE t_reattach_sb_dim"
