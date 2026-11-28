#!/usr/bin/env bash

# A `merge` table whose source table has thousands of ALIAS columns (like `system.metric_log`) must
# be planned in time linear in their number.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

N=2500

alias_columns=""
plain_columns=""
for ((i = 1; i <= N; ++i))
do
    alias_columns="$alias_columns, c$i UInt64 ALIAS m['c$i']"
    plain_columns="$plain_columns, c$i UInt64"
done

# The columns are ALIAS in `t_alias` and ordinary in `t_plain`, so they are ordinary in the `Merge`
# table and `*` reads all of them from `t_alias` as ALIAS columns.
$CLICKHOUSE_CLIENT --max_query_size 10000000 --query "
    CREATE TABLE t_alias (k UInt64, m Map(String, UInt64)$alias_columns, d1 UInt64 ALIAS c1 + 1, d2 UInt64 ALIAS d1 * 2)
    ENGINE = MergeTree ORDER BY k"
# A Memory table, so reading it costs the same memory whatever part format the settings randomize.
$CLICKHOUSE_CLIENT --max_query_size 10000000 --query "
    CREATE TABLE t_plain (k UInt64$plain_columns, d1 UInt64, d2 UInt64) ENGINE = Memory"

$CLICKHOUSE_CLIENT --query "INSERT INTO t_alias (k, m) VALUES (1, {'c1': 10, 'c$N': 7})"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_plain (k, c1, c$N, d1, d2) VALUES (2, 20, 8, 21, 42)"

$CLICKHOUSE_CLIENT --query "SELECT k, c1, c2, c$N, d1, d2 FROM merge(currentDatabase(), '^t_') ORDER BY k"
$CLICKHOUSE_CLIENT --query_id "${CLICKHOUSE_TEST_UNIQUE_NAME}_wide" --query "SELECT count(), sum(sipHash64(*)) FROM merge(currentDatabase(), '^t_')"
$CLICKHOUSE_CLIENT --query "SYSTEM FLUSH LOGS query_log"
# Planning the aliases one by one needs about 510 MB here.
$CLICKHOUSE_CLIENT --query "SELECT memory_usage < 150000000 FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND query_id = '${CLICKHOUSE_TEST_UNIQUE_NAME}_wide'"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_alias"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_plain"

# An ALIAS column named like the table alias of `merge` must not capture the other aliases' references,
# and a child's column name must not be qualified by that alias.
$CLICKHOUSE_CLIENT --query "CREATE TABLE u_alias (x UInt64, __table1 Tuple(x UInt64) ALIAS tuple(100), y UInt64 ALIAS x + 2) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query "CREATE TABLE u_plain (x UInt64, __table1 Tuple(x UInt64), y UInt64) ENGINE = MergeTree ORDER BY x"
$CLICKHOUSE_CLIENT --query "INSERT INTO u_alias (x) VALUES (1)"
$CLICKHOUSE_CLIENT --query "INSERT INTO u_plain SELECT 2, tuple(200), 4"
$CLICKHOUSE_CLIENT --query "SELECT x, __table1.x, y FROM merge(currentDatabase(), '^u_') ORDER BY x"
$CLICKHOUSE_CLIENT --query "SELECT x, __table1.x, y FROM merge(currentDatabase(), '^u_') ORDER BY x SETTINGS analyzer_compatibility_prefer_alias_over_subcolumn = 1"
$CLICKHOUSE_CLIENT --query "DROP TABLE u_alias"
$CLICKHOUSE_CLIENT --query "DROP TABLE u_plain"
