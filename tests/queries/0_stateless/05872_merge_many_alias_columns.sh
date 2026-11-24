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
$CLICKHOUSE_CLIENT --max_query_size 10000000 --query "
    CREATE TABLE t_plain (k UInt64$plain_columns, d1 UInt64, d2 UInt64) ENGINE = MergeTree ORDER BY k"

$CLICKHOUSE_CLIENT --query "INSERT INTO t_alias (k, m) VALUES (1, {'c1': 10, 'c$N': 7})"
$CLICKHOUSE_CLIENT --query "INSERT INTO t_plain (k, c1, c$N, d1, d2) VALUES (2, 20, 8, 21, 42)"

$CLICKHOUSE_CLIENT --query "SELECT k, c1, c2, c$N, d1, d2 FROM merge(currentDatabase(), '^t_') ORDER BY k"
$CLICKHOUSE_CLIENT --query "SELECT count(), sum(sipHash64(*)) FROM merge(currentDatabase(), '^t_')"

$CLICKHOUSE_CLIENT --query "DROP TABLE t_alias"
$CLICKHOUSE_CLIENT --query "DROP TABLE t_plain"
