#!/usr/bin/env bash
# Tags: no-fasttest
# no-fasttest: the AI agent of the client is not compiled in the fast test build.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A table the read-only tool of the AI agent may read without confirmation. The first session reads
# it with the query result cache on, which is when the lookup of its engine could be cached.
$CLICKHOUSE_CLIENT -q "CREATE TABLE t (x String) ENGINE = Memory"
$CLICKHOUSE_CLIENT -q "INSERT INTO t VALUES ('abc')"
python3 "$CUR_DIR"/05877_client_ai_readonly_tool_probes_skip_query_cache.python table "SELECT x FROM t"

# The same name is a view now. The lookup of the second session is the same query as that of the
# first one, and it has to see the view rather than the cached `Memory` table.
$CLICKHOUSE_CLIENT -q "DROP TABLE t"
$CLICKHOUSE_CLIENT -q "CREATE VIEW t AS SELECT 'abc' AS x"
python3 "$CUR_DIR"/05877_client_ai_readonly_tool_probes_skip_query_cache.python view "SELECT length(x) FROM t"

$CLICKHOUSE_CLIENT -q "DROP VIEW t"
