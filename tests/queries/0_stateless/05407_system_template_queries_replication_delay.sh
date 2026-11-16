#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database, no-shared-merge-tree
# Tag no-shared-merge-tree: the template reads `system.replicas`.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
CREATE TABLE t_05407_delay (x UInt64) ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_05407_delay', 'r1') ORDER BY x;
INSERT INTO t_05407_delay VALUES (1);
SYSTEM SYNC REPLICA t_05407_delay;"

# The built-in query lists only lagging replicas, so its filter is replaced to show this caught-up one.
query=$($CLICKHOUSE_CLIENT --format TSVRaw -q "
SELECT replaceRegexpOne(query, 'WHERE [^\\n]*', 'WHERE database = currentDatabase() AND table = ''t_05407_delay''')
FROM system.template_queries WHERE is_builtin AND name = 'Replication delay'")

# The replica copied the only log entry, so it is zero entries behind.
$CLICKHOUSE_CLIENT -q "SELECT log_entries_behind FROM ($query)"

# Without a Keeper session the log indexes are unknown, so the count is NULL.
expired=${query/FROM system.replicas/FROM (SELECT * REPLACE (1 AS is_session_expired, 0 AS log_max_index, 0 AS log_pointer) FROM system.replicas)}
$CLICKHOUSE_CLIENT -q "SELECT log_entries_behind FROM ($expired)"

$CLICKHOUSE_CLIENT -q "DROP TABLE t_05407_delay"
