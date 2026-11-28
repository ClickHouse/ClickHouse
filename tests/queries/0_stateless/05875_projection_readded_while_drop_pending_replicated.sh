#!/usr/bin/env bash
# Tags: zookeeper, no-replicated-database
# Tag no-replicated-database: the table names its replica explicitly.

# With one `ALTER TABLE ... DROP PROJECTION p, ADD PROJECTION p (...)`, a replica applies the new metadata before
# the `DROP PROJECTION` mutation rewrites its parts; until then their projection part of the old `p` must not be read.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    DROP TABLE IF EXISTS t_replicated SYNC;
    CREATE TABLE t_replicated (id UInt64, k UInt64, a UInt64, PROJECTION p (SELECT id, k, a ORDER BY k))
    ENGINE = ReplicatedMergeTree('/clickhouse/tables/{database}/t_replicated', 'r1') ORDER BY id SETTINGS index_granularity = 16;
    INSERT INTO t_replicated SELECT number, number % 100, number % 2 FROM numbers(2000);
    SYSTEM STOP MERGES t_replicated;
    ALTER TABLE t_replicated DROP PROJECTION p, ADD PROJECTION p (SELECT id, k, a ORDER BY a) SETTINGS alter_sync = 0;
"

# `SYSTEM SYNC REPLICA` would wait for the mutation, which cannot run with merges stopped: wait for the metadata only.
$CLICKHOUSE_CLIENT -q "SYSTEM SYNC REPLICA t_replicated PULL"
for _ in {1..600}; do
    [[ $($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.replication_queue WHERE database = currentDatabase() AND table = 't_replicated' AND type = 'ALTER_METADATA'") == 0 ]] && break
    sleep 0.1
done

$CLICKHOUSE_CLIENT --optimize_use_projections 1 -q "
    SELECT count() FROM t_replicated WHERE a = 1 AND k = 5;
    -- The same for a read that does not apply patch parts.
    SELECT count() FROM t_replicated WHERE a = 1 AND k = 5 SETTINGS apply_patch_parts = 0;
    SELECT count() FROM t_replicated WHERE a = 1 AND k = 5 SETTINGS optimize_use_projections = 0;
    DROP TABLE t_replicated SYNC;
"
