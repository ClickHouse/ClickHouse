#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# no-replicated-database: `ATTACH TABLE ... AS REPLICATED` is supported only for `Atomic` and `Ordinary` databases.

# `ATTACH TABLE ... AS REPLICATED` of a `leader_election` table must be refused before it rewrites the stored
# definition: `ReplicatedMergeTree` does not support `leader_election`, and a persisted replicated definition
# with the setting could not be attached any more. The table must stay attachable as it was.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
    CREATE TABLE t_le_attach_as_replicated (x UInt64)
    ENGINE = MergeTree ORDER BY x
    SETTINGS
        disk = disk(
            name = '05459_le_${CLICKHOUSE_DATABASE}',
            type = s3_plain_rewritable,
            enable_hard_links = 0,
            endpoint = 'http://localhost:11111/test/05459_le_${CLICKHOUSE_DATABASE}/',
            access_key_id = clickhouse,
            secret_access_key = clickhouse),
        leader_election = true
"

$CLICKHOUSE_CLIENT -q "DETACH TABLE t_le_attach_as_replicated"
$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_le_attach_as_replicated AS REPLICATED" 2>&1 | grep -o 'it would have `leader_election = 1`' | head -n 1

$CLICKHOUSE_CLIENT -q "ATTACH TABLE t_le_attach_as_replicated"
$CLICKHOUSE_CLIENT -q "SELECT engine FROM system.tables WHERE database = currentDatabase() AND name = 't_le_attach_as_replicated'"
$CLICKHOUSE_CLIENT -q "DROP TABLE t_le_attach_as_replicated SYNC"
