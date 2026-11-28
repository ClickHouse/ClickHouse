#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database, no-shared-merge-tree
# `leader_election` is rejected on a read-only disk. The lease is written to the object storage
# directly, not through the disk, so a node with a read-only disk could take the lease and keep
# renewing it while every write to its table fails as permanently read-only, and the writable
# nodes would stay followers of that lease forever.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A per-database endpoint (see `04065_leader_election_basic.sh` for why the shared
# `s3_plain_rewritable` disk is not used here). Apart from `read_only`, the disk is one that
# `leader_election` accepts.
$CLICKHOUSE_CLIENT -q "
    CREATE TABLE test_leader_election_read_only_disk (x UInt64)
    ENGINE = MergeTree ORDER BY x
    SETTINGS
        disk = disk(
            name = '05843_le_${CLICKHOUSE_DATABASE}',
            type = s3_plain_rewritable,
            read_only = 1,
            enable_hard_links = 0,
            metadata_snapshot_enabled = 0,
            endpoint = 'http://localhost:11111/test/05843_le_${CLICKHOUSE_DATABASE}/',
            access_key_id = clickhouse,
            secret_access_key = clickhouse),
        leader_election = true
" 2>&1 | grep -o -m1 -F "is read-only"

$CLICKHOUSE_CLIENT -q "EXISTS TABLE test_leader_election_read_only_disk"
