#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-replicated-database
# no-parallel: uses a server-wide failpoint that would fail a metadata commit of a concurrently running test.

# A `cache` disk over a `borrow_from_cache` disk writes its objects into the borrowed cache. When the
# metadata commit of such a write fails, the rollback must release the borrowed segments, otherwise they
# stay pinned in the cache with nothing referencing them. The objects of committed data must survive.
# All tables are dropped with `SYNC`, so that a background removal does not consume the one-shot failpoint.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS wrapped SYNC;
DROP TABLE IF EXISTS borrowed SYNC;
DROP TABLE IF EXISTS cache_creator SYNC;

CREATE TABLE cache_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '05453_cache_creator',
    path = '05453_borrow_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

CREATE TABLE borrowed (k UInt64, v String)
ENGINE = MergeTree() ORDER BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '05453_cache_creator',
    name = '05453_borrowed_disk'
);
SYSTEM STOP MERGES borrowed;
INSERT INTO borrowed VALUES (1, 'a'), (2, 'b'), (3, 'c');
"

function borrowed_segments()
{
    $CLICKHOUSE_CLIENT -q "SELECT count() FROM system.filesystem_cache WHERE cache_name = '05453_cache_creator'"
}

# The objects of the committed parts are removed by a removal queue, so wait until the cache returns
# to the previous number of segments. Leaked segments would stay forever.
function wait_for_segments()
{
    local expected=$1
    local actual
    for _ in {1..60}
    do
        actual=$(borrowed_segments)
        if [ "$actual" -eq "$expected" ]
        then
            echo "segments released"
            return
        fi
        sleep 0.5
    done
    echo "segments: expected $expected, got $actual"
}

wrapper_disk="disk(
    type = cache,
    disk = '05453_borrowed_disk',
    name = '05453_wrapper',
    path = '05453_wrapper_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
)"

# The startup of the wrapper fails on its access check.
before=$(borrowed_segments)
$CLICKHOUSE_CLIENT -q "
SYSTEM ENABLE FAILPOINT disk_object_storage_fail_commit_metadata_transaction;
CREATE TABLE wrapped (k UInt64) ENGINE = MergeTree() ORDER BY k SETTINGS disk = $wrapper_disk; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT disk_object_storage_fail_commit_metadata_transaction;
"
echo "after the failed wrapper startup"
wait_for_segments "$before"

# An insert through a running wrapper fails on the metadata commit of one of the files of the part.
$CLICKHOUSE_CLIENT -q "
CREATE TABLE wrapped (k UInt64) ENGINE = MergeTree() ORDER BY k SETTINGS disk = $wrapper_disk;
SYSTEM STOP MERGES wrapped;
INSERT INTO wrapped VALUES (10), (20);
"
before=$(borrowed_segments)
$CLICKHOUSE_CLIENT -q "
SYSTEM ENABLE FAILPOINT disk_object_storage_fail_commit_metadata_transaction;
INSERT INTO wrapped VALUES (30); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT disk_object_storage_fail_commit_metadata_transaction;
"
echo "after the failed insert"
wait_for_segments "$before"

$CLICKHOUSE_CLIENT -q "
SELECT * FROM borrowed ORDER BY k;
SELECT * FROM wrapped ORDER BY k;

DROP TABLE wrapped SYNC;
DROP TABLE borrowed SYNC;
DROP TABLE cache_creator SYNC;
"
