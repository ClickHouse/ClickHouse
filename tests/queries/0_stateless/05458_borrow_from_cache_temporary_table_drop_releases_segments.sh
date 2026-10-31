#!/usr/bin/env bash
# Tags: no-fasttest, no-replicated-database

# A temporary table of an engine that takes a disk directly keeps its files on that disk. When it is dropped,
# the files must be removed from that disk too, otherwise on a `borrow_from_cache` disk their segments stay
# borrowed from the cache until the server restarts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# Caches and disks are server-wide, so name them after the database of the test: concurrent runs of the test
# must not count the segments of each other.
cache_name="05458_cache_${CLICKHOUSE_DATABASE}"
disk_name="05458_borrowed_disk_${CLICKHOUSE_DATABASE}"

$CLICKHOUSE_CLIENT -q "
DROP TABLE IF EXISTS cache_creator;
DROP TABLE IF EXISTS borrowed;
DROP TABLE IF EXISTS baseline;

CREATE TABLE cache_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '${cache_name}',
    path = '${cache_name}/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

-- Registers the disk; the table itself stays empty.
CREATE TABLE borrowed (k UInt64)
ENGINE = MergeTree() ORDER BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '${cache_name}',
    name = '${disk_name}'
);

-- The borrowed \`MergeTree\` table keeps a few files of its own on the disk, so compare with this baseline.
CREATE TABLE baseline ENGINE = Memory AS SELECT count() AS c FROM system.filesystem_cache WHERE cache_name = '${cache_name}';

SELECT 'StripeLog';
CREATE TEMPORARY TABLE t (k UInt64) ENGINE = StripeLog SETTINGS disk = '${disk_name}';
INSERT INTO t VALUES (1), (2);
SELECT count() > (SELECT c FROM baseline) FROM system.filesystem_cache WHERE cache_name = '${cache_name}';
DROP TABLE t;
SELECT count() - (SELECT c FROM baseline) FROM system.filesystem_cache WHERE cache_name = '${cache_name}';

SELECT 'Set';
CREATE TEMPORARY TABLE t (k UInt64) ENGINE = Set SETTINGS disk = '${disk_name}';
INSERT INTO t VALUES (1), (2);
SELECT count() > (SELECT c FROM baseline) FROM system.filesystem_cache WHERE cache_name = '${cache_name}';
DROP TABLE t;
SELECT count() - (SELECT c FROM baseline) FROM system.filesystem_cache WHERE cache_name = '${cache_name}';

SELECT 'Join';
CREATE TEMPORARY TABLE t (k UInt64, v String) ENGINE = Join(ANY, LEFT, k) SETTINGS disk = '${disk_name}';
INSERT INTO t VALUES (1, 'a'), (2, 'b');
SELECT count() > (SELECT c FROM baseline) FROM system.filesystem_cache WHERE cache_name = '${cache_name}';
DROP TABLE t;
SELECT count() - (SELECT c FROM baseline) FROM system.filesystem_cache WHERE cache_name = '${cache_name}';

DROP TABLE baseline;
DROP TABLE borrowed;
DROP TABLE cache_creator;
"
