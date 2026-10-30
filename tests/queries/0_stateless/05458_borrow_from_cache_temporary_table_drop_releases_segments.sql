-- Tags: no-fasttest, no-replicated-database

-- A temporary table of an engine that takes a disk directly keeps its files on that disk. When it is dropped,
-- the files must be removed from that disk too, otherwise on a `borrow_from_cache` disk their segments stay
-- borrowed from the cache until the server restarts.

DROP TABLE IF EXISTS cache_creator;
DROP TABLE IF EXISTS borrowed;
DROP TABLE IF EXISTS baseline;

CREATE TABLE cache_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '05458_cache_creator',
    path = '05458_borrow_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

-- Registers the disk `05458_borrowed_disk`; the table itself stays empty.
CREATE TABLE borrowed (k UInt64)
ENGINE = MergeTree() ORDER BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '05458_cache_creator',
    name = '05458_borrowed_disk'
);

-- The borrowed `MergeTree` table keeps a few files of its own on the disk, so compare with this baseline.
CREATE TABLE baseline ENGINE = Memory AS SELECT count() AS c FROM system.filesystem_cache WHERE cache_name = '05458_cache_creator';

SELECT 'StripeLog';
CREATE TEMPORARY TABLE t (k UInt64) ENGINE = StripeLog SETTINGS disk = '05458_borrowed_disk';
INSERT INTO t VALUES (1), (2);
SELECT count() > (SELECT c FROM baseline) FROM system.filesystem_cache WHERE cache_name = '05458_cache_creator';
DROP TABLE t;
SELECT count() - (SELECT c FROM baseline) FROM system.filesystem_cache WHERE cache_name = '05458_cache_creator';

SELECT 'Set';
CREATE TEMPORARY TABLE t (k UInt64) ENGINE = Set SETTINGS disk = '05458_borrowed_disk';
INSERT INTO t VALUES (1), (2);
SELECT count() > (SELECT c FROM baseline) FROM system.filesystem_cache WHERE cache_name = '05458_cache_creator';
DROP TABLE t;
SELECT count() - (SELECT c FROM baseline) FROM system.filesystem_cache WHERE cache_name = '05458_cache_creator';

SELECT 'Join';
CREATE TEMPORARY TABLE t (k UInt64, v String) ENGINE = Join(ANY, LEFT, k) SETTINGS disk = '05458_borrowed_disk';
INSERT INTO t VALUES (1, 'a'), (2, 'b');
SELECT count() > (SELECT c FROM baseline) FROM system.filesystem_cache WHERE cache_name = '05458_cache_creator';
DROP TABLE t;
SELECT count() - (SELECT c FROM baseline) FROM system.filesystem_cache WHERE cache_name = '05458_cache_creator';

DROP TABLE baseline;
DROP TABLE borrowed;
DROP TABLE cache_creator;
