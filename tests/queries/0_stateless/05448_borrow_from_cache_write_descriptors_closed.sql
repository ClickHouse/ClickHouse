-- Tags: no-fasttest, no-replicated-database

-- The objects of a `borrow_from_cache` disk are `Ephemeral` file segments, which stay held for as
-- long as the object exists. Writing an object must not leave the file of the segment open for
-- writing until then, otherwise every stored object keeps a file descriptor.

DROP TABLE IF EXISTS cache_creator SYNC;
CREATE TABLE cache_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '05448_cache_creator',
    path = '05448_borrow_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

DROP TABLE IF EXISTS borrowed SYNC;
CREATE TABLE borrowed (k UInt64)
ENGINE = MergeTree() ORDER BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '05448_cache_creator',
    name = '05448_borrowed_disk'
);
SYSTEM STOP MERGES borrowed;

DROP TABLE IF EXISTS open_files_before;
CREATE TABLE open_files_before ENGINE = Memory AS SELECT value FROM system.metrics WHERE metric = 'OpenFileForWrite';

-- 300 parts of several files each.
INSERT INTO borrowed SELECT number FROM numbers(300)
SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0, max_insert_threads = 1;

SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'borrowed' AND active;
SELECT count(), sum(k) FROM borrowed;

-- Leaked descriptors would be thousands; leave a margin for files that concurrent queries write.
SELECT (SELECT value FROM system.metrics WHERE metric = 'OpenFileForWrite') - (SELECT value FROM open_files_before) < 1000;

DROP TABLE open_files_before;
DROP TABLE borrowed SYNC;
DROP TABLE cache_creator SYNC;
