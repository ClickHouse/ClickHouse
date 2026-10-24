-- Tags: no-fasttest, no-parallel, no-replicated-database
-- no-parallel -- uses a server-wide failpoint that would fail a metadata commit of a concurrently running test.
-- All tables are dropped with `SYNC`: the data of a table dropped asynchronously is removed in the background,
-- and that removal could consume the one-shot failpoint instead of the access check of the wrapper below.

-- A `cache` disk over a `borrow_from_cache` disk shares its object storage. If the startup of such a
-- wrapper fails (here: its access check), its shutdown must not release the objects of the tables
-- that are already stored on the wrapped disk.

DROP TABLE IF EXISTS cache_creator SYNC;
CREATE TABLE cache_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '05407_cache_creator',
    path = '05407_borrow_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

DROP TABLE IF EXISTS borrowed SYNC;
CREATE TABLE borrowed (k UInt64, v String)
ENGINE = MergeTree() ORDER BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '05407_cache_creator',
    name = '05407_borrowed_disk'
);

INSERT INTO borrowed VALUES (1, 'a'), (2, 'b'), (3, 'c');

SYSTEM ENABLE FAILPOINT disk_object_storage_fail_commit_metadata_transaction;
CREATE TABLE wrapped (k UInt64)
ENGINE = MergeTree() ORDER BY k
SETTINGS disk = disk(
    type = cache,
    disk = '05407_borrowed_disk',
    name = '05407_wrapper',
    path = '05407_wrapper_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT disk_object_storage_fail_commit_metadata_transaction;

SELECT 'after the failed wrapper';
SELECT * FROM borrowed ORDER BY k;

-- Moving a part onto the borrowed disk and off it again. The destination directory of a cloned part
-- has a trailing slash, which used to create an empty-named child directory in the in-memory metadata:
-- moving the part off the disk then recursed into the same directory endlessly.
DROP TABLE IF EXISTS moved SYNC;
CREATE TABLE moved (k UInt64, v String) ENGINE = MergeTree() ORDER BY k SETTINGS storage_policy = 'borrow_from_cache_move';
INSERT INTO moved VALUES (4, 'd'), (5, 'e');

ALTER TABLE moved MOVE PART 'all_1_1_0' TO DISK 'borrow_from_cache_disk';
SELECT 'moved onto the borrowed disk', disk_name FROM system.parts WHERE database = currentDatabase() AND table = 'moved' AND active;
SELECT * FROM moved ORDER BY k;

ALTER TABLE moved MOVE PART 'all_1_1_0' TO DISK 'local_disk';
SELECT 'moved off the borrowed disk', disk_name FROM system.parts WHERE database = currentDatabase() AND table = 'moved' AND active;
SELECT * FROM moved ORDER BY k;

DROP TABLE moved SYNC;
DROP TABLE borrowed SYNC;
DROP TABLE cache_creator SYNC;
