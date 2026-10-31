-- Tags: no-fasttest, no-parallel, no-replicated-database
-- no-parallel: uses a server-wide failpoint, which would hit the commits of cached disks of concurrently running tests.

-- A `cache` disk over a `borrow_from_cache` disk removes the objects of a failed transaction from the borrowed
-- disk as well. If the wrapper fails after the metadata is already committed, this must not be taken as
-- a failed commit, otherwise the objects referenced by the committed metadata would be removed.

DROP TABLE IF EXISTS wrapped SYNC;
DROP TABLE IF EXISTS borrowed SYNC;
DROP TABLE IF EXISTS cache_creator SYNC;

CREATE TABLE cache_creator (x UInt64)
ENGINE = MergeTree() ORDER BY x
SETTINGS disk = disk(
    type = cache,
    disk = 'local_disk',
    name = '05457_cache_creator',
    path = '05457_borrow_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

CREATE TABLE borrowed (k UInt64)
ENGINE = MergeTree() ORDER BY k
SETTINGS disk = disk(
    type = object_storage,
    object_storage_type = 'borrow_from_cache',
    metadata_type = 'memory',
    cache_name = '05457_cache_creator',
    name = '05457_borrowed_disk'
);

CREATE TABLE wrapped (k UInt64, v String)
ENGINE = MergeTree() ORDER BY k
SETTINGS disk = disk(
    type = cache,
    disk = '05457_borrowed_disk',
    name = '05457_wrapper',
    path = '05457_wrapper_cache/',
    max_size = '100Mi',
    load_metadata_asynchronously = 0
);

-- The injected failure is logged as an error by design, it must not be sent to the client.
SET send_logs_level = 'fatal';

SYSTEM ENABLE FAILPOINT cache_metadata_storage_fail_after_commit;
INSERT INTO wrapped VALUES (1, 'a'), (2, 'b');
INSERT INTO wrapped VALUES (3, 'c');
OPTIMIZE TABLE wrapped FINAL;
SYSTEM DISABLE FAILPOINT cache_metadata_storage_fail_after_commit;

SELECT * FROM wrapped ORDER BY k;
SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 'wrapped' AND active;

DROP TABLE wrapped SYNC;
DROP TABLE borrowed SYNC;
DROP TABLE cache_creator SYNC;
