-- Tags: no-parallel, no-fasttest
-- no-parallel: the failpoint is server-wide and would fail inserts into Buffer tables of concurrent tests.
-- no-fasttest: failpoints are not available in the fast test build.

-- A failed first append into an empty Buffer shard, after it installed the columns of the block, must leave the shard
-- empty. Otherwise a later flush finds a block of columns without rows and without the identity of any rows.
DROP TABLE IF EXISTS buf;
DROP TABLE IF EXISTS dst;
CREATE TABLE dst (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE buf (s String) ENGINE = Buffer(currentDatabase(), dst, 1, 3600, 7200, 1000000, 2000000, 100000000, 200000000);

SYSTEM ENABLE FAILPOINT storage_buffer_fail_after_installing_columns;
INSERT INTO buf SETTINGS async_insert = 0 VALUES ('rejected'); -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT storage_buffer_fail_after_installing_columns;

OPTIMIZE TABLE buf;
INSERT INTO buf SETTINGS async_insert = 0 VALUES ('kept');
OPTIMIZE TABLE buf;
SELECT 'delivered', groupArray(s) FROM dst;

DROP TABLE buf;
DROP TABLE dst;
