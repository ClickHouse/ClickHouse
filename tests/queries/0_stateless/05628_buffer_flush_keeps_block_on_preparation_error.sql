-- Tags: no-parallel, no-fasttest
-- no-parallel: the failpoint is server-wide and would fail Buffer flushes of concurrent tests.
-- no-fasttest: failpoints are not available in the fast test build.

-- A failure while preparing a flush after the block has left the buffer (here while capturing the identity that
-- triggered the flush) must put the block back, so the rows are not lost and are delivered on retry.
DROP TABLE IF EXISTS buf;
DROP TABLE IF EXISTS dst;
CREATE TABLE dst (s String) ENGINE = MergeTree ORDER BY s;
CREATE TABLE buf (s String) ENGINE = Buffer(currentDatabase(), dst, 1, 3600, 7200, 1000000, 2000000, 100000000, 200000000);
INSERT INTO buf SETTINGS async_insert = 0 VALUES ('kept');

SYSTEM ENABLE FAILPOINT storage_buffer_fail_capturing_flush_identity;
OPTIMIZE TABLE buf; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT storage_buffer_fail_capturing_flush_identity;

SELECT 'still buffered', count() FROM buf;
OPTIMIZE TABLE buf;
SELECT 'delivered', groupArray(s) FROM dst;

DROP TABLE buf;
DROP TABLE dst;
