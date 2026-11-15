-- A granule of a text bloom filter index allocates one filter per indexed column,
-- so the total size of filters (filter size multiplied by the number of columns) is limited by 1 GiB.

DROP TABLE IF EXISTS t_bf_total_size;

-- A single filter of the maximum size is still allowed.
CREATE TABLE t_bf_total_size (a String, INDEX idx a TYPE tokenbf_v1(1073741824, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_bf_total_size;

-- Two columns with filters of half of the maximum size are allowed.
CREATE TABLE t_bf_total_size (a String, b String, INDEX idx (a, b) TYPE tokenbf_v1(536870912, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple();
DROP TABLE t_bf_total_size;

-- One byte more is too much.
CREATE TABLE t_bf_total_size (a String, b String, INDEX idx (a, b) TYPE tokenbf_v1(536870913, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bf_total_size (a String, b String, INDEX idx (a, b) TYPE ngrambf_v1(3, 536870913, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_bf_total_size (a String, b String, c String, d String, e String, f String, g String, h String,
    INDEX idx (a, b, c, d, e, f, g, h) TYPE tokenbf_v1(268435456, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

-- The same check is done for ALTER TABLE ... ADD INDEX.
CREATE TABLE t_bf_total_size (a String, b String) ENGINE = MergeTree ORDER BY tuple();
ALTER TABLE t_bf_total_size ADD INDEX idx (a, b) TYPE tokenbf_v1(536870913, 3, 0) GRANULARITY 1; -- { serverError BAD_ARGUMENTS }
ALTER TABLE t_bf_total_size ADD INDEX idx (a, b) TYPE tokenbf_v1(1024, 3, 0) GRANULARITY 1;
INSERT INTO t_bf_total_size VALUES ('hello', 'world');
SELECT count() FROM t_bf_total_size WHERE hasToken(a, 'hello');
DROP TABLE t_bf_total_size;

-- The memory of bloom filters is tracked, so a too large filter fails with an exception instead of exhausting the memory.
CREATE TABLE t_bf_total_size (a String, INDEX idx a TYPE tokenbf_v1(268435456, 3, 0) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_bf_total_size SETTINGS max_memory_usage = '100Mi', async_insert = 0 VALUES ('hello world'); -- { serverError MEMORY_LIMIT_EXCEEDED }
SELECT count() FROM t_bf_total_size;
DROP TABLE t_bf_total_size;
