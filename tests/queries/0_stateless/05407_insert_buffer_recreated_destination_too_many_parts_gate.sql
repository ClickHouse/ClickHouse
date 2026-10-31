-- A `Buffer` keeps the pre-write decisions made for the queries whose rows it still holds, and
-- resolves its destination by name again on every flush. When the destination is dropped and
-- recreated under the same name in between, the new table must get its own `Too many parts` check
-- instead of reusing the acceptance made for the old table.

DROP TABLE IF EXISTS t_05407_buf;
DROP TABLE IF EXISTS t_05407_dst;

CREATE TABLE t_05407_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1000;

-- A single row fits into the buffer; the next row flushes it to the destination.
CREATE TABLE t_05407_buf (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05407_dst, 1, 1000000, 1000000, 1000000, 1, 1000000000, 1000000000);

-- Three single-row blocks: the first two are flushed to the destination, which accepts the writes;
-- the last row stays in the buffer.
INSERT INTO t_05407_buf SELECT number FROM numbers(3)
    SETTINGS max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 1,
        max_insert_threads = 1;

SELECT count() FROM t_05407_dst;
SELECT count() FROM t_05407_buf;

-- The destination is replaced by a new table with the same name, which is already at its limit.
DROP TABLE t_05407_dst;
CREATE TABLE t_05407_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 1;
INSERT INTO t_05407_dst VALUES (100);

-- The flush of the remaining row is a write into the new table: it is checked and rejected.
OPTIMIZE TABLE t_05407_buf; -- { serverError TOO_MANY_PARTS }

SELECT count() FROM t_05407_dst;
SELECT count() FROM t_05407_buf;

DROP TABLE t_05407_buf;
DROP TABLE t_05407_dst;
