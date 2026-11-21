-- Tags: no-parallel-replicas
-- `max_rows_to_read` and `max_bytes_to_read` limit how much one read round of a `STREAM` query reads.

SET enable_analyzer = 1; -- streaming queries require the analyzer (CI randomizes this setting)
SET enable_streaming_queries = 1;
SET use_skip_indexes_on_data_read = 0;
SET async_insert = 0, max_insert_threads = 1, max_block_size = 65505, max_insert_block_size = 1048449;

DROP TABLE IF EXISTS t_stream_round_limits;
DROP TABLE IF EXISTS t_stream_round_limits_partitioned;
DROP TABLE IF EXISTS t_stream_round_limits_merged;

CREATE TABLE t_stream_round_limits (k UInt64, ts DateTime DEFAULT toDateTime(k))
ENGINE = MergeTree ORDER BY k
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

-- The limit is estimated from part sizes; single-block parts make it exact.
SYSTEM STOP MERGES t_stream_round_limits;

-- Ten blocks (1 to 10) of ten rows each.
INSERT INTO t_stream_round_limits (k) SELECT number FROM numbers(0, 10);
INSERT INTO t_stream_round_limits (k) SELECT number FROM numbers(10, 10);
INSERT INTO t_stream_round_limits (k) SELECT number FROM numbers(20, 10);
INSERT INTO t_stream_round_limits (k) SELECT number FROM numbers(30, 10);
INSERT INTO t_stream_round_limits (k) SELECT number FROM numbers(40, 10);
INSERT INTO t_stream_round_limits (k) SELECT number FROM numbers(50, 10);
INSERT INTO t_stream_round_limits (k) SELECT number FROM numbers(60, 10);
INSERT INTO t_stream_round_limits (k) SELECT number FROM numbers(70, 10);
INSERT INTO t_stream_round_limits (k) SELECT number FROM numbers(80, 10);
INSERT INTO t_stream_round_limits (k) SELECT number FROM numbers(90, 10);

-- 0 means no limit.
SELECT 'no limit', count(), min(k), max(k) FROM t_stream_round_limits STREAM BOUNDED;

-- A bounded stream reads one round.
SELECT 'rows 30', count(), min(k), max(k) FROM t_stream_round_limits STREAM BOUNDED
SETTINGS max_rows_to_read = 30;

-- A cursor continues from where the previous round stopped.
SELECT 'rows 30 from block 4', count(), min(k), max(k) FROM t_stream_round_limits STREAM BOUNDED CURSOR {'all': {'block_number': 4}}
SETTINGS max_rows_to_read = 30;

-- The round does not take a part that crosses the limit.
SELECT 'rows 25', count(), min(k), max(k) FROM t_stream_round_limits STREAM BOUNDED
SETTINGS max_rows_to_read = 25;

-- A limit below one part still reads one part.
SELECT 'rows 1', count(), min(k), max(k) FROM t_stream_round_limits STREAM BOUNDED
SETTINGS max_rows_to_read = 1;

SELECT 'bytes 1', count(), min(k), max(k) FROM t_stream_round_limits STREAM BOUNDED
SETTINGS max_bytes_to_read = 1;

-- An unbounded stream reads all rows exactly once over several rounds.
SELECT 'unbounded rows 30', count(), uniqExact(k), sum(k)
FROM (SELECT k FROM t_stream_round_limits STREAM LIMIT 100)
SETTINGS max_rows_to_read = 30;

SELECT 'unbounded bytes 1', count(), uniqExact(k), sum(k)
FROM (SELECT k FROM t_stream_round_limits STREAM LIMIT 100)
SETTINGS max_bytes_to_read = 1;

-- The tightest limit of the query and its parent queries applies.
SELECT 'outer rows 30', count(), min(k), max(k)
FROM (SELECT k FROM t_stream_round_limits STREAM BOUNDED SETTINGS max_rows_to_read = 100)
SETTINGS max_rows_to_read = 30;

SELECT 'inner rows 30', count(), min(k), max(k)
FROM (SELECT k FROM t_stream_round_limits STREAM BOUNDED SETTINGS max_rows_to_read = 30)
SETTINGS max_rows_to_read = 100;

SELECT 'outer rows 30, inner no limit', count(), min(k), max(k)
FROM (SELECT k FROM t_stream_round_limits STREAM BOUNDED SETTINGS max_rows_to_read = 0)
SETTINGS max_rows_to_read = 30;

SELECT 'outer bytes 1', count(), min(k), max(k)
FROM (SELECT k FROM t_stream_round_limits STREAM BOUNDED SETTINGS max_bytes_to_read = 0)
SETTINGS max_bytes_to_read = 1;

-- The same with a watermark.
SELECT 'watermark rows 30', count(), min(k), max(k) FROM t_stream_round_limits STREAM BOUNDED WATERMARK FOR ts AS ts - INTERVAL 10 SECOND
SETTINGS max_rows_to_read = 30;

SELECT 'watermark unbounded rows 30', count(), uniqExact(k), sum(k)
FROM (SELECT k FROM t_stream_round_limits STREAM WATERMARK FOR ts AS ts - INTERVAL 10 SECOND LIMIT 100)
SETTINGS max_rows_to_read = 30;

DROP TABLE t_stream_round_limits;

-- Two partitions of five blocks each. The checks do not depend on which partition a round reads first.
-- Several threads: the limit still covers the whole round.
CREATE TABLE t_stream_round_limits_partitioned (k UInt64, ts DateTime DEFAULT toDateTime(k))
ENGINE = MergeTree ORDER BY k PARTITION BY k >= 50
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

SYSTEM STOP MERGES t_stream_round_limits_partitioned;

INSERT INTO t_stream_round_limits_partitioned (k) SELECT number FROM numbers(0, 10);
INSERT INTO t_stream_round_limits_partitioned (k) SELECT number FROM numbers(10, 10);
INSERT INTO t_stream_round_limits_partitioned (k) SELECT number FROM numbers(20, 10);
INSERT INTO t_stream_round_limits_partitioned (k) SELECT number FROM numbers(30, 10);
INSERT INTO t_stream_round_limits_partitioned (k) SELECT number FROM numbers(40, 10);
INSERT INTO t_stream_round_limits_partitioned (k) SELECT number FROM numbers(50, 10);
INSERT INTO t_stream_round_limits_partitioned (k) SELECT number FROM numbers(60, 10);
INSERT INTO t_stream_round_limits_partitioned (k) SELECT number FROM numbers(70, 10);
INSERT INTO t_stream_round_limits_partitioned (k) SELECT number FROM numbers(80, 10);
INSERT INTO t_stream_round_limits_partitioned (k) SELECT number FROM numbers(90, 10);

-- The budget is shared by all partitions of the round.
SELECT 'partitioned rows 30', count() FROM t_stream_round_limits_partitioned STREAM BOUNDED
SETTINGS max_rows_to_read = 30, max_threads = 4;

-- What is left after the first partition goes to the next one.
SELECT 'partitioned rows 70', count(), uniqExact(_partition_id) FROM t_stream_round_limits_partitioned STREAM BOUNDED
SETTINGS max_rows_to_read = 70, max_threads = 4;

SELECT 'partitioned unbounded rows 30', count(), uniqExact(k), sum(k)
FROM (SELECT k FROM t_stream_round_limits_partitioned STREAM LIMIT 100)
SETTINGS max_rows_to_read = 30, max_threads = 4;

-- With a watermark, a partition left out of a round is still read later.
SELECT 'partitioned watermark unbounded rows 30', count(), uniqExact(k), sum(k)
FROM (SELECT k FROM t_stream_round_limits_partitioned STREAM WATERMARK FOR ts AS ts - INTERVAL 10 SECOND LIMIT 100)
SETTINGS max_rows_to_read = 30, max_threads = 4;

DROP TABLE t_stream_round_limits_partitioned;

-- A merged part may have gaps in its block numbers: here partition 0 gets blocks 1, 3 and 5, merged into one part.
CREATE TABLE t_stream_round_limits_merged (k UInt64)
ENGINE = MergeTree ORDER BY k PARTITION BY k % 2
SETTINGS
    enable_block_number_column = 1,
    enable_block_offset_column = 1,
    add_minmax_index_for_block_number_column = 1,
    add_minmax_index_for_block_offset_column = 1,
    part_minmax_index_columns = 'with_block_number_offset';

INSERT INTO t_stream_round_limits_merged SELECT number * 2 FROM numbers(0, 10);
INSERT INTO t_stream_round_limits_merged SELECT number * 2 + 1 FROM numbers(0, 10);
INSERT INTO t_stream_round_limits_merged SELECT number * 2 FROM numbers(10, 10);
INSERT INTO t_stream_round_limits_merged SELECT number * 2 + 1 FROM numbers(10, 10);
INSERT INTO t_stream_round_limits_merged SELECT number * 2 FROM numbers(20, 10);
OPTIMIZE TABLE t_stream_round_limits_merged PARTITION 0 FINAL;

-- From block 2 of the merged part, the round reads the rest of it (blocks 3 and 5) instead of the empty block 2.
SELECT 'merged part from block 2', count(), min(k), max(k)
FROM t_stream_round_limits_merged STREAM BOUNDED CURSOR {'0': {'block_number': 2}, '1': {'block_number': 1000}}
SETTINGS max_rows_to_read = 1;

DROP TABLE t_stream_round_limits_merged;
