-- With `materialized_views_ignore_errors` the rejection of one view branch by its stricter limits is kept with
-- that branch only. A sibling branch writing into the same table through a `Buffer` was accepted with the
-- limits of the table, and the threshold flushes of the buffer the INSERT runs observe that acceptance: a
-- later flush does not count the parts the earlier flushes of the same INSERT have created.

DROP TABLE IF EXISTS t_05458_mv_direct;
DROP TABLE IF EXISTS t_05458_mv_buf;
DROP TABLE IF EXISTS t_05458_src;
DROP TABLE IF EXISTS t_05458_buf;
DROP TABLE IF EXISTS t_05458_dst;

CREATE TABLE t_05458_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 2;
SYSTEM STOP MERGES t_05458_dst;

-- A one-row block stays in the buffer, the next one flushes it by the `max_rows` threshold.
CREATE TABLE t_05458_buf (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05458_dst, 1, 1000000, 1000000, 1000000, 1, 1000000000, 1000000000);

CREATE TABLE t_05458_src (n UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW t_05458_mv_direct TO t_05458_dst AS SELECT n FROM t_05458_src;
CREATE MATERIALIZED VIEW t_05458_mv_buf TO t_05458_buf AS SELECT n + 1000 AS n FROM t_05458_src;

INSERT INTO t_05458_dst VALUES (100);

-- The ignored error of the direct branch is logged.
SET send_logs_level = 'error';

-- The direct branch is rejected by the query-level limit and its error is ignored. The branch through the
-- buffer is accepted with the limit of the table, and its two threshold flushes both write a part.
INSERT INTO t_05458_src SELECT number + 1 FROM numbers(3)
    SETTINGS parts_to_throw_insert = 1, materialized_views_ignore_errors = 1, parallel_view_processing = 0,
        max_insert_threads = 1, max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;

SELECT n FROM t_05458_dst ORDER BY n;

-- The last row is still in the buffer, which is flushed when it is dropped.
ALTER TABLE t_05458_dst MODIFY SETTING parts_to_throw_insert = 1000;

DROP TABLE t_05458_mv_direct;
DROP TABLE t_05458_mv_buf;
DROP TABLE t_05458_src;
DROP TABLE t_05458_buf;
DROP TABLE t_05458_dst;
