-- With `materialized_views_ignore_errors` the rejection of one view branch by its stricter limits is kept with
-- that branch only, also when the rejected branch forwards the write through an `Alias`: its check is enforced
-- by the `Alias`, not by the `Buffer` of the sibling branch. The threshold flushes of the buffer observe the
-- acceptance of the write through the buffer, so a later flush does not count the parts the earlier flushes
-- of the same INSERT have created.

DROP TABLE IF EXISTS t_05460_mv_alias;
DROP TABLE IF EXISTS t_05460_mv_buf;
DROP TABLE IF EXISTS t_05460_src;
DROP TABLE IF EXISTS t_05460_alias;
DROP TABLE IF EXISTS t_05460_buf;
DROP TABLE IF EXISTS t_05460_dst;

CREATE TABLE t_05460_dst (n UInt64) ENGINE = MergeTree ORDER BY n
    SETTINGS parts_to_throw_insert = 2;
SYSTEM STOP MERGES t_05460_dst;

-- A one-row block stays in the buffer, the next one flushes it by the `max_rows` threshold.
CREATE TABLE t_05460_buf (n UInt64)
    ENGINE = Buffer(currentDatabase(), t_05460_dst, 1, 1000000, 1000000, 1000000, 1, 1000000000, 1000000000);
CREATE TABLE t_05460_alias ENGINE = Alias('t_05460_dst');

CREATE TABLE t_05460_src (n UInt64) ENGINE = Null;
CREATE MATERIALIZED VIEW t_05460_mv_alias TO t_05460_alias AS SELECT n FROM t_05460_src;
CREATE MATERIALIZED VIEW t_05460_mv_buf TO t_05460_buf AS SELECT n + 1000 AS n FROM t_05460_src;

INSERT INTO t_05460_dst VALUES (100);

-- The ignored error of the branch through the alias is logged.
SET send_logs_level = 'error';

-- The branch through the alias is rejected by the query-level limit and its error is ignored. The branch
-- through the buffer is accepted with the limit of the table, and its two threshold flushes both write a part.
INSERT INTO t_05460_src SELECT number + 1 FROM numbers(3)
    SETTINGS parts_to_throw_insert = 1, materialized_views_ignore_errors = 1, parallel_view_processing = 0,
        max_insert_threads = 1, max_block_size = 1, min_insert_block_size_rows = 1, min_insert_block_size_bytes = 0;

SELECT n FROM t_05460_dst ORDER BY n;

-- The last row is still in the buffer, which is flushed when it is dropped.
ALTER TABLE t_05460_dst MODIFY SETTING parts_to_throw_insert = 1000;

DROP TABLE t_05460_mv_alias;
DROP TABLE t_05460_mv_buf;
DROP TABLE t_05460_src;
DROP TABLE t_05460_alias;
DROP TABLE t_05460_buf;
DROP TABLE t_05460_dst;
