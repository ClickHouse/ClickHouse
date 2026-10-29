-- The query condition cache must not record a verdict computed from a non-deterministic default.
-- Every case prints the count with the cache and without it.

SET use_query_condition_cache = 1;
-- Without a local plan the filter steps run as part of the remote queries, and this server's cache sees nothing.
SET parallel_replicas_local_plan = 1;
-- The pending mutations below must not run, and the `ALTER`s must not wait for them.
SET mutations_sync = 0;
SET alter_sync = 0;
SET session_timezone = 'UTC';
-- WHERE cases must stay WHERE: the automatic move to PREWHERE would exercise the other writer.
SET optimize_move_to_prewhere = 0;

-- The auto minmax indexes would answer before the cache, and the cache stores nothing for small parts.
-- Non-deterministic defaults: `thr` is 5 seconds after each table's INSERT and the warm-up runs right after it, so no row matches before the sleep and every row after it.
CREATE TABLE t_now (k UInt64, v UInt64, thr DateTime64(3)) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now;
INSERT INTO t_now SELECT number, number, now64(3) + 5 FROM numbers(100000);
ALTER TABLE t_now ADD COLUMN ts DateTime64(3) DEFAULT now64(3);
SELECT count() FROM t_now WHERE ts >= thr FORMAT Null;

CREATE TABLE t_now_prewhere (k UInt64, v UInt64, thr DateTime64(3)) ENGINE = MergeTree ORDER BY k
    SETTINGS index_granularity = 8192, add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now_prewhere;
INSERT INTO t_now_prewhere SELECT number, number, now64(3) + 5 FROM numbers(100000);
ALTER TABLE t_now_prewhere ADD COLUMN ts DateTime64(3) DEFAULT now64(3);
SELECT count() FROM t_now_prewhere PREWHERE ts >= thr FORMAT Null;

-- The same default in a tuple, filtered by a subcolumn.
CREATE TABLE t_now_tuple (k UInt64, thr DateTime64(3)) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now_tuple;
INSERT INTO t_now_tuple SELECT number, now64(3) + 5 FROM numbers(100000);
ALTER TABLE t_now_tuple ADD COLUMN tup Tuple(a DateTime64(3), b UInt8) DEFAULT (now64(3), 1);
SELECT count() FROM t_now_tuple WHERE tup.a >= thr FORMAT Null;

-- A non-deterministic default that replaces NULLs while a change to a non-Nullable type is pending.
CREATE TABLE t_now_nullable (k UInt64, v Nullable(DateTime64(3)), thr DateTime64(3)) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now_nullable;
INSERT INTO t_now_nullable SELECT number, NULL, now64(3) + 5 FROM numbers(100000);
ALTER TABLE t_now_nullable MODIFY COLUMN v DateTime64(3) DEFAULT now64(3);
SELECT count() FROM t_now_nullable WHERE v >= thr FORMAT Null;

CREATE TABLE t_now_nullable_prewhere (k UInt64, v Nullable(DateTime64(3)), thr DateTime64(3)) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now_nullable_prewhere;
INSERT INTO t_now_nullable_prewhere SELECT number, NULL, now64(3) + 5 FROM numbers(100000);
ALTER TABLE t_now_nullable_prewhere MODIFY COLUMN v DateTime64(3) DEFAULT now64(3);
SELECT count() FROM t_now_nullable_prewhere PREWHERE v >= thr FORMAT Null;

-- A default that reads such a column.
CREATE TABLE t_now_nullable_dependent (k UInt64, v Nullable(DateTime64(3)), thr DateTime64(3)) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_now_nullable_dependent;
INSERT INTO t_now_nullable_dependent SELECT number, NULL, now64(3) + 5 FROM numbers(100000);
ALTER TABLE t_now_nullable_dependent MODIFY COLUMN v DateTime64(3) DEFAULT now64(3);
ALTER TABLE t_now_nullable_dependent ADD COLUMN w DateTime64(3) DEFAULT v;
SELECT count() FROM t_now_nullable_dependent WHERE w >= thr FORMAT Null;

SELECT sleep(3) FORMAT Null;
SELECT sleep(3) FORMAT Null;
SELECT 'DEFAULT now64, WHERE', count() FROM t_now WHERE ts >= thr;
SELECT 'DEFAULT now64, WHERE', count() FROM t_now WHERE ts >= thr SETTINGS use_query_condition_cache = 0;
SELECT 'DEFAULT now64, PREWHERE', count() FROM t_now_prewhere PREWHERE ts >= thr;
SELECT 'DEFAULT now64, PREWHERE', count() FROM t_now_prewhere PREWHERE ts >= thr SETTINGS use_query_condition_cache = 0;
SELECT 'DEFAULT now64, subcolumn', count() FROM t_now_tuple WHERE tup.a >= thr;
SELECT 'DEFAULT now64, subcolumn', count() FROM t_now_tuple WHERE tup.a >= thr SETTINGS use_query_condition_cache = 0;
SELECT 'DEFAULT now64 for NULL, WHERE', count() FROM t_now_nullable WHERE v >= thr;
SELECT 'DEFAULT now64 for NULL, WHERE', count() FROM t_now_nullable WHERE v >= thr SETTINGS use_query_condition_cache = 0;
SELECT 'DEFAULT now64 for NULL, PREWHERE', count() FROM t_now_nullable_prewhere PREWHERE v >= thr;
SELECT 'DEFAULT now64 for NULL, PREWHERE', count() FROM t_now_nullable_prewhere PREWHERE v >= thr SETTINGS use_query_condition_cache = 0;
SELECT 'DEFAULT reading a column filled by now64', count() FROM t_now_nullable_dependent WHERE w >= thr;
SELECT 'DEFAULT reading a column filled by now64', count() FROM t_now_nullable_dependent WHERE w >= thr SETTINGS use_query_condition_cache = 0;

-- A condition on stored columns is still cached when the query also reads such a default.
-- The WHERE writer records whole blocks, so a block must not span the matching and the non-matching granules.
SELECT ts FROM t_now WHERE v < 10000 SETTINGS max_block_size = 8192 FORMAT Null;
SELECT ts FROM t_now WHERE v < 10000 SETTINGS max_block_size = 8192, log_comment = '05458_now_where_hit' FORMAT Null;
SELECT ts FROM t_now_prewhere PREWHERE v < 10000 FORMAT Null;
SELECT ts FROM t_now_prewhere PREWHERE v < 10000 SETTINGS log_comment = '05458_now_prewhere_hit' FORMAT Null;

SYSTEM FLUSH LOGS query_log;
SELECT log_comment, ProfileEvents['QueryConditionCacheHits'] > 0, ProfileEvents['SelectedMarks'] < ProfileEvents['SelectedMarksTotal']
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05458_now_where_hit', '05458_now_prewhere_hit')
ORDER BY log_comment;
