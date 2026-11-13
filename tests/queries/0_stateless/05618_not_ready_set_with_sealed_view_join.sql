-- Tags: no-parallel, no-fasttest
-- - no-parallel - global failpoint `prepared_sets_build_ordered_set_inplace_fail`
-- - no-fasttest - tests that run alone are kept out of the fast test

-- An `IN` subquery over a sealed view (`SQL SECURITY NONE` with a `WHERE`) whose body is a `JOIN`.
-- The view's plan is optimized in advance inside `ReadFromSealedViewStep`, so the source plan that
-- `FutureSetFromSubquery::buildOrderedSetInplace` clones already holds a physical `JoinStep`: this is
-- the path where `JoinStep::clone` is reached from SQL.
--
-- The failpoint makes the speculative in-place build stop once without creating the set; it only fires
-- on the recoverable build of a cloned source. The set is then built by the deferred pipeline from the
-- preserved source, so the subquery runs twice. A non-clonable source is consumed by the in-place build
-- (the failpoint does not fire there) and the subquery runs once, so the rows read by the query tell
-- which path was taken.

DROP VIEW IF EXISTS v_sealed_join;
DROP TABLE IF EXISTS t_outer_sv;
DROP TABLE IF EXISTS t_small_sv;
DROP TABLE IF EXISTS t_big_sv;

CREATE TABLE t_outer_sv (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 1;
INSERT INTO t_outer_sv SELECT number FROM numbers(1000);

CREATE TABLE t_small_sv (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO t_small_sv SELECT number FROM numbers(20);
CREATE TABLE t_big_sv (k UInt64) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO t_big_sv SELECT number FROM numbers(1000);

-- The `WHERE` makes the view sealed, so it is read through `ReadFromSealedView`. It keeps [0, 10).
-- Not `SQL SECURITY DEFINER` with `DEFINER = CURRENT_USER`: a view queried by its own definer is read
-- transparently, and its source plan would hold a `JoinStepLogical` instead of a physical `JoinStep`.
CREATE VIEW v_sealed_join SQL SECURITY NONE AS
    SELECT s.k AS k FROM t_small_sv AS s INNER JOIN t_big_sv AS b ON s.k = b.k WHERE s.k < 10;

SET use_index_for_in_with_subqueries = 1;
SET enable_analyzer = 1;
-- `HashJoin` supports `IJoin::clone`; with spilling enabled it would be wrapped into `SpillingHashJoin`,
-- which does not.
SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0;
SET max_bytes_ratio_before_external_join = 0;
SET use_query_condition_cache = 0;

-- Without runtime filters the physical `JoinStep` is cloned and the deferred build recovers.
SET enable_join_runtime_filters = 0;

SELECT count() FROM t_outer_sv WHERE k IN (SELECT k FROM v_sealed_join) SETTINGS log_comment = '05618_clone';

SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
SELECT count() FROM t_outer_sv WHERE k IN (SELECT k FROM v_sealed_join) SETTINGS log_comment = '05618_clone_recovered';
SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

-- With runtime filters the optimized plan holds their lookup keys, so `JoinStep::clone` refuses and the
-- source is consumed by the in-place build, as before `JoinStep::clone` existed. Running a clone that
-- shares the keys with the original would register the same runtime filter twice and fail with
-- "Trying to insert into runtime filter after it was marked as finished".
SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET join_runtime_filter_from_fixed_hash_table = 0;

SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
SELECT count() FROM t_outer_sv WHERE k IN (SELECT k FROM v_sealed_join) SETTINGS log_comment = '05618_runtime_filters';
SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

-- A join algorithm without `IJoin::clone` keeps the plan non-clonable as well: `JoinStep::clone` throws
-- `NOT_IMPLEMENTED`, which stays internal, and the source is consumed by the in-place build.
SET enable_join_runtime_filters = 0;
SET join_algorithm = 'partial_merge';

SYSTEM ENABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;
SELECT count() FROM t_outer_sv WHERE k IN (SELECT k FROM v_sealed_join) SETTINGS log_comment = '05618_partial_merge';
SYSTEM DISABLE FAILPOINT prepared_sets_build_ordered_set_inplace_fail;

-- The subquery reads 20 + 1000 rows. The main query reads 10 rows of `t_outer_sv` when primary key
-- analysis has the set, and all 1000 rows when the speculative build was abandoned, in which case the
-- deferred build also reads the subquery once more.
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, read_rows FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN ('05618_clone', '05618_clone_recovered', '05618_runtime_filters', '05618_partial_merge')
ORDER BY event_time_microseconds;

DROP VIEW v_sealed_join;
DROP TABLE t_outer_sv;
DROP TABLE t_small_sv;
DROP TABLE t_big_sv;
