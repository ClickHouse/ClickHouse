-- The query condition cache must not reuse a verdict across an `ALTER` that changes the type of a column a default depends on, or drops and re-adds a column.
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
-- A pending type change of a column the default depends on, with and without mutations applied on the fly.
CREATE TABLE t_type (k UInt64, d Date) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_type;
INSERT INTO t_type SELECT number, '2020-01-01' FROM numbers(100000);
ALTER TABLE t_type ADD COLUMN s String DEFAULT toString(d);
SELECT count() FROM t_type WHERE s = '2020-01-01 00:00:00' FORMAT Null;
ALTER TABLE t_type MODIFY COLUMN d DateTime;
SELECT 'MODIFY type', count() FROM t_type WHERE s = '2020-01-01 00:00:00';
SELECT 'MODIFY type', count() FROM t_type WHERE s = '2020-01-01 00:00:00' SETTINGS use_query_condition_cache = 0;

CREATE TABLE t_type_not_on_fly (k UInt64, d Date) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_type_not_on_fly;
INSERT INTO t_type_not_on_fly SELECT number, '2020-01-01' FROM numbers(100000);
ALTER TABLE t_type_not_on_fly ADD COLUMN s String DEFAULT toString(d);
SELECT count() FROM t_type_not_on_fly WHERE s = '2020-01-01 00:00:00' SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0 FORMAT Null;
ALTER TABLE t_type_not_on_fly MODIFY COLUMN d DateTime;
SELECT 'MODIFY type, not on the fly', count() FROM t_type_not_on_fly WHERE s = '2020-01-01 00:00:00' SETTINGS apply_mutations_on_fly = 0, apply_patch_parts = 0;
SELECT 'MODIFY type, not on the fly', count() FROM t_type_not_on_fly WHERE s = '2020-01-01 00:00:00' SETTINGS use_query_condition_cache = 0, apply_mutations_on_fly = 0, apply_patch_parts = 0;

-- A time zone change of a column the default depends on, which needs no mutation.
CREATE TABLE t_timezone (k UInt64, d DateTime('UTC')) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_timezone;
INSERT INTO t_timezone SELECT number, '2020-01-01 00:00:00' FROM numbers(100000);
ALTER TABLE t_timezone ADD COLUMN s String DEFAULT toString(d);
SELECT count() FROM t_timezone WHERE s = '2020-01-01 09:00:00' FORMAT Null;
ALTER TABLE t_timezone MODIFY COLUMN d DateTime('Asia/Tokyo');
SELECT 'MODIFY time zone', count() FROM t_timezone WHERE s = '2020-01-01 09:00:00';
SELECT 'MODIFY time zone', count() FROM t_timezone WHERE s = '2020-01-01 09:00:00' SETTINGS use_query_condition_cache = 0;

-- DROP COLUMN and ADD COLUMN of the same column while the drop is pending, then KILL MUTATION.
-- Part pruning by statistics and the trivial count over them do not see the pending drop, so they are off here.
CREATE TABLE t_drop_add (k UInt64, y UInt8) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_drop_add;
INSERT INTO t_drop_add SELECT number, 1 FROM numbers(100000);
SELECT count() FROM t_drop_add WHERE y = 0 SETTINGS use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0 FORMAT Null;
ALTER TABLE t_drop_add DROP COLUMN y;
ALTER TABLE t_drop_add ADD COLUMN y UInt8;
SELECT 'DROP and ADD', count() FROM t_drop_add WHERE y = 0 SETTINGS use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0;
SELECT 'DROP and ADD', count() FROM t_drop_add WHERE y = 0 SETTINGS use_query_condition_cache = 0, use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0;

CREATE TABLE t_drop_add_kill (k UInt64, y UInt8) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_drop_add_kill;
INSERT INTO t_drop_add_kill SELECT number, 1 FROM numbers(100000);
ALTER TABLE t_drop_add_kill DROP COLUMN y;
ALTER TABLE t_drop_add_kill ADD COLUMN y UInt8;
SELECT count() FROM t_drop_add_kill WHERE y = 1 SETTINGS use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0 FORMAT Null;
KILL MUTATION WHERE database = currentDatabase() AND table = 't_drop_add_kill' SYNC FORMAT Null;
SELECT 'DROP and ADD, KILL MUTATION', count() FROM t_drop_add_kill WHERE y = 1 SETTINGS use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0;
SELECT 'DROP and ADD, KILL MUTATION', count() FROM t_drop_add_kill WHERE y = 1 SETTINGS use_query_condition_cache = 0, use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0;

-- DROP COLUMN and ADD COLUMN of a column a `SELECT *` projection stores, while the drop is pending.
CREATE TABLE t_drop_add_projection (k UInt64, k2 UInt64, y UInt8, PROJECTION p (SELECT * ORDER BY k2)) ENGINE = MergeTree ORDER BY k
    SETTINGS add_minmax_index_for_numeric_columns = 0, add_minmax_index_for_temporal_columns = 0, add_minmax_index_for_string_columns = 0;
SYSTEM STOP MERGES t_drop_add_projection;
INSERT INTO t_drop_add_projection SELECT number, number, 1 FROM numbers(100000);
SELECT count() FROM t_drop_add_projection WHERE y = 0
    SETTINGS optimize_use_projections = 1, prefer_optimize_projection = 1, optimize_use_implicit_projections = 0, enable_parallel_replicas = 0,
    use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0 FORMAT Null;
ALTER TABLE t_drop_add_projection DROP COLUMN y;
ALTER TABLE t_drop_add_projection ADD COLUMN y UInt8;
SELECT 'DROP and ADD, projection', count() FROM t_drop_add_projection WHERE y = 0
    SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 0, enable_parallel_replicas = 0,
    use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0;
SELECT 'DROP and ADD, projection', count() FROM t_drop_add_projection WHERE y = 0
    SETTINGS use_query_condition_cache = 0, optimize_use_projections = 1, optimize_use_implicit_projections = 0, enable_parallel_replicas = 0,
    use_statistics_for_part_pruning = 0, optimize_trivial_count_with_sparsity_filter = 0;
