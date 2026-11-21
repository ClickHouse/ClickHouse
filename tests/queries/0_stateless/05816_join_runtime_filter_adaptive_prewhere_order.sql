-- Tags: no-fasttest
-- no-fasttest: Parquet

-- Two runtime filters in PREWHERE, planned weak first (on `a`, 50%, the smaller column) and selective second (on `b`, 1%).
-- Once the first part or file has measured them, the later ones run the selective one first. A filter that could throw
-- on the rows the one ahead removes stays behind.

-- Runtime filters need the analyzer. They are built for every probe, the joins run in query order on one node, and a filter
-- switches off only past 70%, so the one on `a` (50%) keeps removing the zeros `mt_div_on` divides by.
SET enable_analyzer = 1;
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, join_algorithm = 'hash',
    join_runtime_filter_pass_ratio_threshold_for_disabling = 0.7, query_plan_join_swap_table = 0,
    query_plan_optimize_join_order_limit = 0, enable_parallel_replicas = 0, automatic_parallel_replicas_mode = 0;
-- No granule or row group skipped by the filters' keys or by the query condition cache before the filters probe it.
SET enable_join_runtime_filters_index_analysis = 0, use_query_condition_cache = 0, input_format_parquet_dictionary_filter_push_down = 0;
-- Every condition in PREWHERE, a step per condition group, ordered by column size: the weak filter on the smaller `a` first.
SET optimize_move_to_prewhere = 1, query_plan_merge_filters = 1, enable_multiple_prewhere_read_steps = 1,
    allow_reorder_prewhere_conditions = 1;
-- One thread reads the parts and files in order, so the later ones see what the earlier ones measured, in blocks large enough
-- to time the probe. One part per INSERT: the cases select rows by part name.
SET max_threads = 1, max_block_size = 65536, max_insert_threads = 1;

DROP TABLE IF EXISTS probe;
DROP TABLE IF EXISTS probe_nullable;
DROP TABLE IF EXISTS build_weak;
DROP TABLE IF EXISTS build_weak_shifted;
DROP TABLE IF EXISTS build_strong;
DROP TABLE IF EXISTS build_anti;
DROP TABLE IF EXISTS probe_nullable_string;
DROP TABLE IF EXISTS build_anti_string;
DROP TABLE IF EXISTS probe_div;
DROP TABLE IF EXISTS probe_patch;
DROP TABLE IF EXISTS probe_default;
DROP TABLE IF EXISTS probe_readded;
DROP TABLE IF EXISTS probe_modified;
DROP TABLE IF EXISTS probe_shared;
DROP TABLE IF EXISTS build_div;

CREATE TABLE probe (a UInt32 CODEC(NONE), b UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO probe SELECT number % 1000, intHash32(number) % 1000 FROM numbers(0, 131072);
INSERT INTO probe SELECT number % 1000, intHash32(number) % 1000 FROM numbers(131072, 131072);
INSERT INTO probe SELECT number % 1000, intHash32(number) % 1000 FROM numbers(262144, 131072);
INSERT INTO probe SELECT number % 1000, intHash32(number) % 1000 FROM numbers(393216, 131072);
INSERT INTO probe SELECT number % 1000, intHash32(number) % 1000 FROM numbers(524288, 131072);
INSERT INTO probe SELECT number % 1000, intHash32(number) % 1000 FROM numbers(655360, 131072);

CREATE TABLE build_weak (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO build_weak SELECT number FROM numbers(500);
CREATE TABLE build_strong (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO build_strong SELECT number * 100 FROM numbers(10);

SELECT 'MergeTree';
SELECT count(), sum(a), sum(b) FROM probe AS p JOIN build_weak AS w ON p.a = w.k JOIN build_strong AS s ON p.b = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 1, log_comment = 'mt_on';
SELECT count(), sum(a), sum(b) FROM probe AS p JOIN build_weak AS w ON p.a = w.k JOIN build_strong AS s ON p.b = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 0, log_comment = 'mt_off';

-- A second join on `a`: its filter reads the same column, so it shares one step with the first. The step on `b` moves ahead.
CREATE TABLE build_weak_shifted (k UInt32) ENGINE = MergeTree ORDER BY k;
INSERT INTO build_weak_shifted SELECT number + 200 FROM numbers(500);
SELECT count(), sum(a), sum(b) FROM probe AS p JOIN build_weak AS w ON p.a = w.k JOIN build_weak_shifted AS w2 ON p.a = w2.k
JOIN build_strong AS s ON p.b = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 1, log_comment = 'mt_shared_step_on';

-- An ANTI join on a Nullable key, whose filter also passes the NULL keys: it passes 1% and moves ahead.
CREATE TABLE probe_nullable (a UInt32 CODEC(NONE), b Nullable(UInt64) CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', max_bytes_to_merge_at_max_space_in_pool = 1;
INSERT INTO probe_nullable SELECT a, b FROM probe WHERE _part = 'all_1_1_0';
INSERT INTO probe_nullable SELECT a, b FROM probe WHERE _part = 'all_2_2_0';
INSERT INTO probe_nullable SELECT a, b FROM probe WHERE _part = 'all_3_3_0';
INSERT INTO probe_nullable SELECT a, b FROM probe WHERE _part = 'all_4_4_0';
CREATE TABLE build_anti (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO build_anti SELECT number FROM numbers(1000) WHERE number % 100 != 0;
SELECT count() FROM probe_nullable AS p JOIN build_weak AS w ON p.a = w.k LEFT ANTI JOIN build_anti AS s ON p.b = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 1, log_comment = 'mt_anti_on';
SELECT count() FROM probe_nullable AS p JOIN build_weak AS w ON p.a = w.k LEFT ANTI JOIN build_anti AS s ON p.b = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 0, log_comment = 'mt_anti_off';

-- An ANTI join on a Nullable(String) key, mostly NULL, with NULL on the build side: the filter counts the rows of a NULL key as
-- rejected, while the join keeps them. Its measurement does not tell what the conjunct passes, so it does not move.
CREATE TABLE probe_nullable_string (a UInt32 CODEC(NONE), b Nullable(String) CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES probe_nullable_string;
INSERT INTO probe_nullable_string SELECT a, if(b = 0, 'x', NULL) FROM probe WHERE _part = 'all_1_1_0';
INSERT INTO probe_nullable_string SELECT a, if(b = 0, 'x', NULL) FROM probe WHERE _part = 'all_2_2_0';
CREATE TABLE build_anti_string (k Nullable(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO build_anti_string VALUES ('y'), (NULL);
SELECT count() FROM probe_nullable_string AS p JOIN build_weak AS w ON p.a = w.k LEFT ANTI JOIN build_anti_string AS s ON p.b = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 1, log_comment = 'mt_anti_null_build_on';
SELECT count() FROM probe_nullable_string AS p JOIN build_weak AS w ON p.a = w.k LEFT ANTI JOIN build_anti_string AS s ON p.b = s.k
SETTINGS enable_join_runtime_filters = 0;

-- The key of the filter on `b` divides by `b`, which is 0 where `a` is not in `build_weak`. Without runtime filters
-- the division runs after the join on `a`; the filter stays behind the one on `a`. Not sparse: a function on a sparse
-- column also runs on its default value, 0 here.
CREATE TABLE probe_div (a UInt32 CODEC(NONE), b UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', ratio_of_defaults_for_sparse_serialization = 1;
SYSTEM STOP MERGES probe_div;
INSERT INTO probe_div SELECT a, if(a >= 500, 0, b % 1000 + 1) FROM probe WHERE _part IN ('all_1_1_0', 'all_2_2_0');
INSERT INTO probe_div SELECT a, if(a >= 500, 0, b % 1000 + 1) FROM probe WHERE _part IN ('all_3_3_0', 'all_4_4_0');
CREATE TABLE build_div (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO build_div VALUES (100000), (50000);
SELECT count() FROM probe_div AS p JOIN build_weak AS w ON p.a = w.k JOIN build_div AS d ON intDiv(100000, p.b) = d.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 1, log_comment = 'mt_div_on';

-- Patch parts are applied by key columns the first step reads, so the steps keep their order.
CREATE TABLE probe_patch (a UInt32 CODEC(NONE), b UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi', enable_block_number_column = 1, enable_block_offset_column = 1,
    patch_parts_version = 'v2';
SYSTEM STOP MERGES probe_patch;
INSERT INTO probe_patch SELECT a, b FROM probe WHERE _part IN ('all_1_1_0', 'all_2_2_0');
INSERT INTO probe_patch SELECT a, b FROM probe WHERE _part IN ('all_3_3_0', 'all_4_4_0');
UPDATE probe_patch SET b = 0 WHERE a < 10 SETTINGS enable_lightweight_update = 1;
SELECT count(), sum(a), sum(b) FROM probe_patch AS p JOIN build_weak AS w ON p.a = w.k JOIN build_strong AS s ON p.b = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 1, apply_patch_parts = 1, log_comment = 'mt_patch_on';

-- A column added after the parts were written, filled by a default that reads `a`, which a step inside the run reads: a part
-- that does not store the column keeps the planned order.
CREATE TABLE probe_default (a UInt32 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES probe_default;
INSERT INTO probe_default SELECT a + 1 FROM probe WHERE _part = 'all_1_1_0';
INSERT INTO probe_default SELECT a + 1 FROM probe WHERE _part = 'all_2_2_0';
INSERT INTO probe_default SELECT a + 1 FROM probe WHERE _part = 'all_3_3_0';
ALTER TABLE probe_default ADD COLUMN b UInt64 DEFAULT a;
SELECT count(), sum(a), sum(b) FROM probe_default AS p JOIN build_weak AS w ON p.a = w.k JOIN build_strong AS s ON p.b = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 1, log_comment = 'mt_default_on';
SELECT count(), sum(a), sum(b) FROM probe_default AS p JOIN build_weak AS w ON p.a = w.k JOIN build_strong AS s ON p.b = s.k
SETTINGS enable_join_runtime_filters = 0;

-- The same with `b` dropped and added again while the drop is pending: the parts still store the old `b`, which the reader
-- fills from the default instead.
CREATE TABLE probe_readded (a UInt32 CODEC(NONE), b UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES probe_readded;
INSERT INTO probe_readded SELECT a, 999 FROM probe_default WHERE _part = 'all_1_1_0';
INSERT INTO probe_readded SELECT a, 999 FROM probe_default WHERE _part = 'all_2_2_0';
INSERT INTO probe_readded SELECT a, 999 FROM probe_default WHERE _part = 'all_3_3_0';
ALTER TABLE probe_readded DROP COLUMN b SETTINGS alter_sync = 0;
ALTER TABLE probe_readded ADD COLUMN b UInt64 DEFAULT a;
SELECT count(), sum(a), sum(b) FROM probe_readded AS p JOIN build_weak AS w ON p.a = w.k JOIN build_strong AS s ON p.b = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 1, log_comment = 'mt_readded_on';
SELECT count(), sum(a), sum(b) FROM probe_readded AS p JOIN build_weak AS w ON p.a = w.k JOIN build_strong AS s ON p.b = s.k
SETTINGS enable_join_runtime_filters = 0;

-- A pending type change: the reader converts `b` from String, which fails on the rows the filter on `a` removes.
CREATE TABLE probe_modified (a UInt32 CODEC(NONE), b String CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES probe_modified;
INSERT INTO probe_modified SELECT a, if(a < 500, toString(b), 'x') FROM probe WHERE _part = 'all_1_1_0';
INSERT INTO probe_modified SELECT a, if(a < 500, toString(b), 'x') FROM probe WHERE _part = 'all_2_2_0';
ALTER TABLE probe_modified MODIFY COLUMN b UInt64 SETTINGS alter_sync = 0;
SELECT count(), sum(a), sum(b) FROM probe_modified AS p JOIN build_weak AS w ON p.a = w.k JOIN build_strong AS s ON p.b = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 1, log_comment = 'mt_modified_on';
SELECT count(), sum(a), sum(b) FROM probe_modified AS p JOIN build_weak AS w ON p.a = w.k JOIN build_strong AS s ON p.b = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 0;

-- `a != 12345` reads `a` before the runtime filters, so the step of the filter on `a` reads no column of its own: the steps keep
-- their order.
SELECT count(), sum(a), sum(b) FROM probe AS p JOIN build_weak AS w ON p.a = w.k JOIN build_strong AS s ON p.b = s.k WHERE p.a != 12345
SETTINGS join_runtime_filter_adaptive_prewhere_order = 1, log_comment = 'mt_key_read_before_on';

-- Keys computed from `a`, which `a < 9` reads before the runtime filters: a computed key doesn't move.
CREATE TABLE probe_shared (a UInt8 CODEC(NONE), b UInt32 CODEC(NONE), c UInt64 CODEC(NONE)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
SYSTEM STOP MERGES probe_shared;
INSERT INTO probe_shared SELECT number % 10, number % 1000, intHash32(number) % 1000 FROM numbers(0, 98304);
INSERT INTO probe_shared SELECT number % 10, number % 1000, intHash32(number) % 1000 FROM numbers(98304, 16384);
SELECT count() FROM (SELECT p.b + p.a AS x, p.c + p.a AS y FROM probe_shared AS p WHERE p.a < 9) AS t
JOIN build_weak AS w ON t.x = w.k JOIN build_strong AS s ON t.y = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 1, log_comment = 'mt_shared_input_on';
SELECT count() FROM (SELECT p.b + p.a AS x, p.c + p.a AS y FROM probe_shared AS p WHERE p.a < 9) AS t
JOIN build_weak AS w ON t.x = w.k JOIN build_strong AS s ON t.y = s.k
SETTINGS enable_join_runtime_filters = 0;

SELECT 'Parquet';
INSERT INTO FUNCTION file(currentDatabase() || '_05816_0.parquet', Parquet) SELECT a, b FROM probe WHERE _part = 'all_1_1_0' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05816_1.parquet', Parquet) SELECT a, b FROM probe WHERE _part = 'all_2_2_0' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05816_2.parquet', Parquet) SELECT a, b FROM probe WHERE _part = 'all_3_3_0' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05816_3.parquet', Parquet) SELECT a, b FROM probe WHERE _part = 'all_4_4_0' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05816_4.parquet', Parquet) SELECT a, b FROM probe WHERE _part = 'all_5_5_0' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05816_5.parquet', Parquet) SELECT a, b FROM probe WHERE _part = 'all_6_6_0' SETTINGS engine_file_truncate_on_insert = 1;

SELECT count(), sum(a), sum(b) FROM file(currentDatabase() || '_05816_?.parquet', Parquet) AS p
JOIN build_weak AS w ON p.a = w.k JOIN build_strong AS s ON p.b = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 1, log_comment = 'parquet_on';
SELECT count(), sum(a), sum(b) FROM file(currentDatabase() || '_05816_?.parquet', Parquet) AS p
JOIN build_weak AS w ON p.a = w.k JOIN build_strong AS s ON p.b = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 0, log_comment = 'parquet_off';

-- Files that don't store `b`, or store it as UInt32: the reader fills `b` with the type's default or casts it row by row, so
-- the filters see in any order what the join sees. `build_div` holds no 0: the filter on `b` rejects every row without `b`.
INSERT INTO FUNCTION file(currentDatabase() || '_05816_missing_0.parquet', Parquet) SELECT a FROM probe WHERE _part = 'all_1_1_0' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05816_missing_1.parquet', Parquet) SELECT a FROM probe WHERE _part = 'all_2_2_0' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05816_missing_2.parquet', Parquet) SELECT a FROM probe WHERE _part = 'all_3_3_0' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05816_cast_0.parquet', Parquet) SELECT a, toUInt32(b) AS b FROM probe WHERE _part = 'all_1_1_0' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05816_cast_1.parquet', Parquet) SELECT a, toUInt32(b) AS b FROM probe WHERE _part = 'all_2_2_0' SETTINGS engine_file_truncate_on_insert = 1;
INSERT INTO FUNCTION file(currentDatabase() || '_05816_cast_2.parquet', Parquet) SELECT a, toUInt32(b) AS b FROM probe WHERE _part = 'all_3_3_0' SETTINGS engine_file_truncate_on_insert = 1;

SELECT count(), sum(a), sum(b) FROM file(currentDatabase() || '_05816_missing_?.parquet', Parquet, 'a UInt32, b UInt64') AS p
JOIN build_weak AS w ON p.a = w.k JOIN build_div AS d ON p.b = d.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 1, log_comment = 'parquet_missing_key_on';
SELECT count(), sum(a), sum(b) FROM file(currentDatabase() || '_05816_missing_?.parquet', Parquet, 'a UInt32, b UInt64') AS p
JOIN build_weak AS w ON p.a = w.k JOIN build_div AS d ON p.b = d.k
SETTINGS enable_join_runtime_filters = 0;
SELECT count(), sum(a), sum(b) FROM file(currentDatabase() || '_05816_cast_?.parquet', Parquet, 'a UInt32, b UInt64') AS p
JOIN build_weak AS w ON p.a = w.k JOIN build_strong AS s ON p.b = s.k
SETTINGS join_runtime_filter_adaptive_prewhere_order = 1, log_comment = 'parquet_cast_key_on';
SELECT count(), sum(a), sum(b) FROM file(currentDatabase() || '_05816_cast_?.parquet', Parquet, 'a UInt32, b UInt64') AS p
JOIN build_weak AS w ON p.a = w.k JOIN build_strong AS s ON p.b = s.k
SETTINGS enable_join_runtime_filters = 0;

SYSTEM FLUSH LOGS query_log;
SELECT 'reordered';
SELECT log_comment, ProfileEvents['RuntimeFilterPrewhereStepsReordered'] > 0
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment IN
    ('mt_on', 'mt_off', 'mt_anti_on', 'mt_anti_null_build_on', 'mt_div_on',
     'mt_patch_on', 'mt_default_on', 'mt_readded_on', 'mt_modified_on', 'mt_key_read_before_on',
     'mt_shared_input_on', 'mt_shared_step_on',
     'parquet_on', 'parquet_off', 'parquet_missing_key_on', 'parquet_cast_key_on')
ORDER BY event_time_microseconds;

DROP TABLE probe;
DROP TABLE probe_nullable;
DROP TABLE build_weak;
DROP TABLE build_weak_shifted;
DROP TABLE build_strong;
DROP TABLE build_anti;
DROP TABLE probe_nullable_string;
DROP TABLE build_anti_string;
DROP TABLE probe_div;
DROP TABLE probe_patch;
DROP TABLE probe_default;
DROP TABLE probe_readded;
DROP TABLE probe_modified;
DROP TABLE probe_shared;
DROP TABLE build_div;
