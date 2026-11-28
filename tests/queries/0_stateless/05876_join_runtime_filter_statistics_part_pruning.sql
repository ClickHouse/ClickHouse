-- JOIN runtime filter read-time part pruning using column statistics.

SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_statistics_for_part_pruning = 1;
SET materialize_statistics_on_insert = 1;
SET join_runtime_filter_min_probe_rows = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 0;
SET use_query_condition_cache = 0;
SET merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability = 0;
SET min_marks_per_part_to_load_statistics = 0;

DROP ROW POLICY IF EXISTS rf_stats_policy ON rf_stats_fact;
DROP TABLE IF EXISTS rf_stats_fact;
DROP TABLE IF EXISTS rf_stats_cold;
DROP TABLE IF EXISTS rf_stats_dim;
DROP TABLE IF EXISTS rf_stats_comp_pk;
DROP TABLE IF EXISTS rf_stats_pk_before_stats;
DROP TABLE IF EXISTS rf_stats_nullable;
DROP TABLE IF EXISTS rf_stats_cast_float;
DROP TABLE IF EXISTS rf_stats_deprecated_minmax;
DROP TABLE IF EXISTS rf_stats_final;
DROP TABLE IF EXISTS rf_stats_mut;
DROP TABLE IF EXISTS rf_stats_patch;
DROP TABLE IF EXISTS rf_stats_modify;
DROP TABLE IF EXISTS rf_stats_drop_col;
DROP TABLE IF EXISTS rf_stats_drop_done;

-- 10 parts on non-PK `k`, no skip index.
CREATE TABLE rf_stats_fact (p UInt8, id UInt64, k UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 16, auto_statistics_types = 'basic', prewarm_statistics_cache = 0;

INSERT INTO rf_stats_fact
SELECT intDiv(number, 1000), number, number, number FROM numbers(10000);

CREATE TABLE rf_stats_dim (k UInt64, tag String) ENGINE = MergeTree ORDER BY k;
INSERT INTO rf_stats_dim SELECT number, if(number >= 5000 AND number < 5050, 'hot', 'cold') FROM numbers(10000);

-- Exact IN-set versus statistics pruning off.
SELECT 'exact_on', count(), sum(f.v)
FROM rf_stats_fact AS f INNER JOIN rf_stats_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
SETTINGS log_comment = 'rf_stats_05876_exact_on';

SELECT 'exact_off', count(), sum(f.v)
FROM rf_stats_fact AS f INNER JOIN rf_stats_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
SETTINGS use_statistics_for_part_pruning = 0, log_comment = 'rf_stats_05876_exact_off';

-- Range envelope when the exact set overflows.
SELECT 'range_on', count(), sum(f.v)
FROM rf_stats_fact AS f INNER JOIN rf_stats_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
SETTINGS join_runtime_filter_exact_values_limit = 10, log_comment = 'rf_stats_05876_range_on';

-- Disjoint keys drop every granule; an empty build returns 0.
SELECT 'disjoint_all_pruned', count()
FROM rf_stats_fact AS f INNER JOIN (SELECT toUInt64(999999) AS k) AS d ON f.k = d.k
SETTINGS log_comment = 'rf_stats_05876_disjoint_all_pruned';

SELECT 'empty_build', count()
FROM rf_stats_fact AS f LEFT SEMI JOIN (SELECT k FROM rf_stats_dim WHERE tag = 'none') AS d ON f.k = d.k;

-- Refiner on and off read the same rows.
SELECT 'refiner_on', count(), sum(f.v)
FROM rf_stats_fact AS f INNER JOIN rf_stats_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
SETTINGS use_indexes_refiner_in_read_pools = 1, log_comment = 'rf_stats_05876_refiner_on';

SELECT 'refiner_off', count(), sum(f.v)
FROM rf_stats_fact AS f INNER JOIN rf_stats_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
SETTINGS use_indexes_refiner_in_read_pools = 0, log_comment = 'rf_stats_05876_refiner_off';

-- Cold start. `use_statistics = 0` keeps planning from loading estimates into memory.
CREATE TABLE rf_stats_cold (p UInt8, id UInt64, k UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 16, auto_statistics_types = 'basic', prewarm_statistics_cache = 0;

INSERT INTO rf_stats_cold
SELECT intDiv(number, 1000), number, number, number FROM numbers(5000);

DETACH TABLE rf_stats_cold;
ATTACH TABLE rf_stats_cold;

SELECT 'cold_high_threshold', count(), sum(f.v)
FROM rf_stats_cold AS f INNER JOIN (SELECT toUInt64(100) AS k) AS d ON f.k = d.k
SETTINGS use_statistics = 0, min_marks_per_part_to_load_statistics = 100000, log_comment = 'rf_stats_05876_cold_high_threshold';

SELECT 'cold_zero_threshold', count(), sum(f.v)
FROM rf_stats_cold AS f INNER JOIN (SELECT toUInt64(100) AS k) AS d ON f.k = d.k
SETTINGS use_statistics = 0, min_marks_per_part_to_load_statistics = 0, log_comment = 'rf_stats_05876_cold_zero_threshold';

-- Non-leading PK column `k` of `ORDER BY (a, k)`.
CREATE TABLE rf_stats_comp_pk (p UInt8, a UInt64, k UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY (a, k)
SETTINGS index_granularity = 16, auto_statistics_types = 'basic';

INSERT INTO rf_stats_comp_pk
SELECT intDiv(number, 1000), number % 100, number, number FROM numbers(10000);

SELECT 'comp_pk_non_leading', count(), sum(f.v)
FROM rf_stats_comp_pk AS f INNER JOIN rf_stats_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
SETTINGS log_comment = 'rf_stats_05876_comp_pk';

-- The primary key drops the whole part before statistics on the non-leading key are loaded.
CREATE TABLE rf_stats_pk_before_stats (a UInt64, k UInt64, v UInt64)
ENGINE = MergeTree ORDER BY (a, k)
SETTINGS index_granularity = 16, auto_statistics_types = 'basic', prewarm_statistics_cache = 0;

INSERT INTO rf_stats_pk_before_stats SELECT number, number, number FROM numbers(10000);

DETACH TABLE rf_stats_pk_before_stats;
ATTACH TABLE rf_stats_pk_before_stats;

SELECT 'pk_before_stats', count()
FROM rf_stats_pk_before_stats AS f
INNER JOIN (SELECT toUInt64(999999) AS a, toUInt64(5005) AS k) AS d ON f.a = d.a AND f.k = d.k
SETTINGS use_statistics = 0, min_marks_per_part_to_load_statistics = 0, log_comment = 'rf_stats_05876_pk_before_stats';

-- Nullable keys: the all-NULL part is pruned.
CREATE TABLE rf_stats_nullable (p UInt8, id UInt64, kn Nullable(UInt64), ks Nullable(String), v UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 16, auto_statistics_types = 'basic';

INSERT INTO rf_stats_nullable SELECT 0, number, NULL, NULL, number FROM numbers(1000);
INSERT INTO rf_stats_nullable SELECT 1, number + 1000, number + 1000, concat('s', toString(number + 1000)), number FROM numbers(1000);
INSERT INTO rf_stats_nullable SELECT 2, number + 2000, if(number % 2 = 0, NULL, number + 5000), if(number % 2 = 0, NULL, concat('s', toString(number + 5000))), number FROM numbers(1000);

SELECT 'nullable_num_on', count(), sum(f.v)
FROM rf_stats_nullable AS f INNER JOIN rf_stats_dim AS d ON f.kn = d.k
WHERE d.tag = 'hot'
SETTINGS log_comment = 'rf_stats_05876_nullable_num';

SELECT 'nullable_num_off', count(), sum(f.v)
FROM rf_stats_nullable AS f INNER JOIN rf_stats_dim AS d ON f.kn = d.k
WHERE d.tag = 'hot'
SETTINGS use_statistics_for_part_pruning = 0;

SELECT 'nullable_str_on', count(), sum(f.v)
FROM rf_stats_nullable AS f INNER JOIN (SELECT concat('s', toString(k)) AS ks FROM rf_stats_dim WHERE tag = 'hot') AS d ON f.ks = d.ks
SETTINGS log_comment = 'rf_stats_05876_nullable_str';

SELECT 'nullable_str_off', count(), sum(f.v)
FROM rf_stats_nullable AS f INNER JOIN (SELECT concat('s', toString(k)) AS ks FROM rf_stats_dim WHERE tag = 'hot') AS d ON f.ks = d.ks
SETTINGS use_statistics_for_part_pruning = 0;

-- `UInt32`/`UInt64` cast, and `Float64` with `NaN` and signed zero.
CREATE TABLE rf_stats_cast_float (p UInt8, id UInt64, k32 UInt32, kf Float64, v UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 16, auto_statistics_types = 'basic';

INSERT INTO rf_stats_cast_float SELECT 0, number, number, if(number = 0, nan, if(number = 1, -0.0, toFloat64(number))), number FROM numbers(1000);
INSERT INTO rf_stats_cast_float SELECT 1, number + 5000, number + 5000, if(number = 0, nan, toFloat64(number + 5000)), number FROM numbers(1000);

SELECT 'cast_u32_u64', count(), sum(f.v)
FROM rf_stats_cast_float AS f INNER JOIN rf_stats_dim AS d ON f.k32 = d.k
WHERE d.tag = 'hot'
SETTINGS log_comment = 'rf_stats_05876_cast_u32';

SELECT 'float_nan_zero_parity',
    (SELECT count() FROM rf_stats_cast_float AS f INNER JOIN (SELECT arrayJoin([0.0, 5005.0, nan]) AS kf) AS d ON f.kf = d.kf SETTINGS use_statistics_for_part_pruning = 1) =
    (SELECT count() FROM rf_stats_cast_float AS f INNER JOIN (SELECT arrayJoin([0.0, 5005.0, nan]) AS kf) AS d ON f.kf = d.kf SETTINGS use_statistics_for_part_pruning = 0);

-- Deprecated `minmax` statistics also prune parts.
CREATE TABLE rf_stats_deprecated_minmax (p UInt8, id UInt64, k UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 16, auto_statistics_types = 'minmax';

INSERT INTO rf_stats_deprecated_minmax SELECT intDiv(number, 1000), number, number, number FROM numbers(10000);

SELECT 'deprecated_minmax', count(), sum(f.v)
FROM rf_stats_deprecated_minmax AS f INNER JOIN rf_stats_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
SETTINGS log_comment = 'rf_stats_05876_deprecated_minmax';

-- Safety gates: `FINAL`, pending mutation, patch part, killed `MODIFY COLUMN`, row policy, `DROP COLUMN`.
CREATE TABLE rf_stats_final (id UInt64, ver UInt64, k UInt64)
ENGINE = ReplacingMergeTree(ver) ORDER BY id
SETTINGS index_granularity = 16, auto_statistics_types = 'basic';
SYSTEM STOP MERGES rf_stats_final;
INSERT INTO rf_stats_final SELECT number, 1, 5005 FROM numbers(1000);
INSERT INTO rf_stats_final SELECT number, 2, 99999 FROM numbers(1000);
INSERT INTO rf_stats_final SELECT number + 1000, 1, 99999 FROM numbers(1000);
INSERT INTO rf_stats_final SELECT number + 1000, 2, 5005 FROM numbers(1000);

SELECT 'final_on', count()
FROM rf_stats_final AS f FINAL INNER JOIN rf_stats_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
SETTINGS log_comment = 'rf_stats_05876_final';

SELECT 'final_off', count()
FROM rf_stats_final AS f FINAL INNER JOIN rf_stats_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
SETTINGS use_statistics_for_part_pruning = 0;

CREATE TABLE rf_stats_mut (p UInt8, id UInt64, k UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 16, auto_statistics_types = 'basic';
SYSTEM STOP MERGES rf_stats_mut;
INSERT INTO rf_stats_mut SELECT intDiv(number, 1000), number, number, number FROM numbers(10000);
ALTER TABLE rf_stats_mut UPDATE k = 5005 WHERE id < 50 SETTINGS mutations_sync = 0;

SELECT 'pending_mutation', count()
FROM rf_stats_mut AS f INNER JOIN rf_stats_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
SETTINGS apply_mutations_on_fly = 1, log_comment = 'rf_stats_05876_pending_mut';

CREATE TABLE rf_stats_patch (p UInt8, id UInt64, k UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 16, auto_statistics_types = 'basic', enable_block_number_column = 1, enable_block_offset_column = 1;
SYSTEM STOP MERGES rf_stats_patch;
INSERT INTO rf_stats_patch SELECT intDiv(number, 1000), number, number, number FROM numbers(10000);
UPDATE rf_stats_patch SET k = 5005 WHERE id < 50;

SELECT 'patch_part', count()
FROM rf_stats_patch AS f INNER JOIN rf_stats_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
SETTINGS apply_patch_parts = 1, log_comment = 'rf_stats_05876_patch_part';

CREATE TABLE rf_stats_modify (p UInt8, id UInt64, k UInt64, v UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 16, auto_statistics_types = 'basic';
SYSTEM STOP MERGES rf_stats_modify;
INSERT INTO rf_stats_modify SELECT 0, number, 260, number FROM numbers(1000);
INSERT INTO rf_stats_modify SELECT 1, number + 1000, 50, number FROM numbers(1000);
ALTER TABLE rf_stats_modify MODIFY COLUMN k UInt8 SETTINGS mutations_sync = 0, alter_sync = 0;
KILL MUTATION WHERE database = currentDatabase() AND table = 'rf_stats_modify' SYNC FORMAT Null;

SELECT 'killed_modify_parity',
    (SELECT count() FROM rf_stats_modify AS f INNER JOIN (SELECT toUInt8(4) AS k) AS d ON f.k = d.k SETTINGS use_statistics_for_part_pruning = 1),
    (SELECT count() FROM rf_stats_modify AS f INNER JOIN (SELECT toUInt8(4) AS k) AS d ON f.k = d.k SETTINGS use_statistics_for_part_pruning = 0);

-- Pending `DROP` + `ADD` of `k` must not prune.
CREATE TABLE rf_stats_drop_col (p UInt8, id UInt64, k UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 16, auto_statistics_types = 'basic';
SYSTEM STOP MERGES rf_stats_drop_col;
INSERT INTO rf_stats_drop_col SELECT intDiv(number, 1000), number, number FROM numbers(10000);
ALTER TABLE rf_stats_drop_col DROP COLUMN k SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE rf_stats_drop_col ADD COLUMN k UInt64 DEFAULT 5005;

SELECT 'pending_drop_add_col', count()
FROM rf_stats_drop_col AS f INNER JOIN rf_stats_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
SETTINGS log_comment = 'rf_stats_05876_pending_drop_add_col';

-- A finished `DROP` of another column still prunes `k`.
CREATE TABLE rf_stats_drop_done (p UInt8, id UInt64, k UInt64, unused UInt64)
ENGINE = MergeTree PARTITION BY p ORDER BY id
SETTINGS index_granularity = 16, auto_statistics_types = 'basic';
INSERT INTO rf_stats_drop_done SELECT intDiv(number, 1000), number, number, number FROM numbers(10000);
ALTER TABLE rf_stats_drop_done DROP COLUMN unused SETTINGS mutations_sync = 2;

SELECT 'finished_drop_col', count()
FROM rf_stats_drop_done AS f INNER JOIN rf_stats_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
SETTINGS log_comment = 'rf_stats_05876_finished_drop_col';

-- Row policy disables statistics pruning.
CREATE ROW POLICY rf_stats_policy ON rf_stats_fact FOR SELECT USING p >= 0 TO CURRENT_USER;

SELECT 'row_policy', count(), sum(f.v)
FROM rf_stats_fact AS f INNER JOIN rf_stats_dim AS d ON f.k = d.k
WHERE d.tag = 'hot'
SETTINGS log_comment = 'rf_stats_05876_row_policy';

DROP ROW POLICY rf_stats_policy ON rf_stats_fact;

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['RuntimeFilterIndexAnalysisReads'] > 0 AS reads_enabled,
    ProfileEvents['RuntimeFilterGranulesConsidered'] > 0 AS considered,
    ProfileEvents['RuntimeFilterGranulesDropped'] > 0 AS dropped
FROM system.query_log
WHERE current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND log_comment LIKE 'rf\_stats\_05876\_%'
    AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR
ORDER BY log_comment;

-- `exact_on` reads fewer rows than `exact_off`; disjoint drops every granule; refiner on/off match.
SELECT
    'read_rows_checks',
    (SELECT argMax(read_rows, event_time_microseconds) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'rf_stats_05876_exact_on')
        < (SELECT argMax(read_rows, event_time_microseconds) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'rf_stats_05876_exact_off'),
    (SELECT argMax(ProfileEvents['RuntimeFilterGranulesConsidered'] > 0 AND ProfileEvents['RuntimeFilterGranulesDropped'] = ProfileEvents['RuntimeFilterGranulesConsidered'], event_time_microseconds) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'rf_stats_05876_disjoint_all_pruned'),
    (SELECT argMax(read_rows, event_time_microseconds) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'rf_stats_05876_refiner_on')
        = (SELECT argMax(read_rows, event_time_microseconds) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'rf_stats_05876_refiner_off');

SELECT 'pk_before_stats_no_load',
    (SELECT argMax(ProfileEvents['StatisticsCacheMisses'] = 0, event_time_microseconds) FROM system.query_log WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'rf_stats_05876_pk_before_stats');

DROP TABLE rf_stats_fact;
DROP TABLE rf_stats_cold;
DROP TABLE rf_stats_dim;
DROP TABLE rf_stats_comp_pk;
DROP TABLE rf_stats_pk_before_stats;
DROP TABLE rf_stats_nullable;
DROP TABLE rf_stats_cast_float;
DROP TABLE rf_stats_deprecated_minmax;
DROP TABLE rf_stats_final;
DROP TABLE rf_stats_mut;
DROP TABLE rf_stats_patch;
DROP TABLE rf_stats_modify;
DROP TABLE rf_stats_drop_col;
DROP TABLE rf_stats_drop_done;
