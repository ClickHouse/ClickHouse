-- `query_plan_optimize_lazy_final` replaces a `FINAL` read with reads without `FINAL`: one collects the keys of the rows
-- that pass the filter, another reads the part ranges that need no merge. Both prune granules by a JOIN runtime filter
-- (`enable_join_runtime_filters_index_analysis`); the rows of the collected keys are then read with `FINAL` in full.
-- Seal-gated reading applies the runtime filter while reading instead of in the index analysis checked below.
SET enable_join_seal_gated_reading = 0;

-- An inserted part is deduplicated, otherwise a part of its own still needs `FINAL`.
SET optimize_on_insert = 1;

DROP TABLE IF EXISTS rf_lazy_final_overlap;
DROP TABLE IF EXISTS rf_lazy_final_single;
DROP TABLE IF EXISTS rf_lazy_final_dim;

CREATE TABLE rf_lazy_final_overlap (id UInt64, v UInt64, INDEX ix_v v TYPE minmax GRANULARITY 1) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE rf_lazy_final_single (id UInt64, v UInt64) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE rf_lazy_final_dim (id UInt64, tag String) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES rf_lazy_final_overlap;
INSERT INTO rf_lazy_final_overlap SELECT number, number FROM numbers(2000);
-- A second version of every tenth row and of the last one: the parts overlap over the whole key range, so every row needs `FINAL`.
INSERT INTO rf_lazy_final_overlap SELECT if(number = 199, 1999, number * 10) AS id, id + 1000000 FROM numbers(200);
-- One part: no row needs `FINAL`.
INSERT INTO rf_lazy_final_single SELECT number, number FROM numbers(2000);
INSERT INTO rf_lazy_final_dim SELECT number, if(number < 64, 'hot', 'cold') FROM numbers(2000);

SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_if_final = 1;
SET join_runtime_filter_min_probe_rows = 0;
-- The table read with `FINAL` has to stay the probe side.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;
SET make_distributed_plan = 0;
SET query_plan_optimize_lazy_final = 1;
-- Never fall back to the `FINAL` read, which prunes on its own.
SET min_filtered_ratio_for_lazy_final = 0;

-- Overlapping parts: the keys are collected by a read that prunes the granules of the cold rows.
SELECT 'overlap, lazy final plan', count() > 0
FROM (EXPLAIN SELECT count(), sum(f.v) FROM rf_lazy_final_overlap AS f FINAL INNER JOIN rf_lazy_final_dim AS d ON f.id = d.id WHERE d.tag = 'hot')
WHERE explain LIKE '%InputSelector%';

SELECT 'overlap', count(), sum(f.v)
FROM rf_lazy_final_overlap AS f FINAL INNER JOIN rf_lazy_final_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05830_overlap';

-- `v` is not a sorting key column, and the read that collects the keys prunes by it, unlike the `FINAL` read. The first
-- version of every tenth hot row matches and its latest version does not: the latest ones are still read and win.
SELECT 'overlap_non_key', count(), sum(f.id)
FROM rf_lazy_final_overlap AS f FINAL INNER JOIN rf_lazy_final_dim AS d ON f.v = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05830_overlap_non_key';

-- The same for the global row index: the build side finds the latest versions of two rows, and their granules are kept.
SELECT 'overlap_global_row_index', count(), sum(f.id)
FROM rf_lazy_final_overlap AS f FINAL
INNER JOIN (SELECT _part_starting_offset + _part_offset AS g FROM rf_lazy_final_overlap WHERE v IN (1000000, 1000010)) AS d
ON f._part_starting_offset + f._part_offset = d.g
SETTINGS log_comment = '05830_overlap_global_row_index';

-- One part: the whole `FINAL` read is replaced with a read without `FINAL`, which prunes.
SELECT 'single, no final read in plan', count() = 0
FROM (EXPLAIN SELECT count(), sum(f.v) FROM rf_lazy_final_single AS f FINAL INNER JOIN rf_lazy_final_dim AS d ON f.id = d.id WHERE d.tag = 'hot')
WHERE explain LIKE '%FINAL: 1%' OR explain LIKE '%InputSelector%';

SELECT 'single', count(), sum(f.v)
FROM rf_lazy_final_single AS f FINAL INNER JOIN rf_lazy_final_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05830_single';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['RuntimeFilterGranulesConsidered'] > 0 AS granules_considered,
    ProfileEvents['RuntimeFilterGranulesDropped'] > 0 AS granules_dropped
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05830_overlap', '05830_overlap_non_key', '05830_overlap_global_row_index', '05830_single')
    AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR
ORDER BY log_comment;

DROP TABLE rf_lazy_final_overlap;
DROP TABLE rf_lazy_final_single;
DROP TABLE rf_lazy_final_dim;
