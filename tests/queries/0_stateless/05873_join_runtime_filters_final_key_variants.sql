-- With `FINAL` on the probe side, `enable_join_runtime_filters_index_analysis` prunes granules only by a join key
-- that is a sorting key column itself. Here the sorting key or the join key is an expression of the column,
-- or the column is in the sorting key but not in the primary key.

-- The lazy `FINAL` read below needs parts that are deduplicated on insert.
SET optimize_on_insert = 1;

DROP TABLE IF EXISTS rf_final_expr_key;
DROP TABLE IF EXISTS rf_final_plain_key;
DROP TABLE IF EXISTS rf_final_non_pk_key;
DROP TABLE IF EXISTS rf_final_variants_dim;

CREATE TABLE rf_final_expr_key (id UInt64, INDEX ix_id id TYPE set(0) GRANULARITY 1) ENGINE = ReplacingMergeTree ORDER BY id % 1000 SETTINGS index_granularity = 16;
CREATE TABLE rf_final_plain_key (id UInt32, v UInt64) ENGINE = ReplacingMergeTree ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE rf_final_non_pk_key (a UInt64, id UInt64, v UInt64, INDEX ix_id id TYPE minmax GRANULARITY 1) ENGINE = ReplacingMergeTree ORDER BY (a, id) PRIMARY KEY a SETTINGS index_granularity = 16;
CREATE TABLE rf_final_variants_dim (id UInt64, tag String) ENGINE = MergeTree ORDER BY id;
SYSTEM STOP MERGES rf_final_expr_key;
SYSTEM STOP MERGES rf_final_plain_key;
SYSTEM STOP MERGES rf_final_non_pk_key;

INSERT INTO rf_final_expr_key SELECT number FROM numbers(1000);
-- The latest version of the first 32 rows: the same `id % 1000`, and an `id` that no hot row has.
INSERT INTO rf_final_expr_key SELECT number + 1000 FROM numbers(32);
INSERT INTO rf_final_plain_key SELECT number, number FROM numbers(2000);
INSERT INTO rf_final_plain_key SELECT number, number * 10 FROM numbers(64);
INSERT INTO rf_final_non_pk_key SELECT number, number, number FROM numbers(2000);
INSERT INTO rf_final_non_pk_key SELECT number, number, number * 10 FROM numbers(64);
INSERT INTO rf_final_variants_dim SELECT number, if(number < 64, 'hot', 'cold') FROM numbers(2000);

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
SET query_plan_optimize_lazy_final = 0;
-- Never fall back from lazy `FINAL` to the `FINAL` read.
SET min_filtered_ratio_for_lazy_final = 0;

-- The sorting key is an expression of the join key column. Control: without `FINAL` the read prunes by `ix_id`.
SELECT 'expr_key_no_final', count(), sum(f.id)
FROM rf_final_expr_key AS f INNER JOIN rf_final_variants_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05873_expr_key_no_final';

-- `id` differs between the versions of a row, so it does not prune: the granule that `ix_id` would skip holds
-- the latest versions of the first 32 rows, and without it their first versions would win and match.
SELECT 'expr_key', count(), sum(f.id)
FROM rf_final_expr_key AS f FINAL INNER JOIN rf_final_variants_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05873_expr_key';

-- Lazy `FINAL` prunes by `id` in the read that collects the keys, and reads all versions of the collected keys.
SELECT 'expr_key_lazy', count(), sum(f.id)
FROM rf_final_expr_key AS f FINAL INNER JOIN rf_final_variants_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS query_plan_optimize_lazy_final = 1, log_comment = '05873_expr_key_lazy';

-- The join key is the sorting key expression: only a join key that is a column prunes, with or without `FINAL`.
SELECT 'expr_key_same_expr', count(), sum(f.id)
FROM rf_final_expr_key AS f FINAL INNER JOIN rf_final_variants_dim AS d ON f.id % 1000 = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05873_expr_key_same_expr';

-- The join key is an expression of the sorting key column.
SELECT 'plain_key_expr', count(), sum(f.v)
FROM rf_final_plain_key AS f FINAL INNER JOIN rf_final_variants_dim AS d ON f.id % 1000 = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05873_plain_key_expr';

-- The join key is the sorting key column under a cast to the type of the other side: it prunes.
SELECT 'plain_key_cast', count(), sum(f.v)
FROM rf_final_plain_key AS f FINAL INNER JOIN rf_final_variants_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05873_plain_key_cast';

-- A sorting key column outside of the primary key prunes by its skip index, which `use_skip_indexes_if_final` gates.
SELECT 'non_pk_key', count(), sum(f.v)
FROM rf_final_non_pk_key AS f FINAL INNER JOIN rf_final_variants_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS log_comment = '05873_non_pk_key';

SELECT 'non_pk_key_no_skip_index', count(), sum(f.v)
FROM rf_final_non_pk_key AS f FINAL INNER JOIN rf_final_variants_dim AS d ON f.id = d.id
WHERE d.tag = 'hot'
SETTINGS use_skip_indexes_if_final = 0, log_comment = '05873_non_pk_key_no_skip_index';

SYSTEM FLUSH LOGS query_log;

SELECT
    log_comment,
    ProfileEvents['RuntimeFilterGranulesConsidered'] > 0 AS granules_considered,
    ProfileEvents['RuntimeFilterGranulesDropped'] > 0 AS granules_dropped
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish'
    AND log_comment IN ('05873_expr_key_no_final', '05873_expr_key', '05873_expr_key_lazy', '05873_expr_key_same_expr',
        '05873_plain_key_expr', '05873_plain_key_cast', '05873_non_pk_key', '05873_non_pk_key_no_skip_index')
    AND event_date >= yesterday() AND event_time > now() - INTERVAL 1 HOUR
ORDER BY log_comment;

DROP TABLE rf_final_expr_key;
DROP TABLE rf_final_plain_key;
DROP TABLE rf_final_non_pk_key;
DROP TABLE rf_final_variants_dim;
