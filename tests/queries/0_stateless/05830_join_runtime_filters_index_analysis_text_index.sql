-- `enable_join_runtime_filters_index_analysis` prunes probe-side granules by a `text` index on the join key,
-- if the index has the `array` tokenizer and no preprocessor or postprocessor.

SET enable_join_runtime_filters = 1;
SET join_runtime_filter_min_probe_rows = 0;
-- The asserted granule pruning depends on which side builds the runtime filter.
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_join_swap_table = 'false';
SET enable_join_runtime_filters_index_analysis = 1;
SET use_skip_indexes_on_data_read = 1;
-- The per-query counters would depend on the coordinator's assignment of granules to replicas.
SET enable_parallel_replicas = 0;

DROP TABLE IF EXISTS fact_array;
DROP TABLE IF EXISTS fact_split;
DROP TABLE IF EXISTS fact_preprocessor;
DROP TABLE IF EXISTS fact_added_index;
DROP TABLE IF EXISTS dim;

CREATE TABLE fact_array (id UInt64, s String, v UInt64, INDEX idx_s s TYPE text(tokenizer = 'array'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE fact_split (id UInt64, s String, v UInt64, INDEX idx_s s TYPE text(tokenizer = 'splitByNonAlpha'))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
CREATE TABLE fact_preprocessor (id UInt64, s String, v UInt64, INDEX idx_s s TYPE text(tokenizer = 'array', preprocessor = lower(s)))
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
-- The first part is written before the index is added, so it has no index file and is read in full.
CREATE TABLE fact_added_index (id UInt64, s String, v UInt64)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 16;
-- A merge would build the index for the first part too.
SYSTEM STOP MERGES fact_added_index;
CREATE TABLE dim (s String, tag String) ENGINE = MergeTree ORDER BY s;

INSERT INTO fact_array SELECT number, concat('k', leftPad(toString(number), 5, '0')), number FROM numbers(2000);
INSERT INTO fact_split SELECT * FROM fact_array;
INSERT INTO fact_preprocessor SELECT * FROM fact_array;
INSERT INTO fact_added_index SELECT * FROM fact_array WHERE id < 1000;
ALTER TABLE fact_added_index ADD INDEX idx_s s TYPE text(tokenizer = 'array');
INSERT INTO fact_added_index SELECT * FROM fact_array WHERE id >= 1000;
-- The hot keys are in both parts of `fact_added_index`.
INSERT INTO dim SELECT concat('k', leftPad(toString(number), 5, '0')), if(number < 32 OR number BETWEEN 1000 AND 1031, 'hot', 'cold') FROM numbers(2000);

SELECT sum(f.v) FROM fact_array AS f INNER JOIN dim AS d ON f.s = d.s WHERE d.tag = 'hot'
SETTINGS log_comment = '05830_array';

-- Not pruned: every value is split into several tokens.
SELECT sum(f.v) FROM fact_split AS f INNER JOIN dim AS d ON f.s = d.s WHERE d.tag = 'hot'
SETTINGS log_comment = '05830_split';

-- Not pruned: the index does not store the values themselves.
SELECT sum(f.v) FROM fact_preprocessor AS f INNER JOIN dim AS d ON f.s = d.s WHERE d.tag = 'hot'
SETTINGS log_comment = '05830_preprocessor';

-- Not pruned: the runtime filter does not keep the exact values, and a `text` index cannot use a range.
SELECT sum(f.v) FROM fact_array AS f INNER JOIN dim AS d ON f.s = d.s WHERE d.tag = 'hot'
SETTINGS log_comment = '05830_no_exact_values', join_runtime_filter_exact_values_limit = 10;

-- Not pruned: the runtime filter keeps the exact values, but more than `join_runtime_filter_exact_values_limit / 100`.
SELECT sum(f.v) FROM fact_array AS f INNER JOIN dim AS d ON f.s = d.s WHERE d.tag = 'hot'
SETTINGS log_comment = '05830_over_cap', join_runtime_filter_exact_values_limit = 6000;

-- Not pruned: the index is ignored by name.
SELECT sum(f.v) FROM fact_array AS f INNER JOIN dim AS d ON f.s = d.s WHERE d.tag = 'hot'
SETTINGS log_comment = '05830_ignored', ignore_data_skipping_indices = 'idx_s';

-- Only the part with the index file is pruned.
SELECT sum(f.v) FROM fact_added_index AS f INNER JOIN dim AS d ON f.s = d.s WHERE d.tag = 'hot'
SETTINGS log_comment = '05830_added_index';

-- The same results without the pruning.
SELECT sum(f.v) FROM fact_array AS f INNER JOIN dim AS d ON f.s = d.s WHERE d.tag = 'hot'
SETTINGS enable_join_runtime_filters_index_analysis = 0;
SELECT sum(f.v) FROM fact_added_index AS f INNER JOIN dim AS d ON f.s = d.s WHERE d.tag = 'hot'
SETTINGS enable_join_runtime_filters_index_analysis = 0;

SYSTEM FLUSH LOGS query_log;

-- The granule counts depend on the (randomized) granularity of the parts, so only their relation is checked.
-- The hot keys fill a few granules, so most of the granules of an indexed part are dropped. The part of
-- `fact_added_index` without the index file holds half of the rows and is not pruned.
SELECT
    log_comment,
    multiIf(
        log_comment = '05830_array', considered > 0 AND dropped * 4 > considered * 3,
        log_comment = '05830_added_index', dropped > 0 AND dropped * 2 <= considered,
        dropped = 0)
FROM
(
    SELECT
        log_comment,
        argMax(ProfileEvents['RuntimeFilterGranulesConsidered'], event_time) AS considered,
        argMax(ProfileEvents['RuntimeFilterGranulesDropped'], event_time) AS dropped
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('05830_array', '05830_split', '05830_preprocessor', '05830_no_exact_values', '05830_over_cap', '05830_ignored', '05830_added_index')
    GROUP BY log_comment
)
ORDER BY log_comment;

DROP TABLE fact_array;
DROP TABLE fact_split;
DROP TABLE fact_preprocessor;
DROP TABLE fact_added_index;
DROP TABLE dim;
