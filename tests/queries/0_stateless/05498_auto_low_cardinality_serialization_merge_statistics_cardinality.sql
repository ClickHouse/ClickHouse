-- A row-reducing merge rebuilds the statistics, but automatic `LowCardinality` serialization estimates
-- the cardinality before they are rebuilt. The estimate published for the result part must reflect the
-- rebuilt statistics, not the provisional value that was looked up while choosing the encoding.

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP TABLE IF EXISTS t_auto_lc_merge_stats_cardinality;

CREATE TABLE t_auto_lc_merge_stats_cardinality
(
    id UInt64,
    s String STATISTICS(uniq_v2)
)
ENGINE = ReplacingMergeTree
ORDER BY id
SETTINGS
    max_uniq_number_for_low_cardinality = 100,
    min_bytes_for_wide_part = 0;

SYSTEM STOP MERGES t_auto_lc_merge_stats_cardinality;

INSERT INTO t_auto_lc_merge_stats_cardinality SELECT number, 'v_' || toString(number % 10) FROM numbers(1000);
INSERT INTO t_auto_lc_merge_stats_cardinality SELECT number + 500, 'v_' || toString(number % 10) FROM numbers(1000);

SYSTEM START MERGES t_auto_lc_merge_stats_cardinality;
OPTIMIZE TABLE t_auto_lc_merge_stats_cardinality FINAL;

SELECT count(), uniqExact(s) FROM t_auto_lc_merge_stats_cardinality;

SELECT column, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_merge_stats_cardinality' AND active AND column = 's';

DETACH TABLE t_auto_lc_merge_stats_cardinality;
ATTACH TABLE t_auto_lc_merge_stats_cardinality;

SELECT column, estimates.cardinality
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_auto_lc_merge_stats_cardinality' AND active AND column = 's';

DROP TABLE t_auto_lc_merge_stats_cardinality;
