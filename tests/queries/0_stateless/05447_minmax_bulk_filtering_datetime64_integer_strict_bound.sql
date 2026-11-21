-- Bulk minmax filtering of a strict comparison of a sub-second `DateTime64` key with an integer constant.
-- `KeyCondition` steps the `Range` bound of such a comparison to a `Decimal256`, which must not reach the direct comparison.

DROP TABLE IF EXISTS t_minmax;

CREATE TABLE t_minmax (time DateTime64(9, 'UTC'), INDEX idx_time time TYPE minmax GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t_minmax VALUES (0::Decimal(9, 2)::DateTime64(9, 'UTC')), (0.01::Decimal(9, 2)::DateTime64(9, 'UTC')), (0.99::Decimal(9, 2)::DateTime64(9, 'UTC')), (1::Decimal(9, 2)::DateTime64(9, 'UTC')), (1.01::Decimal(9, 2)::DateTime64(9, 'UTC'));

SET use_minmax_index_bulk_filtering = 1, force_data_skipping_indices = 'idx_time';

SELECT '>', count() FROM t_minmax WHERE time > 0;
SELECT '<', count() FROM t_minmax WHERE time < 1;
SELECT 'reversed', count() FROM t_minmax WHERE 0 < time;
SELECT 'NOT <=', count() FROM t_minmax WHERE NOT (time <= 0);
SELECT 'pruned', count() FROM t_minmax WHERE time > 2;

DROP TABLE t_minmax;
