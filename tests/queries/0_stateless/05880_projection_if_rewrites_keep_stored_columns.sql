-- `if` with a constant condition and a chain of `if` are changed before the types are found, so the stored columns stay as they were

DROP TABLE IF EXISTS t_projection_if_rewrites;

CREATE TABLE t_projection_if_rewrites (a UInt8, b UInt64)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

ALTER TABLE t_projection_if_rewrites ADD PROJECTION p_types (SELECT if(1, a, b), count(), sum(if(1, a, b)) GROUP BY if(1, a, b));
ALTER TABLE t_projection_if_rewrites ADD PROJECTION p_null (SELECT if(NULL, a, b), count() GROUP BY if(NULL, a, b));
ALTER TABLE t_projection_if_rewrites ADD PROJECTION p_cast (SELECT if(CAST(0, 'UInt8'), a, b + 1), count() GROUP BY if(CAST(0, 'UInt8'), a, b + 1));
ALTER TABLE t_projection_if_rewrites ADD PROJECTION p_multi_if (SELECT multiIf(1, b, b + 1), count() GROUP BY multiIf(1, b, b + 1));
ALTER TABLE t_projection_if_rewrites ADD PROJECTION p_literal (SELECT a, max(if(1, 1, a)) GROUP BY a, if(1, 1, a));
SET optimize_if_chain_to_multiif = 1;
ALTER TABLE t_projection_if_rewrites ADD PROJECTION p_chain (SELECT if(a > 1, 2, 3), if(b > 5, 1, if(a > 1, 2, 3)), count() GROUP BY if(a > 1, 2, 3), if(b > 5, 1, if(a > 1, 2, 3)));
SET optimize_if_chain_to_multiif = 0;

INSERT INTO t_projection_if_rewrites VALUES (1, 10), (1, 10), (2, 20);

SELECT name, sorting_key FROM system.projections
WHERE database = currentDatabase() AND table = 't_projection_if_rewrites'
ORDER BY name;

SELECT name, column, type FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_if_rewrites' AND active
ORDER BY name, column_position;

SELECT b, finalizeAggregation(`count()`) FROM mergeTreeProjection(currentDatabase(), 't_projection_if_rewrites', 'p_null') ORDER BY b;

DROP TABLE t_projection_if_rewrites;
