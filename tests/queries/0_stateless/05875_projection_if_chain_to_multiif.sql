-- a query with `optimize_if_chain_to_multiif` reads a projection that was added with the setting

DROP TABLE IF EXISTS t_projection_if_chain;

CREATE TABLE t_projection_if_chain (a Int32)
ENGINE = MergeTree ORDER BY tuple()
SETTINGS materialize_projections_on_insert = 1;

SET optimize_if_chain_to_multiif = 1;
ALTER TABLE t_projection_if_chain ADD PROJECTION p (SELECT if(a > 0, 1, if(a > 5, 2, 3)), count() GROUP BY if(a > 0, 1, if(a > 5, 2, 3)));
SET optimize_if_chain_to_multiif = 0;

INSERT INTO t_projection_if_chain VALUES (1), (2), (-3);

SELECT column FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_if_chain' AND active
ORDER BY column_position;

SELECT if(a > 0, 1, if(a > 5, 2, 3)) AS k, count() FROM t_projection_if_chain GROUP BY k ORDER BY k
SETTINGS optimize_if_chain_to_multiif = 1, force_optimize_projection = 1;

DROP TABLE t_projection_if_chain;
