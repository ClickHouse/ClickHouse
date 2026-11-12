-- Lightweight DELETE in 'rebuild' mode rebuilds the projections a part has and does not add one the part never had.

DROP TABLE IF EXISTS t_lwd_rebuild_absent_proj;

CREATE TABLE t_lwd_rebuild_absent_proj (g UInt8, k UInt32, v UInt32, PROJECTION p1 (SELECT g, count() GROUP BY g))
ENGINE = MergeTree PARTITION BY g ORDER BY k
SETTINGS index_granularity = 16, lightweight_mutation_projection_mode = 'rebuild';

INSERT INTO t_lwd_rebuild_absent_proj SELECT 1, number, number FROM numbers(100);
ALTER TABLE t_lwd_rebuild_absent_proj ADD PROJECTION p2 (SELECT k, v ORDER BY v);
INSERT INTO t_lwd_rebuild_absent_proj SELECT 2, number, number FROM numbers(100);

DELETE FROM t_lwd_rebuild_absent_proj WHERE k < 50 SETTINGS lightweight_deletes_sync = 2;

SELECT partition, name FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_lwd_rebuild_absent_proj' AND active ORDER BY partition, name;
SELECT g, count() FROM t_lwd_rebuild_absent_proj GROUP BY g ORDER BY g SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'p1', enable_parallel_replicas = 0, optimize_aggregation_in_order = 0;
SELECT count() FROM t_lwd_rebuild_absent_proj WHERE v = 60 SETTINGS optimize_use_projections = 1, force_optimize_projection_name = 'p2', enable_parallel_replicas = 0;

DROP TABLE t_lwd_rebuild_absent_proj;
