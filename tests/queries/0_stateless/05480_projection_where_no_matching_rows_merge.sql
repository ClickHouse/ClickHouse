-- A part in which no row passes the WHERE of a projection has an empty projection part,
-- so it merges with the parts that have rows in the projection (issue #128062).
-- Background merges are disabled to keep the parts separate until OPTIMIZE.

DROP TABLE IF EXISTS t_insert;
DROP TABLE IF EXISTS t_materialize;
DROP TABLE IF EXISTS t_delete;

-- INSERT
CREATE TABLE t_insert
(
    k UInt64,
    kind String,
    PROJECTION p_errors (SELECT k, kind WHERE kind = 'error' ORDER BY kind, k),
    PROJECTION p_count (SELECT kind, count() WHERE kind = 'error' GROUP BY kind)
)
ENGINE = MergeTree ORDER BY k
SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_insert VALUES (1, 'error'), (2, 'info');
INSERT INTO t_insert VALUES (3, 'info'), (4, 'info');
INSERT INTO t_insert VALUES (5, 'error'), (6, 'info');
INSERT INTO t_insert VALUES (7, 'info'), (8, 'info');

SELECT 'insert', count(), countIf(arraySort(projections) != ['p_count', 'p_errors'])
FROM system.parts WHERE database = currentDatabase() AND table = 't_insert' AND active;

SELECT k FROM t_insert WHERE kind = 'error' ORDER BY k
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, optimize_read_in_order = 0, enable_parallel_replicas = 0;

OPTIMIZE TABLE t_insert FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT 'insert, merged', count(), sum(rows), arraySort(any(projections))
FROM system.parts WHERE database = currentDatabase() AND table = 't_insert' AND active;
SELECT name, rows FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_insert' AND active ORDER BY name;

-- MATERIALIZE PROJECTION
CREATE TABLE t_materialize (k UInt64, kind String) ENGINE = MergeTree ORDER BY k
SETTINGS max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_materialize VALUES (1, 'error'), (2, 'info');
INSERT INTO t_materialize VALUES (3, 'info'), (4, 'info');
ALTER TABLE t_materialize ADD PROJECTION p_errors (SELECT k, kind WHERE kind = 'error' ORDER BY kind, k);
ALTER TABLE t_materialize ADD PROJECTION p_count (SELECT kind, count() WHERE kind = 'error' GROUP BY kind);
ALTER TABLE t_materialize MATERIALIZE PROJECTION p_errors SETTINGS mutations_sync = 2;
ALTER TABLE t_materialize MATERIALIZE PROJECTION p_count SETTINGS mutations_sync = 2;

SELECT 'materialize', count(), countIf(arraySort(projections) != ['p_count', 'p_errors'])
FROM system.parts WHERE database = currentDatabase() AND table = 't_materialize' AND active;
-- the empty projection parts have the same columns as the non-empty ones
SELECT 'materialize, column sets', name, count(DISTINCT columns)
FROM
(
    SELECT parent_name, name, arraySort(groupArray(column)) AS columns
    FROM system.projection_parts_columns
    WHERE database = currentDatabase() AND table = 't_materialize' AND active
    GROUP BY parent_name, name
)
GROUP BY name ORDER BY name;

OPTIMIZE TABLE t_materialize FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT 'materialize, merged', count(), sum(rows), arraySort(any(projections))
FROM system.parts WHERE database = currentDatabase() AND table = 't_materialize' AND active;

-- Lightweight DELETE of the only matching row, then a merge that rebuilds the projection
CREATE TABLE t_delete
(
    k UInt64,
    kind String,
    PROJECTION p_errors (SELECT k, kind WHERE kind = 'error' ORDER BY kind, k)
)
ENGINE = MergeTree ORDER BY k
SETTINGS lightweight_mutation_projection_mode = 'rebuild', max_bytes_to_merge_at_max_space_in_pool = 1;

INSERT INTO t_delete VALUES (1, 'error'), (2, 'info');
INSERT INTO t_delete VALUES (3, 'info'), (4, 'info');
DELETE FROM t_delete WHERE k = 1 SETTINGS lightweight_deletes_sync = 2;

SELECT 'delete', count(), countIf(projections != ['p_errors'])
FROM system.parts WHERE database = currentDatabase() AND table = 't_delete' AND active;

OPTIMIZE TABLE t_delete FINAL SETTINGS optimize_throw_if_noop = 1;
INSERT INTO t_delete VALUES (5, 'error'), (6, 'info');
OPTIMIZE TABLE t_delete FINAL SETTINGS optimize_throw_if_noop = 1;

SELECT 'delete, merged', count(), sum(rows), any(projections)
FROM system.parts WHERE database = currentDatabase() AND table = 't_delete' AND active;
SELECT k FROM t_delete WHERE kind = 'error' ORDER BY k;

DROP TABLE t_insert;
DROP TABLE t_materialize;
DROP TABLE t_delete;
