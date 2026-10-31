-- A projection is calculated without resolving its positional arguments again: creating it resolves them in
-- the stored query, whatever `enable_positional_arguments_for_projections` the inserting query has.

-- Built without positional arguments: `1` is a constant key, not the first column.
SET enable_positional_arguments_for_projections = 0;

DROP TABLE IF EXISTS t_projection_positional_off;

CREATE TABLE t_projection_positional_off (c0 UInt64, PROJECTION p (SELECT 'a' GROUP BY 'a', 1))
ENGINE = MergeTree ORDER BY c0
SETTINGS materialize_projections_on_insert = 1;
SYSTEM STOP MERGES t_projection_positional_off;

INSERT INTO t_projection_positional_off VALUES (1);
INSERT INTO t_projection_positional_off SETTINGS enable_positional_arguments_for_projections = 1 VALUES (2);

SELECT 'built without positional arguments, inserted with them';
SELECT arraySort(groupArray(DISTINCT column)) FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_positional_off' AND name = 'p' AND active;
SELECT (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_projection_positional_off' AND active)
    = (SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_projection_positional_off' AND name = 'p' AND active);
SELECT count() FROM t_projection_positional_off;

DROP TABLE t_projection_positional_off;

-- Built with positional arguments: `1` is `b`.
SET enable_positional_arguments_for_projections = 1;

DROP TABLE IF EXISTS t_projection_positional_on;

CREATE TABLE t_projection_positional_on (a UInt64, b String)
ENGINE = MergeTree ORDER BY a
SETTINGS materialize_projections_on_insert = 1;

ALTER TABLE t_projection_positional_on ADD PROJECTION p (SELECT b, sum(a) GROUP BY 1);
SYSTEM STOP MERGES t_projection_positional_on;

INSERT INTO t_projection_positional_on SETTINGS enable_positional_arguments_for_projections = 0
SELECT number % 7, toString(number % 3) FROM numbers(20);

SELECT 'built with positional arguments, inserted without them';
SELECT arraySort(groupArray(DISTINCT column)) FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_positional_on' AND name = 'p' AND active;
SELECT (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_projection_positional_on' AND active)
    = (SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_projection_positional_on' AND name = 'p' AND active);

SELECT b, sum(a) FROM t_projection_positional_on GROUP BY b ORDER BY b SETTINGS optimize_use_projections = 0;

-- The projection optimization does not match an aggregate projection when the aggregation is done in
-- order, and it is not supported with parallel replicas, so pin the settings which the test
-- randomization may flip - the test forces the projection.
SET optimize_use_projections = 1, optimize_aggregation_in_order = 0, parallel_replicas_local_plan = 1, parallel_replicas_support_projection = 1;

SELECT b, sum(a) FROM t_projection_positional_on GROUP BY b ORDER BY b SETTINGS force_optimize_projection = 1;

DROP TABLE t_projection_positional_on;

-- Created with positional arguments that resolve to an integer literal.
SET enable_positional_arguments_for_projections = 1;

DROP TABLE IF EXISTS t_projection_positional_literal;

CREATE TABLE t_projection_positional_literal (c0 UInt64)
ENGINE = MergeTree ORDER BY c0
SETTINGS materialize_projections_on_insert = 1;

ALTER TABLE t_projection_positional_literal ADD PROJECTION p (SELECT 2, 1 GROUP BY 1);
SYSTEM STOP MERGES t_projection_positional_literal;

INSERT INTO t_projection_positional_literal SETTINGS enable_positional_arguments_for_projections = 0 VALUES (1);
INSERT INTO t_projection_positional_literal SETTINGS enable_positional_arguments_for_projections = 1 VALUES (2);

SELECT 'built with positional arguments resolved to a literal';
SELECT arraySort(groupArray(DISTINCT column)) FROM system.projection_parts_columns
WHERE database = currentDatabase() AND table = 't_projection_positional_literal' AND name = 'p' AND active;
SELECT (SELECT count() FROM system.parts WHERE database = currentDatabase() AND table = 't_projection_positional_literal' AND active)
    = (SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 't_projection_positional_literal' AND name = 'p' AND active);
SELECT count() FROM t_projection_positional_literal;

DROP TABLE t_projection_positional_literal;
