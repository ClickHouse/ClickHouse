-- A table read lazily over a join can still use a projection: to filter its parts and granules, or to read from it
-- instead of the table, when the lazy read can read the rows of the projection parts - by the offsets of the parent
-- part the projection has (`_part_offset`), or from the projection parts, which then have the lazily read columns.
-- Each query is compared with the feature off, and the plan shows the projection and the columns read lazily.

SET enable_analyzer = 1;
SET query_plan_optimize_lazy_materialization = 1;
SET query_plan_max_limit_for_lazy_materialization = 1000;
SET optimize_move_to_prewhere = 0;
SET query_plan_join_swap_table = 0;
SET query_plan_optimize_join_order_limit = 0;
SET enable_join_runtime_filters = 0;
SET enable_parallel_replicas = 0;
SET optimize_use_projections = 1;
SET optimize_use_projection_filtering = 1;

DROP TABLE IF EXISTS t_filter;
DROP TABLE IF EXISTS t_parent_offset;
DROP TABLE IF EXISTS t_payload;
DROP TABLE IF EXISTS t_partial;
DROP TABLE IF EXISTS r;

CREATE TABLE r (k UInt64, v String) ENGINE = MergeTree ORDER BY k;
INSERT INTO r SELECT number, concat('v', toString(number)) FROM numbers(30000);

-- The projection has neither the columns of the query nor the payload: it only filters the read.
CREATE TABLE t_filter (k UInt64, id String, payload String, PROJECTION p (SELECT _part_offset ORDER BY id))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
-- The projection has the columns the main read needs and the offsets of the parent part, but not the payload.
CREATE TABLE t_parent_offset (k UInt64, id String, payload String, PROJECTION p (SELECT k, id, _part_offset ORDER BY id))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
-- The projection has every column, the payload included.
CREATE TABLE t_payload (k UInt64, id String, payload String, PROJECTION p (SELECT * ORDER BY id))
ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;

-- Several parts each, so that the projection keeps some of them only.
INSERT INTO t_filter SELECT number, concat('id', toString(number % 1000)), concat('payload', toString(number)) FROM numbers(10000);
INSERT INTO t_filter SELECT number, concat('id', toString(number % 1000)), concat('payload', toString(number)) FROM numbers(10000, 10000);
INSERT INTO t_filter SELECT number, concat('id', toString(1000 + number % 1000)), concat('payload', toString(number)) FROM numbers(20000, 10000);
INSERT INTO t_parent_offset SELECT * FROM t_filter;
INSERT INTO t_payload SELECT * FROM t_filter;

SELECT '-- the projection filters the read';
SELECT t.k, t.id, t.payload, r.v FROM t_filter AS t JOIN r ON t.k = r.k WHERE t.id IN ('id5', 'id7') ORDER BY t.k DESC LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT t.k, t.id, t.payload, r.v FROM t_filter AS t JOIN r ON t.k = r.k WHERE t.id IN ('id5', 'id7') ORDER BY t.k DESC LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT extract(explain, 'Lazily read columns: .*') FROM (EXPLAIN actions = 1
    SELECT t.k, t.id, t.payload, r.v FROM t_filter AS t JOIN r ON t.k = r.k WHERE t.id IN ('id5', 'id7') ORDER BY t.k DESC LIMIT 5
    SETTINGS query_plan_lazy_materialization_for_join = 1)
WHERE explain LIKE '%Lazily read columns%';

SELECT '-- the projection is read with the offsets of the parent part';
SELECT t.k, t.id, t.payload, r.v FROM t_parent_offset AS t JOIN r ON t.k = r.k WHERE t.id IN ('id5', 'id7') ORDER BY t.k DESC LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT t.k, t.id, t.payload, r.v FROM t_parent_offset AS t JOIN r ON t.k = r.k WHERE t.id IN ('id5', 'id7') ORDER BY t.k DESC LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT extract(explain, '(ReadFromMergeTree .*|Lazily read columns: .*)') AS e FROM (EXPLAIN actions = 1
    SELECT t.k, t.id, t.payload, r.v FROM t_parent_offset AS t JOIN r ON t.k = r.k WHERE t.id IN ('id5', 'id7') ORDER BY t.k DESC LIMIT 5
    SETTINGS query_plan_lazy_materialization_for_join = 1)
WHERE e != '';

SELECT '-- the projection is read, and the payload from it';
SELECT t.k, t.id, t.payload, r.v FROM t_payload AS t JOIN r ON t.k = r.k WHERE t.id IN ('id5', 'id7') ORDER BY t.k DESC LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT t.k, t.id, t.payload, r.v FROM t_payload AS t JOIN r ON t.k = r.k WHERE t.id IN ('id5', 'id7') ORDER BY t.k DESC LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT extract(explain, '(ReadFromMergeTree .*|Lazily read columns: .*)') AS e FROM (EXPLAIN actions = 1
    SELECT t.k, t.id, t.payload, r.v FROM t_payload AS t JOIN r ON t.k = r.k WHERE t.id IN ('id5', 'id7') ORDER BY t.k DESC LIMIT 5
    SETTINGS query_plan_lazy_materialization_for_join = 1)
WHERE e != '';

SELECT '-- the projection serves some of the parts';
CREATE TABLE t_partial (k UInt64, id String, payload String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 128;
SYSTEM STOP MERGES t_partial;
INSERT INTO t_partial SELECT * FROM t_filter WHERE k < 10000;
ALTER TABLE t_partial ADD PROJECTION p (SELECT * ORDER BY id);
INSERT INTO t_partial SELECT * FROM t_filter WHERE k >= 10000;
SELECT t.k, t.id, t.payload, r.v FROM t_partial AS t JOIN r ON t.k = r.k WHERE t.id IN ('id5', 'id7') ORDER BY t.k LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT t.k, t.id, t.payload, r.v FROM t_partial AS t JOIN r ON t.k = r.k WHERE t.id IN ('id5', 'id7') ORDER BY t.k LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT t.k, t.id, t.payload, r.v FROM t_partial AS t JOIN r ON t.k = r.k WHERE t.id IN ('id5', 'id7') ORDER BY t.k DESC LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 1;
SELECT t.k, t.id, t.payload, r.v FROM t_partial AS t JOIN r ON t.k = r.k WHERE t.id IN ('id5', 'id7') ORDER BY t.k DESC LIMIT 5
SETTINGS query_plan_lazy_materialization_for_join = 0;
SELECT extract(explain, '(ReadFromMergeTree .*|Lazily read columns: .*|Union)') AS e FROM (EXPLAIN actions = 1
    SELECT t.k, t.id, t.payload, r.v FROM t_partial AS t JOIN r ON t.k = r.k WHERE t.id IN ('id5', 'id7') ORDER BY t.k DESC LIMIT 5
    SETTINGS query_plan_lazy_materialization_for_join = 1)
WHERE e != '';

DROP TABLE t_filter;
DROP TABLE t_parent_offset;
DROP TABLE t_payload;
DROP TABLE t_partial;
DROP TABLE r;
