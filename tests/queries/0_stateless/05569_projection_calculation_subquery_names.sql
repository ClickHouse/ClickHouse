-- A projection column with a subquery (`IN`, `EXISTS`) is named after the AST of the subquery. When the
-- projection is calculated by a query (a normal projection with `WHERE`, or an aggregate projection), the
-- calculated columns must still get the names its metadata describes.

-- Disable force_primary_key_reverse_order: The output includes `_part_offset`, which depends on the sort order
SET force_primary_key_reverse_order = 0;

DROP TABLE IF EXISTS t_subquery_src;
DROP TABLE IF EXISTS t_subquery_join;
DROP TABLE IF EXISTS t_subquery_normal;
DROP TABLE IF EXISTS t_subquery_global;
DROP TABLE IF EXISTS t_subquery_aggregate;
DROP TABLE IF EXISTS t_subquery_materialize;

CREATE TABLE t_subquery_src (b UInt32) ENGINE = MergeTree ORDER BY b;
CREATE TABLE t_subquery_join (b UInt32) ENGINE = MergeTree ORDER BY b;
INSERT INTO t_subquery_src VALUES (1), (2);
INSERT INTO t_subquery_join VALUES (1);

SELECT 'normal projection with WHERE';
CREATE TABLE t_subquery_normal
(
    a UInt32,
    b UInt32,
    PROJECTION p
    (
        SELECT a, b, b IN (SELECT b FROM t_subquery_src), (b NOT IN (SELECT 1)) + 1, exists(SELECT b FROM t_subquery_src WHERE b = 2)
        WHERE a > 0
        ORDER BY b
    )
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_subquery_normal VALUES (1, 1), (2, 2), (7, 7);
-- The projection is built again from its stored definition, as after a restart.
DETACH TABLE t_subquery_normal;
ATTACH TABLE t_subquery_normal;
INSERT INTO t_subquery_normal VALUES (8, 1);
SELECT * FROM mergeTreeProjection(currentDatabase(), t_subquery_normal, p) ORDER BY a;

SELECT 'GLOBAL IN with a GLOBAL JOIN';
CREATE TABLE t_subquery_global
(
    a UInt32,
    b UInt32,
    PROJECTION p
    (
        SELECT b GLOBAL IN (SELECT b FROM t_subquery_src GLOBAL ALL INNER JOIN (SELECT b FROM t_subquery_join) AS j USING (b)), a, b, _part_offset
        WHERE (b GLOBAL IN (SELECT b FROM t_subquery_src)) OR (a GLOBAL IN (SELECT b FROM t_subquery_src))
        ORDER BY b
    )
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_subquery_global VALUES (1, 1), (7, 7);
SELECT * FROM mergeTreeProjection(currentDatabase(), t_subquery_global, p) ORDER BY a;

SELECT 'aggregate projection';
CREATE TABLE t_subquery_aggregate
(
    a UInt32,
    b UInt32,
    PROJECTION p (SELECT count(), countIf(a IN (SELECT b FROM t_subquery_src)) GROUP BY b IN (SELECT b FROM t_subquery_src))
)
ENGINE = MergeTree ORDER BY a;

INSERT INTO t_subquery_aggregate VALUES (1, 1), (2, 2), (7, 7);
SELECT COLUMNS('^in\\('), COLUMNS('^count') APPLY finalizeAggregation
FROM mergeTreeProjection(currentDatabase(), t_subquery_aggregate, p) ORDER BY ALL;

SELECT 'MATERIALIZE PROJECTION';
CREATE TABLE t_subquery_materialize (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_subquery_materialize VALUES (1, 1), (7, 7);
ALTER TABLE t_subquery_materialize ADD PROJECTION p2 (SELECT a, b IN (SELECT b FROM t_subquery_src) WHERE a > 0 ORDER BY a);
ALTER TABLE t_subquery_materialize MATERIALIZE PROJECTION p2 SETTINGS mutations_sync = 2;
SELECT * FROM mergeTreeProjection(currentDatabase(), t_subquery_materialize, p2) ORDER BY a;

SELECT 'INSERT with rewrite_in_to_join';
SET allow_correlated_subqueries = 1, rewrite_in_to_join = 1;
INSERT INTO t_subquery_normal VALUES (9, 2);
SET rewrite_in_to_join = 0;
SELECT * FROM mergeTreeProjection(currentDatabase(), t_subquery_normal, p) ORDER BY a;

DROP TABLE t_subquery_materialize;
DROP TABLE t_subquery_aggregate;
DROP TABLE t_subquery_global;
DROP TABLE t_subquery_normal;
DROP TABLE t_subquery_join;
DROP TABLE t_subquery_src;
