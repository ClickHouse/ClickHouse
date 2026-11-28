-- A `GLOBAL IN` is replaced by a temporary ("external") table only for a query that may run on
-- more than one server; a non-distributed query keeps the subquery as it is. See the section
-- "Distributed subqueries" of docs/reference/statements/in.mdx.
-- The `WHERE` of a `PROJECTION` is analysed when the table is created. A projection is always
-- calculated locally, so even with parallel replicas enabled the right hand side of the
-- `GLOBAL IN` - a subquery, a table name, or a joined table expression - must be kept as is:
-- a temporary table in the stored projection query would break every later `INSERT`.

DROP TABLE IF EXISTS gs_src;
DROP TABLE IF EXISTS gs_join;
DROP TABLE IF EXISTS gs_not_distributed;
DROP TABLE IF EXISTS gs_table_name;
DROP TABLE IF EXISTS gs_nested_join;
DROP TABLE IF EXISTS gs_shared_subquery;

CREATE TABLE gs_src (b UInt32) ENGINE = MergeTree ORDER BY b;
CREATE TABLE gs_join (b UInt32) ENGINE = MergeTree ORDER BY b;
INSERT INTO gs_src VALUES (1), (2);
INSERT INTO gs_join VALUES (1);

-- Not distributed: no temporary table is made, the subquery is left alone.
SET enable_parallel_replicas = 0;

CREATE TABLE gs_not_distributed
(
    a UInt32,
    b UInt32,
    PROJECTION p (SELECT a, b WHERE b GLOBAL IN (SELECT b FROM gs_src) ORDER BY b)
)
ENGINE = MergeTree ORDER BY a;

-- From here on parallel replicas are enabled, which must not affect the projections.
SET enable_parallel_replicas = 1,
    max_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    cluster_for_parallel_replicas = '',
    parallel_replicas_allow_in_with_subquery = 1,
    parallel_replicas_for_non_replicated_merge_tree = 1;

-- The right hand side is a table name and not a subquery.
CREATE TABLE gs_table_name
(
    a UInt32,
    b UInt32,
    PROJECTION p (SELECT a, b WHERE b GLOBAL IN gs_src ORDER BY b)
)
ENGINE = MergeTree ORDER BY a;

-- A `GLOBAL JOIN` inside the `GLOBAL IN` subquery: there the right hand side is a joined table
-- expression.
CREATE TABLE gs_nested_join
(
    a UInt32,
    b UInt32,
    PROJECTION p (SELECT a, b WHERE b GLOBAL IN (
        SELECT b FROM gs_src GLOBAL ALL INNER JOIN (SELECT b FROM gs_join) AS j USING (b)
    ) ORDER BY b)
)
ENGINE = MergeTree ORDER BY a;

-- Two identical `GLOBAL IN` subqueries.
CREATE TABLE gs_shared_subquery
(
    a UInt32,
    b UInt32,
    PROJECTION p (SELECT a, b
        WHERE b GLOBAL IN (SELECT b FROM gs_src) OR a GLOBAL IN (SELECT b FROM gs_src)
        ORDER BY b)
)
ENGINE = MergeTree ORDER BY a;

-- The projections must not depend on the settings they were created with: the tables keep all
-- their rows and the projections are materialised.
SET enable_parallel_replicas = 0;

INSERT INTO gs_not_distributed VALUES (1, 1), (7, 7);
INSERT INTO gs_table_name VALUES (1, 1), (7, 7);
INSERT INTO gs_nested_join VALUES (1, 1), (7, 7);
INSERT INTO gs_shared_subquery VALUES (1, 1), (7, 7);

SELECT count() FROM gs_not_distributed;
SELECT count() FROM gs_table_name;
SELECT count() FROM gs_nested_join;
SELECT count() FROM gs_shared_subquery;

SELECT a, b FROM gs_nested_join ORDER BY a;

-- One materialised projection per table proves each `WHERE` was analysed and run.
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND name = 'p' AND active;

-- And the rows of each projection are the rows its own `WHERE` selects: `gs_src` holds 1 and 2
-- and `gs_join` holds 1, so each of the four conditions keeps (1, 1) and drops (7, 7).
SELECT a, b FROM mergeTreeProjection(currentDatabase(), gs_not_distributed, p) ORDER BY a;
SELECT a, b FROM mergeTreeProjection(currentDatabase(), gs_table_name, p) ORDER BY a;
SELECT a, b FROM mergeTreeProjection(currentDatabase(), gs_nested_join, p) ORDER BY a;
SELECT a, b FROM mergeTreeProjection(currentDatabase(), gs_shared_subquery, p) ORDER BY a;

DROP TABLE gs_shared_subquery;
DROP TABLE gs_nested_join;
DROP TABLE gs_table_name;
DROP TABLE gs_not_distributed;
DROP TABLE gs_join;
DROP TABLE gs_src;
