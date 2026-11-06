-- A projection is calculated from the block being written, on the server that writes the part, so the
-- analysis of its query does not replace a `GLOBAL IN` with a temporary ("external") table, as it is done
-- for a query that may run on more than one server (see the section "Distributed subqueries" of
-- docs/reference/statements/in.mdx), whatever the parallel replicas settings: those of the server, with
-- which `CREATE TABLE` builds the table, and those of the query, with which `ALTER TABLE ... ADD PROJECTION`
-- builds the projection. With parallel replicas an `IN` with a subquery is handled as a `GLOBAL IN`.

DROP TABLE IF EXISTS gs_src;
DROP TABLE IF EXISTS gs_join;
DROP TABLE IF EXISTS gs_not_distributed;
DROP TABLE IF EXISTS gs_table_name;
DROP TABLE IF EXISTS gs_nested_join;
DROP TABLE IF EXISTS gs_shared_subquery;
DROP TABLE IF EXISTS gs_alter;

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

-- From here on parallel replicas are enabled, which must not change how the projections are analysed.
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

-- A `GLOBAL JOIN` inside the `GLOBAL IN` subquery.
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

-- `ALTER TABLE ... ADD PROJECTION` analyses the projection with the settings of the query, so with
-- parallel replicas enabled here, for a `GLOBAL IN` and for an `IN`.
CREATE TABLE gs_alter (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
ALTER TABLE gs_alter ADD PROJECTION p_global (SELECT a, b WHERE b GLOBAL IN (SELECT b FROM gs_src) ORDER BY b);
ALTER TABLE gs_alter ADD PROJECTION p_in (SELECT a, b WHERE b IN (SELECT b FROM gs_src) ORDER BY b);

-- The tables keep all their rows and the projections are materialised.
SET enable_parallel_replicas = 0;

INSERT INTO gs_not_distributed VALUES (1, 1), (7, 7);
INSERT INTO gs_table_name VALUES (1, 1), (7, 7);
INSERT INTO gs_nested_join VALUES (1, 1), (7, 7);
INSERT INTO gs_shared_subquery VALUES (1, 1), (7, 7);
INSERT INTO gs_alter VALUES (1, 1), (7, 7);

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

-- The same for the projections added by `ALTER TABLE`.
SELECT a, b FROM mergeTreeProjection(currentDatabase(), gs_alter, p_global) ORDER BY a;
SELECT a, b FROM mergeTreeProjection(currentDatabase(), gs_alter, p_in) ORDER BY a;

DROP TABLE gs_alter;
DROP TABLE gs_shared_subquery;
DROP TABLE gs_nested_join;
DROP TABLE gs_table_name;
DROP TABLE gs_not_distributed;
DROP TABLE gs_join;
DROP TABLE gs_src;
