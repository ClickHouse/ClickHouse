-- Regression test: analysing the `WHERE` of a `PROJECTION` with parallel replicas enabled replaced the right hand
-- side of `GLOBAL IN` in the stored projection query with a temporary table `_data_...`, so every later `INSERT`
-- failed with `UNKNOWN_IDENTIFIER`. A projection is always calculated locally, so the query must be kept as is.

DROP TABLE IF EXISTS pgi_src;
DROP TABLE IF EXISTS pgi_create;
DROP TABLE IF EXISTS pgi_alter;

CREATE TABLE pgi_src (b UInt32) ENGINE = MergeTree ORDER BY b;
INSERT INTO pgi_src VALUES (1), (2);

SET enable_parallel_replicas = 1,
    max_parallel_replicas = 2,
    automatic_parallel_replicas_mode = 0,
    cluster_for_parallel_replicas = 'test_cluster_one_shard_three_replicas_localhost',
    parallel_replicas_for_non_replicated_merge_tree = 1;

CREATE TABLE pgi_create
(
    a UInt32,
    b UInt32,
    PROJECTION p (SELECT a, b WHERE b GLOBAL IN (SELECT b FROM pgi_src) ORDER BY b)
)
ENGINE = MergeTree ORDER BY a;

-- `ALTER` builds the stored projection with the settings of the session.
CREATE TABLE pgi_alter (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
ALTER TABLE pgi_alter ADD PROJECTION p (SELECT a, b WHERE b GLOBAL IN pgi_src ORDER BY b);

INSERT INTO pgi_create VALUES (1, 1), (7, 7);
INSERT INTO pgi_alter VALUES (1, 1), (7, 7);

SET enable_parallel_replicas = 0;

INSERT INTO pgi_create VALUES (2, 2), (8, 8);
INSERT INTO pgi_alter VALUES (2, 2), (8, 8);

SELECT a, b FROM mergeTreeProjection(currentDatabase(), pgi_create, p) ORDER BY a;
SELECT '-';
SELECT a, b FROM mergeTreeProjection(currentDatabase(), pgi_alter, p) ORDER BY a;

DROP TABLE pgi_alter;
DROP TABLE pgi_create;
DROP TABLE pgi_src;
