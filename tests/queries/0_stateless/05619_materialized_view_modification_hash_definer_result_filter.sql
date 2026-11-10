-- Tags: no-parallel-replicas
-- no-parallel-replicas: the replicas of `cluster_for_parallel_replicas` in that configuration are not local, so the `MergeTree` modification hash is `NULL` by design.
-- A materialized view pushes its read into the target table without a planner of its own, so the
-- `additional_result_filter` of its definer is never applied to the read. A change of that filter must
-- neither move `modification_hash` of the view (a `REFRESH ... IF CHANGED APPEND` reading the view would
-- append the unchanged rows again) nor make it `NULL`.

DROP TABLE IF EXISTS mv_05619;
DROP TABLE IF EXISTS tgt_05619;
DROP TABLE IF EXISTS src_05619;
DROP USER IF EXISTS u_05619;

CREATE USER u_05619;
GRANT SELECT, INSERT ON *.* TO u_05619;

CREATE TABLE src_05619 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE tgt_05619 (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE MATERIALIZED VIEW mv_05619 TO tgt_05619 DEFINER = u_05619 SQL SECURITY DEFINER AS SELECT x FROM src_05619;
INSERT INTO tgt_05619 VALUES (1);

CREATE TEMPORARY TABLE hash_before AS
    SELECT modification_hash AS h FROM system.tables WHERE database = currentDatabase() AND name = 'mv_05619';
SELECT h IS NOT NULL FROM hash_before;

ALTER USER u_05619 SETTINGS additional_result_filter = 'x > 1';

SELECT modification_hash IS NOT NULL, modification_hash = (SELECT h FROM hash_before)
FROM system.tables WHERE database = currentDatabase() AND name = 'mv_05619';

-- The filter of the definer does not apply to the read.
SELECT x FROM mv_05619;

DROP TABLE mv_05619;
DROP TABLE tgt_05619;
DROP TABLE src_05619;
DROP USER u_05619;
