-- Tags: no-parallel, no-random-settings, no-random-merge-tree-settings, no-parallel-replicas
-- no-parallel: Messes with the global partial aggregate cache.
-- no-random-* / no-parallel-replicas: settings randomization changes whether the cache is used at all.

-- Each case warms the partial aggregate cache and repeats the identical query after a change that the per-part
-- key does not represent, or with a read that does not feed the aggregation. The second result must be correct.

SYSTEM DROP AGGREGATE CACHE;

SET use_partial_aggregate_cache = 1;
SET optimize_aggregation_in_order = 0;
SET max_rows_to_group_by = 0;
SET group_by_overflow_mode = 'throw';

SELECT '--- UNION ALL with a raw arm';
DROP TABLE IF EXISTS t_pac_union;
CREATE TABLE t_pac_union (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_pac_union VALUES (1, 10), (2, 20);
SELECT * FROM (SELECT k, sum(v) AS s FROM t_pac_union GROUP BY k UNION ALL SELECT k, v FROM t_pac_union) ORDER BY ALL;
SELECT * FROM (SELECT k, sum(v) AS s FROM t_pac_union GROUP BY k UNION ALL SELECT k, v FROM t_pac_union) ORDER BY ALL;
DROP TABLE t_pac_union;

SELECT '--- IN table';
DROP TABLE IF EXISTS t_pac_in;
DROP TABLE IF EXISTS t_pac_dim;
CREATE TABLE t_pac_in (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_pac_dim (k UInt64) ENGINE = Memory;
INSERT INTO t_pac_in VALUES (1, 10), (2, 20);
INSERT INTO t_pac_dim VALUES (1);
SELECT sum(v) FROM t_pac_in WHERE k IN t_pac_dim;
INSERT INTO t_pac_dim VALUES (2);
SELECT sum(v) FROM t_pac_in WHERE k IN t_pac_dim;
SELECT sum(v) FROM t_pac_in WHERE k NOT IN t_pac_dim;
TRUNCATE TABLE t_pac_dim;
SELECT sum(v) FROM t_pac_in WHERE k NOT IN t_pac_dim;
DROP TABLE t_pac_in;
DROP TABLE t_pac_dim;

SELECT '--- pending DROP COLUMN followed by ADD COLUMN';
DROP TABLE IF EXISTS t_pac_alter;
CREATE TABLE t_pac_alter (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
SYSTEM STOP MERGES t_pac_alter;
INSERT INTO t_pac_alter VALUES (1, 10);
SELECT k, sum(v) FROM t_pac_alter GROUP BY k;
ALTER TABLE t_pac_alter DROP COLUMN v SETTINGS mutations_sync = 0, alter_sync = 0;
ALTER TABLE t_pac_alter ADD COLUMN v UInt64 SETTINGS mutations_sync = 0, alter_sync = 0;
SELECT k, sum(v) FROM t_pac_alter GROUP BY k;
DROP TABLE t_pac_alter;
