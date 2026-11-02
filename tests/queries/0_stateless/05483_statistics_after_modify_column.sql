-- A part keeps the statistics of the column type it stores. After `ALTER TABLE ... MODIFY COLUMN` this type may
-- differ from the current one: for good after a metadata-only conversion, and until a mutation rewrites the part
-- otherwise, or for a part that was detached during the `ALTER`. A read converts the values to the current type,
-- so the statistics are used only if the conversion keeps the values.

DROP TABLE IF EXISTS t_metadata_only;
DROP TABLE IF EXISTS t_converted;
DROP TABLE IF EXISTS t_stale_first;
DROP TABLE IF EXISTS t_stale_second;
DROP TABLE IF EXISTS t_inserted_after;

-- Pinned (randomized in CI): the statistics must be built and used.
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET mutations_sync = 2;

SET enable_analyzer = 1;
SET explain_query_plan_default = 'legacy';
SET enable_parallel_replicas = 0;
SET param__internal_cascades_cluster_node_count = 4;

-- `Enum8` -> `Int8` is metadata-only: the part keeps its `Enum8` column and statistics, which still describe the
-- 4 distinct values.
CREATE TABLE t_metadata_only (k UInt64, e Enum8('a' = 1, 'b' = 2, 'c' = 3, 'd' = 4)) ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_metadata_only SELECT number, number % 4 + 1 FROM numbers(10000);

ALTER TABLE t_metadata_only MODIFY COLUMN e Int8;
SELECT 'metadata-only, stored type:', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_metadata_only' AND active AND column = 'e';

-- The outer probe queries turn distributed planning off because they read the plan through `viewExplain`, which
-- Cascades rejects; the explained queries turn it back on.
SELECT 'metadata-only, group by:', countIf(explain LIKE '%Aggregating%(rows: ~4.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT e, count() FROM t_metadata_only GROUP BY e
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT e, count() FROM t_metadata_only GROUP BY e ORDER BY e;

-- `DateTime` -> `Date` changes the values. The part is detached during the `ALTER`, so it keeps the `DateTime`
-- column and its statistics, which count 10000 distinct seconds, all of them on one day. They are rejected, and
-- the key without a known number of distinct values falls back to 10% of the rows.
CREATE TABLE t_converted (k UInt64, d DateTime('UTC')) ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_converted SELECT number, toDateTime('2024-01-01 00:00:00', 'UTC') + number FROM numbers(10000);

SELECT 'converted, before the alter:', countIf(explain LIKE '%Aggregating%(rows: ~10000.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT d, count() FROM t_converted GROUP BY d
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

ALTER TABLE t_converted DETACH PARTITION tuple();
ALTER TABLE t_converted MODIFY COLUMN d Date;
ALTER TABLE t_converted ATTACH PARTITION tuple();
SELECT 'converted, stored type:', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_converted' AND active AND column = 'd';

SELECT 'converted, group by:', countIf(explain LIKE '%Aggregating%(rows: ~1000.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT d, count() FROM t_converted GROUP BY d
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT d, count() FROM t_converted GROUP BY d ORDER BY d;

-- The parts may store the column with different types. Their statistics cannot be merged, so only the parts of
-- one type contribute: those of the current type, whatever the part order.

-- `Int32` -> `Int64`: a partition is detached during the `ALTER` and keeps the `Int32` column and statistics. The
-- rewritten partition has 10 distinct values, the stale one 1000. The stale partition is the first part of
-- `t_stale_first` and the second part of `t_stale_second`.
CREATE TABLE t_stale_first (p UInt8, k Int32) ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_stale_first SELECT 1, number % 1000 FROM numbers(10000);
INSERT INTO t_stale_first SELECT 2, number % 10 FROM numbers(10000);
ALTER TABLE t_stale_first DETACH PARTITION 1;
ALTER TABLE t_stale_first MODIFY COLUMN k Int64;
ALTER TABLE t_stale_first ATTACH PARTITION 1;

CREATE TABLE t_stale_second (p UInt8, k Int32) ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_stale_second SELECT 1, number % 10 FROM numbers(10000);
INSERT INTO t_stale_second SELECT 2, number % 1000 FROM numbers(10000);
ALTER TABLE t_stale_second DETACH PARTITION 2;
ALTER TABLE t_stale_second MODIFY COLUMN k Int64;
ALTER TABLE t_stale_second ATTACH PARTITION 2;

SELECT 'stored type:', table, partition, type FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('t_stale_first', 't_stale_second') AND active AND column = 'k'
ORDER BY table, partition;

SELECT 'stale first, group by:', countIf(explain LIKE '%Aggregating%(rows: ~10.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT k, count() FROM t_stale_first GROUP BY k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

SELECT 'stale second, group by:', countIf(explain LIKE '%Aggregating%(rows: ~10.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT k, count() FROM t_stale_second GROUP BY k
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

-- After the metadata-only `Enum8` -> `Int8`, the part inserted before keeps `Enum8` statistics, and the part
-- inserted after gets `Int8` statistics. Merges are stopped to keep both parts.
CREATE TABLE t_inserted_after (k UInt64, e Enum8('a' = 1, 'b' = 2, 'c' = 3, 'd' = 4)) ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'basic, uniq_v2';
SYSTEM STOP MERGES t_inserted_after;
INSERT INTO t_inserted_after SELECT number, number % 4 + 1 FROM numbers(10000);
ALTER TABLE t_inserted_after MODIFY COLUMN e Int8;
INSERT INTO t_inserted_after SELECT number, number % 4 + 1 FROM numbers(10000);

SELECT 'inserted after, stored type:', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_inserted_after' AND active AND column = 'e' ORDER BY name;

SELECT 'inserted after, group by:', countIf(explain LIKE '%Aggregating%(rows: ~4.0,%') > 0 FROM (
    EXPLAIN estimates = 1 SELECT e, count() FROM t_inserted_after GROUP BY e
    SETTINGS make_distributed_plan = 1, enable_cascades_optimizer = 1, distributed_plan_execute_locally = 1
) SETTINGS make_distributed_plan = 0, enable_cascades_optimizer = 0;

DROP TABLE t_metadata_only;
DROP TABLE t_converted;
DROP TABLE t_stale_first;
DROP TABLE t_stale_second;
DROP TABLE t_inserted_after;
