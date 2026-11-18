-- After a metadata-only `ALTER TABLE ... MODIFY COLUMN` (here `UInt32` -> `DateTime`), the existing parts keep the
-- old type and its statistics, which still describe the values, and the parts inserted afterwards get the new type.
-- In a table partitioned by time, the old partitions are never merged with the new ones. Statistics of different
-- types cannot be merged, so only the parts of one type are used. That was the type of the first part, and with a
-- filter the statistics of an old type were rejected, so the estimate depended on the part order and on the filter.
-- The current type is preferred now.

DROP TABLE IF EXISTS t_events_a;
DROP TABLE IF EXISTS t_events_b;
DROP TABLE IF EXISTS t_ts;

-- Pinned (randomized in CI): the statistics must be built and used, and joins reordered by their estimates.
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;

-- The tables hold the same data. In `t_events_a` the old data is in the partition that sorts first, in `t_events_b`
-- in the partition that sorts last.
CREATE TABLE t_events_a (p UInt8, ts UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';
CREATE TABLE t_events_b (p UInt8, ts UInt32) ENGINE = MergeTree PARTITION BY p ORDER BY tuple()
SETTINGS auto_statistics_types = 'basic, uniq_v2';

-- January, 1000 distinct timestamps.
INSERT INTO t_events_a SELECT 1, 1704067200 + number % 1000 FROM numbers(100000);
INSERT INTO t_events_b SELECT 2, 1704067200 + number % 1000 FROM numbers(100000);

ALTER TABLE t_events_a MODIFY COLUMN ts DateTime('UTC');
ALTER TABLE t_events_b MODIFY COLUMN ts DateTime('UTC');

-- February, 100000 distinct timestamps.
INSERT INTO t_events_a SELECT 2, toDateTime('2024-02-01 00:00:00', 'UTC') + number FROM numbers(100000);
INSERT INTO t_events_b SELECT 1, toDateTime('2024-02-01 00:00:00', 'UTC') + number FROM numbers(100000);

SELECT DISTINCT 'stored type:', table, partition, type FROM system.parts_columns
WHERE database = currentDatabase() AND table IN ('t_events_a', 't_events_b') AND active AND column = 'ts'
ORDER BY table, partition;

-- 1000 of the February timestamps.
CREATE TABLE t_ts (ts DateTime('UTC')) ENGINE = MergeTree ORDER BY ts
SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_ts SELECT toDateTime('2024-02-01 00:00:00', 'UTC') + number FROM numbers(1000);

SELECT 'join rows:',
    (SELECT count() FROM t_events_a AS e JOIN t_ts AS t ON e.ts = t.ts),
    (SELECT count() FROM t_events_b AS e JOIN t_ts AS t ON e.ts = t.ts);

-- With the statistics of the first part, the join with the old data first was estimated at 198.22 thousand rows from
-- the 1009 distinct values of January, with a filter at 201.00 thousand rows without statistics of `ts`, and the join
-- with the old data last at 2.01 thousand rows from the 99448 distinct values of February.
SELECT 'join estimate, old data first:', extract(explain, 'Output rows: (.*)') FROM (
    EXPLAIN actions = 1, compact = 1, pretty = 1
    SELECT count() FROM t_events_a AS e JOIN t_ts AS t ON e.ts = t.ts
) WHERE explain LIKE '%Output rows:%';

SELECT 'join estimate, old data first, filter:', extract(explain, 'Output rows: (.*)') FROM (
    EXPLAIN actions = 1, compact = 1, pretty = 1
    SELECT count() FROM t_events_a AS e JOIN t_ts AS t ON e.ts = t.ts WHERE e.p <= 2
) WHERE explain LIKE '%Output rows:%';

SELECT 'join estimate, old data last:', extract(explain, 'Output rows: (.*)') FROM (
    EXPLAIN actions = 1, compact = 1, pretty = 1
    SELECT count() FROM t_events_b AS e JOIN t_ts AS t ON e.ts = t.ts
) WHERE explain LIKE '%Output rows:%';

DROP TABLE t_events_a;
DROP TABLE t_events_b;
DROP TABLE t_ts;
