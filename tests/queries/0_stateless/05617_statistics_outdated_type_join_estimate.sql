-- After `ALTER TABLE ... MODIFY COLUMN` with a conversion that changes the values (here `DateTime` -> `Date`), a part
-- keeps the column of the old type and its statistics until a mutation rewrites it. A read converts the values to
-- the current type, so the statistics no longer describe them. They were used by the join-order optimizer when the
-- read has no filter, and rejected when it has one.

DROP TABLE IF EXISTS t_events;
DROP TABLE IF EXISTS t_days;

-- Pinned (randomized in CI): the statistics must be built and used, and joins reordered by their estimates.
SET use_statistics = 1;
SET materialize_statistics_on_insert = 1;
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_limit = 10;
SET query_plan_optimize_join_order_randomize = 0;

-- 10000 distinct seconds, all on one day.
CREATE TABLE t_events (k UInt64, ts DateTime('UTC')) ENGINE = MergeTree ORDER BY k
SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_events SELECT number, toDateTime('2024-01-01 00:00:00', 'UTC') + number FROM numbers(10000);

-- The mutation that rewrites the part waits while merges are stopped, as a long mutation on a large table does.
SYSTEM STOP MERGES t_events;
ALTER TABLE t_events MODIFY COLUMN ts Date SETTINGS alter_sync = 0, mutations_sync = 0;

SELECT DISTINCT 'stored type:', type FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_events' AND active AND column = 'ts';

-- 100 days, 10 rows each.
CREATE TABLE t_days (ts Date) ENGINE = MergeTree ORDER BY ts
SETTINGS auto_statistics_types = 'basic, uniq_v2';
INSERT INTO t_days SELECT toDate('2024-01-01') + number % 100 FROM numbers(1000);

SELECT 'join rows:', count() FROM t_events AS e JOIN t_days AS d ON e.ts = d.ts;

-- With the `DateTime` statistics, `ts` had 9826 distinct values instead of 1, and the join without a filter was
-- estimated at 1.02 thousand rows.
SELECT 'join estimate:', extract(explain, 'Output rows: (.*)') FROM (
    EXPLAIN actions = 1, compact = 1, pretty = 1
    SELECT count() FROM t_events AS e JOIN t_days AS d ON e.ts = d.ts
) WHERE explain LIKE '%Output rows:%';

SELECT 'join estimate, filter:', extract(explain, 'Output rows: (.*)') FROM (
    EXPLAIN actions = 1, compact = 1, pretty = 1
    SELECT count() FROM t_events AS e JOIN t_days AS d ON e.ts = d.ts WHERE e.k < 10000
) WHERE explain LIKE '%Output rows:%';

DROP TABLE t_events;
DROP TABLE t_days;
