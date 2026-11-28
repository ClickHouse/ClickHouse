-- The monotonicity of `hop`, `hopStart` and `hopEnd` depends on the time zone, which is inherited from the time column
-- when it is not passed explicitly. The time zone has to be found also when the column is wrapped
-- into `LowCardinality(Nullable(...))`: in `Pacific/Chatham` the clocks go from 03:45 back to 02:45,
-- so three-hour windows are not ordered there, although they are ordered in the session time zone.
SET session_timezone = 'UTC';
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS hop_lc_nullable;

CREATE TABLE hop_lc_nullable (t LowCardinality(Nullable(DateTime('Pacific/Chatham')))) ENGINE = MergeTree ORDER BY t
SETTINGS index_granularity = 1, allow_nullable_key = 1;
INSERT INTO hop_lc_nullable SELECT toDateTime('2026-04-04 00:00:00', 'UTC') + number * 60 FROM numbers(2880);

SELECT count() FROM (
    EXPLAIN actions = 1 SELECT t FROM hop_lc_nullable ORDER BY hopStart(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR) LIMIT 1
    SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1)
WHERE explain LIKE '%Read type: InOrder%';

-- One-hour hops are ordered in every time zone.
SELECT count() FROM (
    EXPLAIN actions = 1 SELECT t FROM hop_lc_nullable ORDER BY hopStart(t, INTERVAL 1 HOUR, INTERVAL 3 HOUR) LIMIT 1
    SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1)
WHERE explain LIKE '%Read type: InOrder%';

-- The results are the same with and without the optimizations.
SELECT toTimeZone(assumeNotNull(t), 'UTC') FROM hop_lc_nullable WHERE t >= toDateTime('2026-04-04 13:15:00', 'UTC') ORDER BY hopStart(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR), t LIMIT 1
SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1, max_threads = 1;
SELECT toTimeZone(assumeNotNull(t), 'UTC') FROM hop_lc_nullable WHERE t >= toDateTime('2026-04-04 13:15:00', 'UTC') ORDER BY hopStart(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR), t LIMIT 1
SETTINGS optimize_read_in_order = 0, query_plan_read_in_order = 0, max_threads = 1;

SELECT toTimeZone(assumeNotNull(t), 'UTC') FROM hop_lc_nullable WHERE t >= toDateTime('2026-04-04 13:15:00', 'UTC') ORDER BY hopEnd(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR), t LIMIT 1
SETTINGS optimize_read_in_order = 1, query_plan_read_in_order = 1, max_threads = 1;
SELECT toTimeZone(assumeNotNull(t), 'UTC') FROM hop_lc_nullable WHERE t >= toDateTime('2026-04-04 13:15:00', 'UTC') ORDER BY hopEnd(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR), t LIMIT 1
SETTINGS optimize_read_in_order = 0, query_plan_read_in_order = 0, max_threads = 1;

SELECT w, count() FROM (SELECT hop(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR) AS w FROM hop_lc_nullable WHERE t >= toDateTime('2026-04-04 10:00:00', 'UTC') AND t < toDateTime('2026-04-04 18:00:00', 'UTC')) GROUP BY w ORDER BY w
SETTINGS optimize_aggregation_in_order = 1, query_plan_aggregation_in_order = 1, max_threads = 1;
SELECT w, count() FROM (SELECT hop(t, INTERVAL 3 HOUR, INTERVAL 3 HOUR) AS w FROM hop_lc_nullable WHERE t >= toDateTime('2026-04-04 10:00:00', 'UTC') AND t < toDateTime('2026-04-04 18:00:00', 'UTC')) GROUP BY w ORDER BY w
SETTINGS optimize_aggregation_in_order = 0, query_plan_aggregation_in_order = 0, max_threads = 1;

DROP TABLE hop_lc_nullable;
