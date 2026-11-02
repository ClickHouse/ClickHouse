-- TTL GROUP BY ... SET with an aggregate function over an expression or a subcolumn: https://github.com/ClickHouse/ClickHouse/issues/128596
-- Later merges apply a TTL GROUP BY rule again, so a non-idempotent assignment is checked through MODIFY TTL, which applies it once.

DROP TABLE IF EXISTS t_ttl_argument_mutation;
CREATE TABLE t_ttl_argument_mutation (k UInt8, event DateTime, value UInt64, cnt UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_ttl_argument_mutation VALUES (1, '2020-01-01 00:00:00', 10, 0), (1, '2020-01-01 09:00:00', 20, 0);
ALTER TABLE t_ttl_argument_mutation MODIFY TTL event + INTERVAL 1 SECOND GROUP BY k SET value = sum(value * 2), cnt = sum(1) SETTINGS mutations_sync = 2;
SELECT value, cnt FROM t_ttl_argument_mutation;
ALTER TABLE t_ttl_argument_mutation MODIFY TTL event + INTERVAL 1 SECOND GROUP BY k SET value = sumIf(value, value IN (SELECT 10)); -- { serverError BAD_ARGUMENTS }
DROP TABLE t_ttl_argument_mutation;

DROP TABLE IF EXISTS t_ttl_argument_merge;
CREATE TABLE t_ttl_argument_merge (k UInt8, event DateTime, value UInt64) ENGINE = MergeTree ORDER BY k
TTL event + INTERVAL 1 SECOND GROUP BY k SET value = sumIf(value, value > 15);
INSERT INTO t_ttl_argument_merge VALUES (1, '2020-01-01 00:00:00', 10), (1, '2020-01-01 09:00:00', 20);
OPTIMIZE TABLE t_ttl_argument_merge FINAL;
SELECT value FROM t_ttl_argument_merge;
DROP TABLE t_ttl_argument_merge;

DROP TABLE IF EXISTS t_ttl_argument_subcolumn;
CREATE TABLE t_ttl_argument_subcolumn (k UInt8, event DateTime, tup Tuple(ts UInt64, x UInt8)) ENGINE = MergeTree ORDER BY k
TTL event + INTERVAL 1 SECOND GROUP BY k SET tup = tuple(max(tup.ts), 0);
INSERT INTO t_ttl_argument_subcolumn VALUES (1, '2020-01-01 00:00:00', (10, 1)), (1, '2020-01-01 09:00:00', (20, 2));
OPTIMIZE TABLE t_ttl_argument_subcolumn FINAL;
SELECT tup FROM t_ttl_argument_subcolumn;
DROP TABLE t_ttl_argument_subcolumn;

DROP TABLE IF EXISTS t_ttl_argument_state;
CREATE TABLE t_ttl_argument_state (k UInt8, event DateTime, s AggregateFunction(sumForEach, Array(Int32))) ENGINE = MergeTree ORDER BY k
TTL event + INTERVAL 1 SECOND GROUP BY k SET s = sumForEachState(CAST(finalizeAggregation(s), 'Array(Int32)'));
INSERT INTO t_ttl_argument_state SELECT 1, '2020-01-01 00:00:00', sumForEachState(CAST([1, 2, 3], 'Array(Int32)'));
INSERT INTO t_ttl_argument_state SELECT 1, '2020-01-01 09:00:00', sumForEachState(CAST([3, 2, 1], 'Array(Int32)'));
OPTIMIZE TABLE t_ttl_argument_state FINAL;
SELECT finalizeAggregation(s) FROM t_ttl_argument_state;
DROP TABLE t_ttl_argument_state;

CREATE TABLE t_ttl_argument_in (k UInt8, event DateTime, value UInt64) ENGINE = MergeTree ORDER BY k
TTL event + INTERVAL 1 SECOND GROUP BY k SET value = sumIf(value, value IN (SELECT 10)); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_ttl_argument_in (k UInt8, event DateTime, value UInt64) ENGINE = MergeTree ORDER BY k
TTL event + INTERVAL 1 SECOND GROUP BY k SET value = sumIf(value, k IN system.one); -- { serverError BAD_ARGUMENTS }
CREATE TABLE t_ttl_argument_in (k UInt8, event DateTime, value UInt64) ENGINE = MergeTree ORDER BY k
TTL event + INTERVAL 1 SECOND GROUP BY k SET value = sum(value * (SELECT 2)); -- { serverError BAD_ARGUMENTS }
