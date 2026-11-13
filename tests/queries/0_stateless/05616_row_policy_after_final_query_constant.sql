-- A row policy that compares sorting key columns with a value computed once per query is applied before FINAL:
-- the value is the same for every version of a key, so it cannot change which version wins.
SET explain_query_plan_default = 'legacy';
SET apply_row_policy_after_final = 1;

DROP TABLE IF EXISTS t_rp_const;
DROP TABLE IF EXISTS t_rp_const_keys;
DROP TABLE IF EXISTS t_rp_const_set;

CREATE TABLE t_rp_const (k UInt64, s String, payload String, v UInt64) ENGINE = ReplacingMergeTree(v) ORDER BY (k, s);
INSERT INTO t_rp_const SELECT number, toString(number % 10), 'old', 1 FROM numbers(100);
INSERT INTO t_rp_const SELECT number, toString(number % 10), 'new', 2 FROM numbers(100);

CREATE TABLE t_rp_const_keys (k UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_rp_const_keys VALUES (1), (2);

CREATE TABLE t_rp_const_set (k UInt64) ENGINE = Set;
INSERT INTO t_rp_const_set VALUES (1), (2);

DROP ROW POLICY IF EXISTS p_rp_const ON t_rp_const;

SELECT 'currentUser';
CREATE ROW POLICY p_rp_const ON t_rp_const USING s = concat('1', substring(currentUser(), 1, 0)) TO ALL;
SELECT count() FROM (EXPLAIN actions = 1 SELECT * FROM t_rp_const FINAL) WHERE explain LIKE '%Deferred row level filter%';
SELECT count(), countIf(payload = 'old') FROM t_rp_const FINAL;
DROP ROW POLICY p_rp_const ON t_rp_const;

SELECT 'now';
CREATE ROW POLICY p_rp_const ON t_rp_const USING k < if(now() > toDateTime('2000-01-01'), 10, 0) TO ALL;
SELECT count() FROM (EXPLAIN actions = 1 SELECT * FROM t_rp_const FINAL) WHERE explain LIKE '%Deferred row level filter%';
SELECT count(), countIf(payload = 'old') FROM t_rp_const FINAL;
DROP ROW POLICY p_rp_const ON t_rp_const;

SELECT 'IN subquery';
CREATE ROW POLICY p_rp_const ON t_rp_const USING k IN (SELECT k FROM t_rp_const_keys) TO ALL;
SELECT count() FROM (EXPLAIN actions = 1 SELECT * FROM t_rp_const FINAL) WHERE explain LIKE '%Deferred row level filter%';
SELECT count(), countIf(payload = 'old') FROM t_rp_const FINAL;
DROP ROW POLICY p_rp_const ON t_rp_const;

-- A concurrent INSERT into a Set table changes its set while the query runs, so this policy stays after FINAL.
SELECT 'IN Set table';
CREATE ROW POLICY p_rp_const ON t_rp_const USING k IN t_rp_const_set TO ALL;
SELECT count() FROM (EXPLAIN actions = 1 SELECT * FROM t_rp_const FINAL) WHERE explain LIKE '%Deferred row level filter%';
SELECT count(), countIf(payload = 'old') FROM t_rp_const FINAL;
DROP ROW POLICY p_rp_const ON t_rp_const;

SELECT 'IN Set table in a lambda';
CREATE ROW POLICY p_rp_const ON t_rp_const USING arrayExists(x -> x IN t_rp_const_set, [k]) TO ALL;
SELECT count() FROM (EXPLAIN actions = 1 SELECT * FROM t_rp_const FINAL) WHERE explain LIKE '%Deferred row level filter%';
SELECT count(), countIf(payload = 'old') FROM t_rp_const FINAL;
DROP ROW POLICY p_rp_const ON t_rp_const;

-- Control: the same constant compared with a column outside the sorting key keeps the policy after FINAL.
SELECT 'column outside the sorting key';
CREATE ROW POLICY p_rp_const ON t_rp_const USING payload != substring(currentUser(), 1, 0) TO ALL;
SELECT count() FROM (EXPLAIN actions = 1 SELECT * FROM t_rp_const FINAL) WHERE explain LIKE '%Deferred row level filter%';
SELECT count(), countIf(payload = 'old') FROM t_rp_const FINAL;
DROP ROW POLICY p_rp_const ON t_rp_const;

DROP TABLE t_rp_const;
DROP TABLE t_rp_const_keys;
DROP TABLE t_rp_const_set;

-- GraphiteMergeTree rolls up rows with different times into one row: only a policy on the leading path column
-- is applied before FINAL.
SET optimize_on_insert = 0;
DROP TABLE IF EXISTS t_rp_const_graphite;
DROP TABLE IF EXISTS t_rp_const_graphite_time_first;

CREATE TABLE t_rp_const_graphite (Path String, Time DateTime('UTC'), Value Float64, Version UInt32)
ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Path, Time);
INSERT INTO t_rp_const_graphite VALUES ('sum_1', 960000100, 1, 1), ('sum_1', 960000200, 2, 1);

SELECT 'Graphite time';
CREATE ROW POLICY p_rp_const ON t_rp_const_graphite USING Time >= if(now() > toDateTime('2000-01-01'), toDateTime(960000150, 'UTC'), toDateTime(0, 'UTC')) TO ALL;
SELECT count() FROM (EXPLAIN actions = 1 SELECT * FROM t_rp_const_graphite FINAL) WHERE explain LIKE '%Deferred row level filter%';
SELECT count(), sum(Value) FROM t_rp_const_graphite FINAL;
DROP ROW POLICY p_rp_const ON t_rp_const_graphite;

SELECT 'Graphite time, literal';
CREATE ROW POLICY p_rp_const ON t_rp_const_graphite USING Time <= toDateTime(960000150, 'UTC') TO ALL;
SELECT count() FROM (EXPLAIN actions = 1 SELECT * FROM t_rp_const_graphite FINAL) WHERE explain LIKE '%Deferred row level filter%';
SELECT count(), sum(Value) FROM t_rp_const_graphite FINAL;
DROP ROW POLICY p_rp_const ON t_rp_const_graphite;

SELECT 'Graphite leading path';
CREATE ROW POLICY p_rp_const ON t_rp_const_graphite USING Path = if(now() > toDateTime('2000-01-01'), 'sum_1', '') TO ALL;
SELECT count() FROM (EXPLAIN actions = 1 SELECT * FROM t_rp_const_graphite FINAL) WHERE explain LIKE '%Deferred row level filter%';
SELECT count(), sum(Value) FROM t_rp_const_graphite FINAL;
DROP ROW POLICY p_rp_const ON t_rp_const_graphite;

CREATE TABLE t_rp_const_graphite_time_first (Time DateTime('UTC'), Path String, Value Float64, Version UInt32)
ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Time, Path);
INSERT INTO t_rp_const_graphite_time_first VALUES (960000100, 'sum_a', 1, 1), (960000150, 'sum_b', 5, 1), (960000200, 'sum_a', 2, 1);

SELECT 'Graphite path not leading';
CREATE ROW POLICY p_rp_const ON t_rp_const_graphite_time_first USING Path = if(now() > toDateTime('2000-01-01'), 'sum_a', '') TO ALL;
SELECT count() FROM (EXPLAIN actions = 1 SELECT * FROM t_rp_const_graphite_time_first FINAL) WHERE explain LIKE '%Deferred row level filter%';
SELECT count(), sum(Value) FROM t_rp_const_graphite_time_first FINAL;
DROP ROW POLICY p_rp_const ON t_rp_const_graphite_time_first;

DROP TABLE t_rp_const_graphite;
DROP TABLE t_rp_const_graphite_time_first;
