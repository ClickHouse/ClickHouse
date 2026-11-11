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
