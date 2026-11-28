-- A `Merge` table applies a child's row policy once, below that child's ORDER BY, LIMIT and aggregation; a child that
-- processes the query itself cannot have its policy applied and is rejected, as a direct read of it is.

DROP ROW POLICY IF EXISTS p_05400_local ON t_policy_local;
DROP ROW POLICY IF EXISTS p_05400_in ON t_policy_in;
DROP ROW POLICY IF EXISTS p_05400_inner ON m_policy_inner;
DROP ROW POLICY IF EXISTS p_05400_dist ON d_policy_local;
DROP TABLE IF EXISTS m_policy_dist; DROP TABLE IF EXISTS m_policy_nested; DROP TABLE IF EXISTS m_policy_in;
DROP TABLE IF EXISTS m_policy_mixed; DROP TABLE IF EXISTS m_policy_inner;
DROP TABLE IF EXISTS d_policy_local; DROP TABLE IF EXISTS d_policy_other;
DROP TABLE IF EXISTS t_policy_other; DROP TABLE IF EXISTS t_policy_in; DROP TABLE IF EXISTS t_policy_local;

CREATE TABLE t_policy_local (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_policy_local SELECT number, if(number = 5, 1000, number) FROM numbers(10);
CREATE TABLE t_policy_in (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_policy_in SELECT number, number FROM numbers(10);
CREATE TABLE t_policy_other (a UInt64, b UInt64) ENGINE = MergeTree ORDER BY a;
INSERT INTO t_policy_other SELECT number + 100, number + 100 FROM numbers(3);
CREATE TABLE d_policy_other AS t_policy_other ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_policy_other);
CREATE TABLE d_policy_local AS t_policy_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), t_policy_local);
CREATE TABLE m_policy_inner AS t_policy_other ENGINE = Merge(currentDatabase(), '^t_policy_other$');
CREATE TABLE m_policy_mixed AS t_policy_local ENGINE = Merge(currentDatabase(), '^(t_policy_local|d_policy_other)$');
CREATE TABLE m_policy_in AS t_policy_in ENGINE = Merge(currentDatabase(), '^(t_policy_in|d_policy_other)$');
CREATE TABLE m_policy_nested AS t_policy_other ENGINE = Merge(currentDatabase(), '^(m_policy_inner|d_policy_other)$');
CREATE TABLE m_policy_dist AS t_policy_local ENGINE = Merge(currentDatabase(), '^d_policy_local$');

CREATE ROW POLICY p_05400_local ON t_policy_local USING a != 5 TO ALL;
CREATE ROW POLICY p_05400_in ON t_policy_in USING a IN (SELECT number FROM numbers(3)) TO ALL;
CREATE ROW POLICY p_05400_inner ON m_policy_inner USING a != 101 TO ALL;
CREATE ROW POLICY p_05400_dist ON d_policy_local USING a != 5 TO ALL;

SELECT '-- a policy on a child next to a Distributed child';
SELECT a, b FROM m_policy_mixed ORDER BY a SETTINGS prefer_localhost_replica = 0;
SELECT a, b FROM m_policy_mixed ORDER BY a SETTINGS prefer_localhost_replica = 1;
SELECT count(), sum(b), max(b) FROM m_policy_mixed;
SELECT b FROM m_policy_mixed ORDER BY b DESC LIMIT 4;
SELECT a, b FROM m_policy_mixed ORDER BY a SETTINGS extremes = 1;
SELECT count() FROM m_policy_in;
SELECT a FROM m_policy_nested ORDER BY a;

SELECT '-- a policy on a Distributed child is rejected';
SELECT a FROM m_policy_dist ORDER BY a LIMIT 2 SETTINGS prefer_localhost_replica = 0; -- { serverError ILLEGAL_PREWHERE }
SELECT a FROM m_policy_dist ORDER BY a LIMIT 2 SETTINGS prefer_localhost_replica = 1; -- { serverError ILLEGAL_PREWHERE }
SELECT a, b FROM m_policy_dist ORDER BY a SETTINGS extremes = 1, prefer_localhost_replica = 1; -- { serverError ILLEGAL_PREWHERE }
SELECT count() FROM m_policy_dist; -- { serverError ILLEGAL_PREWHERE }
SELECT a FROM m_policy_dist CROSS JOIN (SELECT 1 AS x) AS j; -- { serverError ILLEGAL_PREWHERE }

DROP ROW POLICY p_05400_local ON t_policy_local; DROP ROW POLICY p_05400_in ON t_policy_in;
DROP ROW POLICY p_05400_inner ON m_policy_inner; DROP ROW POLICY p_05400_dist ON d_policy_local;
DROP TABLE m_policy_dist; DROP TABLE m_policy_nested; DROP TABLE m_policy_in; DROP TABLE m_policy_mixed;
DROP TABLE m_policy_inner; DROP TABLE d_policy_local; DROP TABLE d_policy_other;
DROP TABLE t_policy_other; DROP TABLE t_policy_in; DROP TABLE t_policy_local;
