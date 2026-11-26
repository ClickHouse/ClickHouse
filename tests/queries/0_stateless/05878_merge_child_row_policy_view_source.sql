-- The row policy of a `Merge` child reads a table whose own policy reads the source of a materialized view.
-- That policy sees the inserted block, as on a direct read of the child.
SET enable_analyzer = 1;

DROP ROW POLICY IF EXISTS p_rpvs ON t_rpvs_allowed;
DROP ROW POLICY IF EXISTS p_rpvs ON t_rpvs_mt;
DROP ROW POLICY IF EXISTS p_rpvs ON t_rpvs_log;
DROP TABLE IF EXISTS mv_rpvs_direct_mt, mv_rpvs_merge_mt, mv_rpvs_direct_log, mv_rpvs_merge_log;
DROP TABLE IF EXISTS m_rpvs_mt, m_rpvs_log, t_rpvs_dst, t_rpvs_mt, t_rpvs_log, t_rpvs_allowed, t_rpvs_src;

CREATE TABLE t_rpvs_src (x UInt64) ENGINE = Null;
CREATE TABLE t_rpvs_allowed (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_rpvs_mt (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE t_rpvs_log (x UInt64) ENGINE = Log;
CREATE TABLE t_rpvs_dst (label String, x UInt64) ENGINE = MergeTree ORDER BY (label, x);
INSERT INTO t_rpvs_allowed VALUES (1), (2), (3);
INSERT INTO t_rpvs_mt VALUES (1), (2), (3);
INSERT INTO t_rpvs_log VALUES (1), (2), (3);
CREATE TABLE m_rpvs_mt AS t_rpvs_mt ENGINE = Merge(currentDatabase(), '^t_rpvs_mt$');
CREATE TABLE m_rpvs_log AS t_rpvs_log ENGINE = Merge(currentDatabase(), '^t_rpvs_log$');

CREATE MATERIALIZED VIEW mv_rpvs_direct_mt TO t_rpvs_dst AS SELECT 'direct mt' AS label, x FROM t_rpvs_src WHERE x IN (SELECT x FROM t_rpvs_mt);
CREATE MATERIALIZED VIEW mv_rpvs_merge_mt TO t_rpvs_dst AS SELECT 'merge mt' AS label, x FROM t_rpvs_src WHERE x IN (SELECT x FROM m_rpvs_mt);
CREATE MATERIALIZED VIEW mv_rpvs_direct_log TO t_rpvs_dst AS SELECT 'direct log' AS label, x FROM t_rpvs_src WHERE x IN (SELECT x FROM t_rpvs_log);
CREATE MATERIALIZED VIEW mv_rpvs_merge_log TO t_rpvs_dst AS SELECT 'merge log' AS label, x FROM t_rpvs_src WHERE x IN (SELECT x FROM m_rpvs_log);

CREATE ROW POLICY p_rpvs ON t_rpvs_allowed USING x IN (SELECT x FROM t_rpvs_src) TO ALL;
CREATE ROW POLICY p_rpvs ON t_rpvs_mt USING x IN (SELECT x FROM t_rpvs_allowed) TO ALL;
CREATE ROW POLICY p_rpvs ON t_rpvs_log USING x IN (SELECT x FROM t_rpvs_allowed) TO ALL;

SELECT 'nested';
INSERT INTO t_rpvs_src SETTINGS async_insert = 0 VALUES (1), (2), (10);
SELECT label, groupArray(x) FROM (SELECT label, x FROM t_rpvs_dst ORDER BY label, x) GROUP BY label ORDER BY label;
TRUNCATE TABLE t_rpvs_dst;

-- Referenced twice, so it is not inlined.
DROP ROW POLICY p_rpvs ON t_rpvs_mt;
CREATE ROW POLICY p_rpvs ON t_rpvs_mt USING x IN (WITH c AS MATERIALIZED (SELECT x FROM t_rpvs_allowed) SELECT x FROM c WHERE x IN (SELECT x FROM c)) TO ALL;

SELECT 'materialized cte';
INSERT INTO t_rpvs_src SETTINGS async_insert = 0, enable_materialized_cte = 1 VALUES (1), (2), (10);
SELECT label, groupArray(x) FROM (SELECT label, x FROM t_rpvs_dst ORDER BY label, x) GROUP BY label ORDER BY label;

DROP ROW POLICY p_rpvs ON t_rpvs_allowed;
DROP ROW POLICY p_rpvs ON t_rpvs_mt;
DROP ROW POLICY p_rpvs ON t_rpvs_log;
DROP TABLE mv_rpvs_direct_mt, mv_rpvs_merge_mt, mv_rpvs_direct_log, mv_rpvs_merge_log;
DROP TABLE m_rpvs_mt, m_rpvs_log, t_rpvs_dst, t_rpvs_mt, t_rpvs_log, t_rpvs_allowed, t_rpvs_src;
