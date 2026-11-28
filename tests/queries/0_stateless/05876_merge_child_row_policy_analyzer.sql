-- A `Merge` table analyzes a child's row policy as a direct read of the child does.
SET enable_analyzer = 1;

DROP ROW POLICY IF EXISTS p_rpa ON t_rpa_a;
DROP ROW POLICY IF EXISTS p_rpa ON t_rpa_b;
DROP ROW POLICY IF EXISTS p_rpa_m ON m_rpa_a;
DROP ROW POLICY IF EXISTS p_rpa ON t_rpa_j;
DROP ROW POLICY IF EXISTS p_rpa ON t_rpa_m;
DROP ROW POLICY IF EXISTS p_rpa ON t_rpa_l;
DROP TABLE IF EXISTS m_rpa_a, m_rpa_ab, m_rpa_j, m_rpa_l, m_rpa_m, t_rpa_a, t_rpa_b, t_rpa_j, t_rpa_l, t_rpa_m;

CREATE TABLE t_rpa_a (tenant String, payload String, tv String ALIAS concat(_table, ':', tenant)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_rpa_b (tenant String, payload String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_rpa_a (tenant, payload) VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
INSERT INTO t_rpa_b VALUES ('a', '40'), ('b', 'hidden');
CREATE TABLE m_rpa_a (tenant String, payload String) ENGINE = Merge(currentDatabase(), '^t_rpa_a$');
CREATE TABLE m_rpa_ab (tenant String, payload String) ENGINE = Merge(currentDatabase(), '^t_rpa_[ab]$');

CREATE ROW POLICY p_rpa ON t_rpa_a USING t_rpa_a.tenant = 'a' TO ALL;
SELECT 'qualified', count() FROM m_rpa_a;
DROP ROW POLICY p_rpa ON t_rpa_a;

CREATE ROW POLICY p_rpa ON t_rpa_a USING _part != '' AND tenant = 'a' TO ALL;
SELECT 'part', count() FROM m_rpa_a;
DROP ROW POLICY p_rpa ON t_rpa_a;

CREATE ROW POLICY p_rpa ON t_rpa_a USING _table = 't_rpa_a' AND tenant = 'a' TO ALL;
SELECT 'table', count() FROM m_rpa_a;
SELECT 'table grouped', _table, count() FROM m_rpa_a GROUP BY _table;
DROP ROW POLICY p_rpa ON t_rpa_a;

CREATE ROW POLICY p_rpa ON t_rpa_a USING tv = 't_rpa_a:a' TO ALL;
SELECT 'alias over virtual', count() FROM m_rpa_a;
DROP ROW POLICY p_rpa ON t_rpa_a;

CREATE ROW POLICY p_rpa ON t_rpa_a USING tenant = 'a' TO ALL;
CREATE ROW POLICY p_rpa ON t_rpa_b USING tenant = 'a' TO ALL;
SELECT 'two children', tenant, payload FROM m_rpa_ab WHERE tenant != 'zz' ORDER BY toUInt64(payload);
SELECT 'old analyzer', count() FROM m_rpa_ab SETTINGS enable_analyzer = 0;
DROP ROW POLICY p_rpa ON t_rpa_b;

CREATE ROW POLICY p_rpa_m ON m_rpa_a USING tenant = 'a' TO ALL;
SELECT 'same expression', count() FROM m_rpa_a;
DROP ROW POLICY p_rpa_m ON m_rpa_a;
DROP ROW POLICY p_rpa ON t_rpa_a;

CREATE ROW POLICY p_rpa ON t_rpa_a USING tenant = 'a' OR tenant = 'c' OR tenant = 'd' TO ALL;
SELECT 'or chain', count() FROM m_rpa_a;
SELECT 'or chain plan', count() FROM (EXPLAIN actions = 1 SELECT payload FROM m_rpa_a) WHERE explain LIKE '%Row level filter column:%' AND explain NOT LIKE '% OR %';
DROP ROW POLICY p_rpa ON t_rpa_a;

CREATE ROW POLICY p_rpa ON t_rpa_a USING arrayExists(tenant -> tenant = t_rpa_a.tenant, ['a']) TO ALL;
SELECT 'lambda', count() FROM m_rpa_a;
DROP ROW POLICY p_rpa ON t_rpa_a;

-- A correlated subquery in a policy is not supported on a direct read either.
CREATE ROW POLICY p_rpa ON t_rpa_a USING EXISTS (SELECT 1 FROM system.one WHERE tenant = 'a') TO ALL;
SELECT count() FROM m_rpa_a; -- { serverError NOT_IMPLEMENTED }
DROP ROW POLICY p_rpa ON t_rpa_a;

CREATE ROW POLICY p_rpa ON t_rpa_a USING toUInt8(tenant = 'a') IN (SELECT dummy + 1 FROM (SELECT dummy FROM system.one) WHERE dummy IN (SELECT dummy FROM system.one)) TO ALL;
SELECT 'nested sets', count() FROM m_rpa_a;
DROP ROW POLICY p_rpa ON t_rpa_a;

-- Referenced twice, so it is not inlined.
CREATE ROW POLICY p_rpa ON t_rpa_a USING tenant IN (WITH c AS MATERIALIZED (SELECT 'a' AS x) SELECT x FROM c WHERE x IN (SELECT x FROM c)) TO ALL;
SELECT 'materialized cte', count() FROM m_rpa_a SETTINGS enable_materialized_cte = 1;
DROP ROW POLICY p_rpa ON t_rpa_a;

-- A direct read of the `Log` table throws here; the expected value is the one without `MATERIALIZED`.
CREATE TABLE t_rpa_l (tenant String, payload String) ENGINE = Log;
INSERT INTO t_rpa_l VALUES ('a', '20'), ('b', 'secret'), ('a', '30');
CREATE TABLE m_rpa_l AS t_rpa_l ENGINE = Merge(currentDatabase(), '^t_rpa_l$');
CREATE ROW POLICY p_rpa ON t_rpa_l USING tenant IN (WITH c AS MATERIALIZED (SELECT 'a' AS x) SELECT x FROM c WHERE x IN (SELECT x FROM c)) TO ALL;
SELECT 'materialized cte log', count() FROM m_rpa_l SETTINGS enable_materialized_cte = 1;
DROP ROW POLICY p_rpa ON t_rpa_l;

CREATE TABLE t_rpa_j (k String, tup Tuple(a UInt8, b String)) ENGINE = Join(ANY, LEFT, k);
INSERT INTO t_rpa_j VALUES ('1', (1, 'x')), ('2', (0, 'y'));
CREATE TABLE m_rpa_j (k String) ENGINE = Merge(currentDatabase(), '^t_rpa_j$');
CREATE ROW POLICY p_rpa ON t_rpa_j USING tup.a = 1 TO ALL;
SELECT 'join subcolumn', count() FROM m_rpa_j;
DROP ROW POLICY p_rpa ON t_rpa_j;

CREATE TABLE t_rpa_m (tenant String, payload String) ENGINE = Memory;
INSERT INTO t_rpa_m VALUES ('a', '20'), ('b', 'secret');
CREATE TABLE m_rpa_m AS t_rpa_m ENGINE = Merge(currentDatabase(), '^t_rpa_m$');
CREATE ROW POLICY p_rpa ON t_rpa_m USING _table = 't_rpa_m' AND tenant = 'a' TO ALL;
SELECT 'memory table', count() FROM m_rpa_m;
DROP ROW POLICY p_rpa ON t_rpa_m;

DROP TABLE m_rpa_a, m_rpa_ab, m_rpa_j, m_rpa_l, m_rpa_m, t_rpa_a, t_rpa_b, t_rpa_j, t_rpa_l, t_rpa_m;
