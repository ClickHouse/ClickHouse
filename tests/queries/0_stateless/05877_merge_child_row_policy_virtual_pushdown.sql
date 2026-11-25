-- A child's row policy over a virtual column is applied before the query's PREWHERE, as on a direct read of the child.
-- Only the hidden row holds a value `toUInt64` cannot parse.
SET enable_analyzer = 1;

DROP ROW POLICY IF EXISTS p_rpv ON t_rpv;
DROP TABLE IF EXISTS m_rpv, t_rpv;

CREATE TABLE t_rpv (tenant String, payload String, tv String ALIAS concat(_table, ':', tenant)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_rpv (tenant, payload) VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
CREATE TABLE m_rpv (tenant String, payload String) ENGINE = Merge(currentDatabase(), '^t_rpv$');

CREATE ROW POLICY p_rpv ON t_rpv USING _part != '' AND tenant = 'a' TO ALL;
SELECT 'part prewhere', count() FROM m_rpv PREWHERE toUInt64(payload) > 10;
SELECT 'part pushed', count() FROM (EXPLAIN actions = 1 SELECT payload FROM m_rpv) WHERE explain LIKE '%Row level filter column:%_part%';
DROP ROW POLICY p_rpv ON t_rpv;

CREATE ROW POLICY p_rpv ON t_rpv USING tv = 't_rpv:a' TO ALL;
SELECT 'alias over virtual prewhere', count() FROM m_rpv PREWHERE toUInt64(payload) > 10;
SELECT 'alias over virtual where', count() FROM m_rpv WHERE toUInt64(payload) > 10;
DROP ROW POLICY p_rpv ON t_rpv;

DROP TABLE m_rpv, t_rpv;
