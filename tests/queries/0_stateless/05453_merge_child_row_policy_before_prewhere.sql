-- Random settings limits: use_query_condition_cache=(0, 0); optimize_move_to_prewhere=(1, None)
-- A `Merge` table applies a child's row policy before the query's PREWHERE and WHERE, as a direct read of the child does.
-- Only the rows the policies hide hold values `toUInt64` cannot parse, so evaluating the condition on them throws.

SET query_plan_optimize_prewhere = 1;

DROP ROW POLICY IF EXISTS p_05453_mt ON t_mt;
DROP ROW POLICY IF EXISTS p_05453_memory ON t_memory;
DROP ROW POLICY IF EXISTS p_05453_memory_default ON t_memory_default;
DROP ROW POLICY IF EXISTS p_05453_final ON t_final;
DROP ROW POLICY IF EXISTS p_05453_final_ver ON t_final_ver;
DROP ROW POLICY IF EXISTS p_05453_in ON t_in;
DROP ROW POLICY IF EXISTS p_05453_alias ON t_alias;
DROP ROW POLICY IF EXISTS p_05453_alias_sub ON t_alias_sub;
DROP ROW POLICY IF EXISTS p_05453_alias_lambda ON t_alias_lambda;
DROP ROW POLICY IF EXISTS p_05453_alias_virtual ON t_alias_virtual;
DROP ROW POLICY IF EXISTS p_05453_both_child ON t_both;
DROP ROW POLICY IF EXISTS p_05453_both_merge ON m_both;
DROP ROW POLICY IF EXISTS p_05453_nested ON m_nested_inner;
DROP ROW POLICY IF EXISTS p_05453_nested_both_child ON t_nested_both;
DROP ROW POLICY IF EXISTS p_05453_nested_both_merge ON m_nested_both_inner;
DROP TABLE IF EXISTS m_mt, t_mt, m_memory, t_memory, m_memory_default, t_memory_default, m_final, t_final, t_final_ver, m_final_ver, m_in, t_in, t_in_dst, m_alias, t_alias, m_alias_sub, t_alias_sub, m_alias_lambda, t_alias_lambda, m_alias_virtual, t_alias_virtual, m_both, t_both,
    m_nested_outer, m_nested_inner, t_nested, m_nested_both_outer, m_nested_both_inner, t_nested_both;

CREATE TABLE t_mt (tenant String, payload String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_mt VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
CREATE TABLE m_mt AS t_mt ENGINE = Merge(currentDatabase(), '^t_mt$');

SELECT count() FROM m_mt PREWHERE toUInt64(payload) > 10; -- { serverError CANNOT_PARSE_TEXT }

CREATE ROW POLICY p_05453_mt ON t_mt USING tenant = 'a' TO ALL;
SELECT 'mt prewhere', count() FROM m_mt PREWHERE toUInt64(payload) > 10;
SELECT 'mt where', count() FROM m_mt WHERE toUInt64(payload) > 10;
SELECT 'mt rows', payload FROM m_mt PREWHERE toUInt64(payload) > 10 ORDER BY payload;

CREATE TABLE t_memory (tenant String, payload String) ENGINE = Memory;
INSERT INTO t_memory VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
CREATE TABLE m_memory AS t_memory ENGINE = Merge(currentDatabase(), '^t_memory$');
CREATE ROW POLICY p_05453_memory ON t_memory USING tenant = 'a' TO ALL;
SELECT 'memory prewhere', count() FROM m_memory PREWHERE toUInt64(payload) > 10;
SELECT 'memory plan', count() > 0 FROM (EXPLAIN actions = 1 SELECT payload FROM m_memory) WHERE explain ILIKE '%Row level filter column%';

CREATE TABLE t_memory_default (tenant String DEFAULT 'a', payload String) ENGINE = Memory;
INSERT INTO t_memory_default VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
CREATE TABLE m_memory_default (tenant String, payload String) ENGINE = Merge(currentDatabase(), '^t_memory_default$');
CREATE ROW POLICY p_05453_memory_default ON t_memory_default USING tenant = 'a' TO ALL;
SELECT 'memory default rows', payload FROM m_memory_default ORDER BY payload;
SELECT 'memory default plan', count() > 0 FROM (EXPLAIN actions = 1 SELECT payload FROM m_memory_default) WHERE explain ILIKE '%Row level filter column%';

CREATE TABLE t_final (k UInt64, tenant String, payload String) ENGINE = ReplacingMergeTree ORDER BY k;
INSERT INTO t_final VALUES (1, 'a', '5'), (2, 'a', '20'), (3, 'b', 'secret'), (4, 'a', '30');
CREATE TABLE m_final AS t_final ENGINE = Merge(currentDatabase(), '^t_final$');
CREATE ROW POLICY p_05453_final ON t_final USING tenant = 'a' TO ALL;
SELECT 'final prewhere', count() FROM m_final FINAL PREWHERE toUInt64(payload) > 10;
SELECT 'final prewhere, policy before final', count() FROM m_final FINAL PREWHERE toUInt64(payload) > 10
SETTINGS apply_row_policy_after_final = 0;

CREATE TABLE t_final_ver (k UInt64, v UInt64, tenant String) ENGINE = ReplacingMergeTree(v) ORDER BY k;
SYSTEM STOP MERGES t_final_ver;
INSERT INTO t_final_ver VALUES (1, 1, 'a');
INSERT INTO t_final_ver VALUES (1, 2, 'b');
CREATE TABLE m_final_ver AS t_final_ver ENGINE = Merge(currentDatabase(), '^t_final_ver$');
CREATE ROW POLICY p_05453_final_ver ON t_final_ver USING tenant = 'a' TO ALL;
SELECT 'final order, policy after final', count() FROM m_final_ver FINAL SETTINGS apply_row_policy_after_final = 1;
SELECT 'final order, policy before final', count() FROM m_final_ver FINAL SETTINGS apply_row_policy_after_final = 0;

CREATE TABLE t_in_dst (tenant String, payload String) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_in AS t_in_dst ENGINE = Buffer(currentDatabase(), t_in_dst, 1, 1000, 1000, 100000, 100000, 10000000, 10000000);
INSERT INTO t_in VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
CREATE TABLE m_in AS t_in_dst ENGINE = Merge(currentDatabase(), '^t_in$');
CREATE ROW POLICY p_05453_in ON t_in USING tenant IN (SELECT 'a') TO ALL;
SELECT 'in prewhere', count() FROM m_in PREWHERE toUInt64(payload) > 10;

CREATE TABLE t_alias (tenant String, payload String, t2 String ALIAS lower(tenant)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias (tenant, payload) VALUES ('a', '5'), ('a', '20'), ('B', 'secret'), ('a', '30');
CREATE TABLE m_alias (tenant String, payload String) ENGINE = Merge(currentDatabase(), '^t_alias$');
CREATE ROW POLICY p_05453_alias ON t_alias USING t2 = 'a' TO ALL;
SELECT 'alias prewhere', count() FROM m_alias PREWHERE toUInt64(payload) > 10;

CREATE TABLE t_alias_sub (tenant String, payload String, a Tuple(t String) ALIAS tuple(tenant)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_sub (tenant, payload) VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
CREATE TABLE m_alias_sub (tenant String, payload String) ENGINE = Merge(currentDatabase(), '^t_alias_sub$');
CREATE ROW POLICY p_05453_alias_sub ON t_alias_sub USING a.t = 'a' AND arrayExists(x -> x = a.t, ['a']) TO ALL;
SELECT 'alias subcolumn prewhere', count() FROM m_alias_sub PREWHERE toUInt64(payload) > 10;

CREATE TABLE t_alias_lambda (tenant String, payload String, a Tuple(t String) ALIAS tuple(tenant)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_lambda (tenant, payload) VALUES ('a', '5'), ('b', 'secret');
CREATE TABLE m_alias_lambda (tenant String, payload String) ENGINE = Merge(currentDatabase(), '^t_alias_lambda$');
CREATE ROW POLICY p_05453_alias_lambda ON t_alias_lambda USING arrayExists(`a.t` -> `a.t` = 'b', ['a']) TO ALL;
SELECT 'alias lambda argument rows', count() FROM m_alias_lambda;

CREATE TABLE t_alias_virtual (tenant String, payload String, tv String ALIAS concat(_table, ':', tenant)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_alias_virtual (tenant, payload) VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
CREATE TABLE m_alias_virtual (tenant String, payload String) ENGINE = Merge(currentDatabase(), '^t_alias_virtual$');
CREATE ROW POLICY p_05453_alias_virtual ON t_alias_virtual USING tv = 't_alias_virtual:a' TO ALL;
SELECT 'alias over virtual rows', payload FROM m_alias_virtual ORDER BY payload;

CREATE TABLE t_both (tenant String, region String, payload String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_both VALUES ('a', 'eu', '5'), ('a', 'eu', '20'), ('b', 'eu', 'secret'), ('a', 'eu', '30'), ('a', 'us', 'other');
CREATE TABLE m_both AS t_both ENGINE = Merge(currentDatabase(), '^t_both$');
CREATE ROW POLICY p_05453_both_child ON t_both USING tenant = 'a' TO ALL;
CREATE ROW POLICY p_05453_both_merge ON m_both USING region = 'eu' TO ALL;
SELECT 'both prewhere', count() FROM m_both PREWHERE toUInt64(payload) > 10;
SELECT 'both where', count() FROM m_both WHERE toUInt64(payload) > 10;
SELECT 'both rows', payload FROM m_both PREWHERE toUInt64(payload) > 10 ORDER BY payload;
SELECT 'both rows with region', region, payload FROM m_both PREWHERE toUInt64(payload) > 10 ORDER BY payload;

CREATE TABLE t_nested (tenant String, payload String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
CREATE TABLE m_nested_inner AS t_nested ENGINE = Merge(currentDatabase(), '^t_nested$');
CREATE TABLE m_nested_outer AS t_nested ENGINE = Merge(currentDatabase(), '^m_nested_inner$');
CREATE ROW POLICY p_05453_nested ON m_nested_inner USING tenant = 'a' TO ALL;
SELECT 'nested prewhere', count() FROM m_nested_outer PREWHERE toUInt64(payload) > 10;

CREATE TABLE t_nested_both (tenant String, region String, payload String) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested_both VALUES ('a', 'eu', '5'), ('a', 'eu', '20'), ('b', 'eu', 'secret'), ('a', 'eu', '30'), ('a', 'us', 'other');
CREATE TABLE m_nested_both_inner AS t_nested_both ENGINE = Merge(currentDatabase(), '^t_nested_both$');
CREATE TABLE m_nested_both_outer AS t_nested_both ENGINE = Merge(currentDatabase(), '^m_nested_both_inner$');
CREATE ROW POLICY p_05453_nested_both_child ON t_nested_both USING tenant = 'a' TO ALL;
CREATE ROW POLICY p_05453_nested_both_merge ON m_nested_both_inner USING region = 'eu' TO ALL;
SELECT 'nested both rows', payload FROM m_nested_both_outer ORDER BY payload;
SELECT 'nested both prewhere', count() FROM m_nested_both_outer PREWHERE toUInt64(payload) > 10;

DROP ROW POLICY p_05453_mt ON t_mt;
DROP ROW POLICY p_05453_memory ON t_memory;
DROP ROW POLICY p_05453_memory_default ON t_memory_default;
DROP ROW POLICY p_05453_final ON t_final;
DROP ROW POLICY p_05453_final_ver ON t_final_ver;
DROP ROW POLICY p_05453_in ON t_in;
DROP ROW POLICY p_05453_alias ON t_alias;
DROP ROW POLICY p_05453_alias_sub ON t_alias_sub;
DROP ROW POLICY p_05453_alias_lambda ON t_alias_lambda;
DROP ROW POLICY p_05453_alias_virtual ON t_alias_virtual;
DROP ROW POLICY p_05453_both_child ON t_both;
DROP ROW POLICY p_05453_both_merge ON m_both;
DROP ROW POLICY p_05453_nested ON m_nested_inner;
DROP ROW POLICY p_05453_nested_both_child ON t_nested_both;
DROP ROW POLICY p_05453_nested_both_merge ON m_nested_both_inner;
DROP TABLE m_mt, t_mt, m_memory, t_memory, m_memory_default, t_memory_default, m_final, t_final, t_final_ver, m_final_ver, m_in, t_in, t_in_dst, m_alias, t_alias, m_alias_sub, t_alias_sub, m_alias_lambda, t_alias_lambda, m_alias_virtual, t_alias_virtual, m_both, t_both,
    m_nested_outer, m_nested_inner, t_nested, m_nested_both_outer, m_nested_both_inner, t_nested_both;
