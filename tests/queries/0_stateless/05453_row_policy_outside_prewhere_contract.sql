-- Tags: no-fasttest
-- Tag no-fasttest: Parquet
-- Random settings limits: query_plan_merge_filters=(1, None)

-- A row policy on a column outside the PREWHERE contract of the read (here a column with a DEFAULT) is applied after the
-- read, so neither PREWHERE nor a condition moved to PREWHERE may run before it on the rows it hides.

SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS m_child_policy;
DROP TABLE IF EXISTS m_own_policy;
DROP TABLE IF EXISTS m_bare_policy;
DROP TABLE IF EXISTS m_alias;
DROP TABLE IF EXISTS m_log_bare;
DROP TABLE IF EXISTS t_memory;
DROP TABLE IF EXISTS t_file;
DROP TABLE IF EXISTS t_order_sensitive_policy;
DROP TABLE IF EXISTS t_stateful_policy;
DROP TABLE IF EXISTS t_no_policy;
DROP TABLE IF EXISTS t_bare_policy;
DROP TABLE IF EXISTS t_bare_chain_policy;
DROP TABLE IF EXISTS t_numbers;
DROP TABLE IF EXISTS t_join;
DROP TABLE IF EXISTS j_join;
DROP TABLE IF EXISTS t_join_partial;
DROP TABLE IF EXISTS t_np_mt;
DROP TABLE IF EXISTS t_alias;
DROP TABLE IF EXISTS t_alias_prewhere;
DROP TABLE IF EXISTS t_log_bare;
DROP VIEW IF EXISTS v_sealed;
DROP TABLE IF EXISTS t_view_base;

CREATE TABLE t_memory (tenant String DEFAULT 'a', payload String) ENGINE = Memory;
CREATE TABLE t_file (tenant String DEFAULT 'a', payload String) ENGINE = File(Parquet);
CREATE TABLE t_order_sensitive_policy (tenant String DEFAULT 'a', payload String) ENGINE = Memory;
CREATE TABLE t_stateful_policy (tenant String DEFAULT 'a', payload String) ENGINE = Memory;
CREATE TABLE t_no_policy (tenant String DEFAULT 'a', payload String) ENGINE = Memory;
CREATE TABLE t_bare_policy (visible UInt8 DEFAULT 1, payload String) ENGINE = Memory;
CREATE TABLE t_bare_chain_policy (visible UInt8 DEFAULT 1, payload String) ENGINE = Memory;
CREATE TABLE t_numbers (x UInt64) ENGINE = Memory;
CREATE TABLE t_join (id UInt64, tenant String DEFAULT 'a', payload String) ENGINE = Memory;
CREATE TABLE j_join (id UInt64) ENGINE = Memory;
CREATE TABLE t_join_partial (id UInt64, tenant String DEFAULT 'a', payload String) ENGINE = Memory;
CREATE TABLE t_np_mt (k UInt64, flag UInt8, s String) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t_alias (tenant String DEFAULT 'a', payload String, parsed UInt64 ALIAS toUInt64(payload)) ENGINE = Memory;
CREATE TABLE t_alias_prewhere (tenant String DEFAULT 'a', payload String, parsed UInt64 ALIAS toUInt64(payload)) ENGINE = Memory;
CREATE TABLE t_log_bare (visible UInt8, payload String) ENGINE = Log;
CREATE TABLE t_view_base (visible UInt8, payload String) ENGINE = Memory;
INSERT INTO t_memory VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
INSERT INTO t_file VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
INSERT INTO t_order_sensitive_policy VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
INSERT INTO t_stateful_policy VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
INSERT INTO t_no_policy VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
INSERT INTO t_bare_policy VALUES (1, '5'), (1, '20'), (0, 'secret'), (1, '30');
INSERT INTO t_bare_chain_policy VALUES (1, '5'), (1, '20'), (0, 'secret'), (1, '30');
INSERT INTO t_numbers SELECT number FROM numbers(10);
INSERT INTO t_join VALUES (1, 'a', '5'), (2, 'a', '20'), (3, 'b', 'secret'), (4, 'a', '30');
INSERT INTO j_join VALUES (1), (2), (3), (4);
INSERT INTO t_join_partial VALUES (1, 'a', '5'), (2, 'a', '20'), (3, 'b', 'secret'), (4, 'a', '30');
INSERT INTO t_np_mt VALUES (1, 1, '5'), (2, 1, '20'), (3, 0, 'x'), (4, 1, '30');
INSERT INTO t_alias (tenant, payload) VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
INSERT INTO t_alias_prewhere (tenant, payload) VALUES ('a', '5'), ('a', '20'), ('a', 'bad'), ('a', '30');
INSERT INTO t_log_bare VALUES (1, '5'), (1, '20'), (0, 'secret'), (1, '30');
INSERT INTO t_view_base VALUES (1, '5'), (1, '20'), (0, 'secret'), (1, '30');
CREATE TABLE m_child_policy AS t_memory ENGINE = Merge(currentDatabase(), '^t_memory$');
CREATE TABLE m_own_policy AS t_no_policy ENGINE = Merge(currentDatabase(), '^t_no_policy$');
CREATE TABLE m_bare_policy AS t_bare_policy ENGINE = Merge(currentDatabase(), '^t_bare_policy$');
CREATE TABLE m_alias (tenant String, payload String, parsed UInt64) ENGINE = Merge(currentDatabase(), '^t_alias$');
CREATE TABLE m_log_bare (visible UInt8, payload String) ENGINE = Merge(currentDatabase(), '^t_log_bare$');
CREATE VIEW v_sealed DEFINER = CURRENT_USER SQL SECURITY DEFINER AS SELECT visible, payload FROM t_view_base;

CREATE ROW POLICY p_05453_memory ON t_memory USING tenant = 'a' TO ALL;
CREATE ROW POLICY p_05453_file ON t_file USING tenant = 'a' TO ALL;
CREATE ROW POLICY p_05453_merge ON m_own_policy USING tenant = 'a' TO ALL;
CREATE ROW POLICY p_05453_order_sensitive ON t_order_sensitive_policy USING tenant = 'a' AND rand() >= 0 TO ALL;
CREATE ROW POLICY p_05453_stateful ON t_stateful_policy USING tenant = 'a' AND rowNumberInBlock() < 1000000 TO ALL;
CREATE ROW POLICY p_05453_bare ON t_bare_policy USING visible TO ALL;
CREATE ROW POLICY p_05453_bare_chain ON t_bare_chain_policy USING visible AND toUInt64(payload) < 1000 TO ALL;
CREATE ROW POLICY p_05453_join ON t_join USING tenant = 'a' TO ALL;
CREATE ROW POLICY p_05453_join_partial ON t_join_partial USING tenant = 'a' AND payload != 'zzz' TO ALL;
CREATE ROW POLICY p_05453_alias ON t_alias USING tenant = 'a' TO ALL;
CREATE ROW POLICY p_05453_alias_prewhere ON t_alias_prewhere USING tenant = 'a' TO ALL;
CREATE ROW POLICY p_05453_log ON t_log_bare USING visible TO ALL;
CREATE ROW POLICY p_05453_sealed ON v_sealed USING visible TO ALL;

SELECT '-- Memory';
SELECT count() FROM t_memory WHERE toUInt64(payload) > 10;
SELECT count() FROM t_memory PREWHERE toUInt64(payload) > 10;
SELECT count() FROM t_memory WHERE NOT throwIf(payload = 'secret');
SELECT count() FROM t_memory WHERE toUInt64(payload) > 10 SETTINGS short_circuit_function_evaluation = 'disable';
SELECT count() FROM t_memory WHERE toUInt64(payload) > 10 SETTINGS query_plan_merge_filters = 0;

SELECT '-- File';
SELECT count() FROM t_file WHERE toUInt64(payload) > 10;
SELECT count() FROM t_file PREWHERE toUInt64(payload) > 10;

SELECT '-- Merge, policy on the child';
SELECT count() FROM m_child_policy WHERE toUInt64(payload) > 10;
SELECT count() FROM m_child_policy PREWHERE toUInt64(payload) > 10;

SELECT '-- Merge, policy on the Merge table';
SELECT count() FROM m_own_policy WHERE toUInt64(payload) > 10;
SELECT count() FROM m_own_policy PREWHERE toUInt64(payload) > 10;

SELECT '-- a condition that cannot throw is still moved to PREWHERE';
SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM t_memory WHERE payload != 'zzz'
    SETTINGS query_plan_enable_optimizations = 1, query_plan_merge_filters = 1) WHERE explain ILIKE '%Prewhere filter column%';
SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM t_memory WHERE toUInt64(payload) > 10
    SETTINGS query_plan_enable_optimizations = 1, query_plan_merge_filters = 1) WHERE explain ILIKE '%Prewhere filter column%';

SELECT '-- an order-sensitive policy';
SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM t_order_sensitive_policy WHERE payload != 'zzz'
    SETTINGS query_plan_enable_optimizations = 1, query_plan_merge_filters = 1) WHERE explain ILIKE '%Prewhere filter column%';
SELECT count() FROM t_order_sensitive_policy;

SELECT '-- a stateful policy or a stateful condition of the query';
SELECT count() FROM t_stateful_policy WHERE toUInt64(payload) > 10 SETTINGS short_circuit_function_evaluation = 'disable';
SELECT count() FROM t_memory WHERE rowNumberInBlock() >= 0 AND toUInt64(payload) > 10 SETTINGS short_circuit_function_evaluation = 'disable';

SELECT '-- a policy that is a column';
SELECT count() FROM t_bare_policy WHERE toUInt64(payload) > 10 SETTINGS short_circuit_function_evaluation = 'disable', query_plan_merge_filters = 1;
SELECT count() FROM m_bare_policy WHERE toUInt64(payload) > 10 SETTINGS short_circuit_function_evaluation = 'disable', query_plan_merge_filters = 1;
SELECT count() FROM t_bare_policy WHERE toUInt64(payload) > 10 AND toUInt64(payload) < 100 SETTINGS query_plan_merge_filters = 1;
SELECT count() FROM m_bare_policy WHERE toUInt64(payload) > 10 AND toUInt64(payload) < 100 SETTINGS query_plan_merge_filters = 1;
SELECT count() FROM t_bare_policy PREWHERE toUInt64(payload) > 10 AND toUInt64(payload) < 100 SETTINGS query_plan_merge_filters = 1;
SELECT count() FROM t_bare_policy SETTINGS additional_table_filters = {'t_bare_policy': 'toUInt64(payload) > 10 AND toUInt64(payload) < 100'}, query_plan_merge_filters = 1;
SELECT count() FROM t_bare_chain_policy WHERE payload != 'zzz'
    SETTINGS query_plan_merge_filters = 1, optimize_move_to_prewhere = 0, short_circuit_function_evaluation = 'enable', short_circuit_function_evaluation_reorder_arguments = 0;
SELECT sum(n) FROM (SELECT toUInt64(payload) AS n, payload FROM t_bare_policy) WHERE payload != 'zzz'
    SETTINGS query_plan_filter_push_down = 0, query_plan_split_filter = 0, query_plan_merge_filters = 1, optimize_move_to_prewhere = 0;

SELECT '-- a stateful condition is not merged into the filter below it';
SELECT groupArray(x) FROM (SELECT x FROM (SELECT x FROM t_numbers WHERE x % 2 = 1) WHERE rowNumberInAllBlocks() < 2)
    SETTINGS max_threads = 1, query_plan_merge_filters = 1, optimize_move_to_prewhere = 0;

SELECT '-- a filter merged although query_plan_merge_filters = 0';
SELECT count() FROM t_join AS t INNER JOIN j_join AS j USING id WHERE toUInt64(t.payload) > 10
    SETTINGS query_plan_merge_filters = 0, short_circuit_function_evaluation = 'disable', enable_join_runtime_filters = 1,
        join_runtime_filter_min_probe_rows = 0, query_plan_join_swap_table = 'false';

SELECT '-- without a policy, a filter is still merged into one with a column atom';
SELECT count() FROM (EXPLAIN actions = 1 SELECT count() FROM (SELECT * FROM t_np_mt WHERE flag) WHERE toUInt64(s) > 10
    SETTINGS query_plan_enable_optimizations = 1, query_plan_merge_filters = 1, move_all_conditions_to_prewhere = 1)
    WHERE explain ILIKE '%Prewhere filter column%toUInt64%';

SELECT '-- an ALIAS column is computed after the policy';
SELECT count() FROM t_alias WHERE parsed > 10 SETTINGS query_plan_enable_optimizations = 0;
SELECT sum(parsed) FROM t_alias SETTINGS query_plan_enable_optimizations = 0;
SELECT count() FROM m_alias WHERE parsed > 10 SETTINGS query_plan_enable_optimizations = 0;
SELECT sum(parsed) FROM m_alias SETTINGS query_plan_enable_optimizations = 0;
SELECT sum(parsed) FROM t_alias_prewhere PREWHERE payload != 'bad' SETTINGS query_plan_enable_optimizations = 0;

SELECT '-- a read that keeps no query info';
SELECT count() FROM t_log_bare WHERE toUInt64(payload) > 10 AND toUInt64(payload) < 100;
SELECT count() FROM v_sealed WHERE toUInt64(payload) > 10 AND toUInt64(payload) < 100;
SELECT count() FROM t_log_bare WHERE toUInt64(payload) > 10 AND toUInt64(payload) < 100
    SETTINGS additional_table_filters = {'t_log_bare': 'payload != \'zzz\''};
SELECT count() FROM m_log_bare WHERE toUInt64(payload) > 10 AND toUInt64(payload) < 100;

SELECT '-- a serialized query plan';
SELECT count() FROM cluster('test_shard_localhost', currentDatabase(), t_memory) WHERE toUInt64(payload) > 10
    SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;
SELECT count() FROM cluster('test_shard_localhost', currentDatabase(), t_memory) PREWHERE toUInt64(payload) > 10
    SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;
SELECT count() FROM cluster('test_shard_localhost', currentDatabase(), t_log_bare) WHERE toUInt64(payload) > 10 AND toUInt64(payload) < 100
    SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;
SELECT count() FROM cluster('test_shard_localhost', currentDatabase(), t_bare_chain_policy)
    SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0;
SELECT count() FROM cluster('test_shard_localhost', currentDatabase(), t_join_partial) AS t INNER JOIN j_join AS j USING id
    WHERE toUInt64(t.payload) > 10
    SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, query_plan_merge_filters = 0, short_circuit_function_evaluation = 'disable',
        enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0, query_plan_join_swap_table = 'false';
SELECT id FROM cluster('test_shard_localhost', currentDatabase(), t_join) WHERE toUInt64(payload) > 10 ORDER BY payload LIMIT 3
    SETTINGS serialize_query_plan = 1, prefer_localhost_replica = 0, query_plan_optimize_lazy_materialization = 1,
        query_plan_max_limit_for_lazy_materialization = 10, query_plan_merge_filters = 0, short_circuit_function_evaluation = 'disable';

SELECT '-- without a policy the row is in scope';
SELECT count() FROM t_no_policy WHERE toUInt64(payload) > 10; -- { serverError CANNOT_PARSE_TEXT }

DROP ROW POLICY p_05453_memory ON t_memory;
DROP ROW POLICY p_05453_file ON t_file;
DROP ROW POLICY p_05453_merge ON m_own_policy;
DROP ROW POLICY p_05453_order_sensitive ON t_order_sensitive_policy;
DROP ROW POLICY p_05453_stateful ON t_stateful_policy;
DROP ROW POLICY p_05453_bare ON t_bare_policy;
DROP ROW POLICY p_05453_bare_chain ON t_bare_chain_policy;
DROP ROW POLICY p_05453_join ON t_join;
DROP ROW POLICY p_05453_join_partial ON t_join_partial;
DROP ROW POLICY p_05453_alias ON t_alias;
DROP ROW POLICY p_05453_alias_prewhere ON t_alias_prewhere;
DROP ROW POLICY p_05453_log ON t_log_bare;
DROP ROW POLICY p_05453_sealed ON v_sealed;
DROP TABLE m_child_policy;
DROP TABLE m_own_policy;
DROP TABLE m_bare_policy;
DROP TABLE m_alias;
DROP TABLE m_log_bare;
DROP TABLE t_memory;
DROP TABLE t_file;
DROP TABLE t_order_sensitive_policy;
DROP TABLE t_stateful_policy;
DROP TABLE t_no_policy;
DROP TABLE t_bare_policy;
DROP TABLE t_bare_chain_policy;
DROP TABLE t_numbers;
DROP TABLE t_join;
DROP TABLE j_join;
DROP TABLE t_join_partial;
DROP TABLE t_np_mt;
DROP TABLE t_alias;
DROP TABLE t_alias_prewhere;
DROP VIEW v_sealed;
DROP TABLE t_view_base;
DROP TABLE t_log_bare;
