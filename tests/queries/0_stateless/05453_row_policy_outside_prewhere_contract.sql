-- Tags: no-fasttest
-- Tag no-fasttest: Parquet

-- A row policy on a column outside the PREWHERE contract of the read (here a column with a DEFAULT) is applied after the
-- read, so neither PREWHERE nor a condition moved to PREWHERE may run before it on the rows it hides.

SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS m_child_policy;
DROP TABLE IF EXISTS m_own_policy;
DROP TABLE IF EXISTS t_memory;
DROP TABLE IF EXISTS t_file;
DROP TABLE IF EXISTS t_order_sensitive_policy;
DROP TABLE IF EXISTS t_stateful_policy;
DROP TABLE IF EXISTS t_no_policy;

CREATE TABLE t_memory (tenant String DEFAULT 'a', payload String) ENGINE = Memory;
CREATE TABLE t_file (tenant String DEFAULT 'a', payload String) ENGINE = File(Parquet);
CREATE TABLE t_order_sensitive_policy (tenant String DEFAULT 'a', payload String) ENGINE = Memory;
CREATE TABLE t_stateful_policy (tenant String DEFAULT 'a', payload String) ENGINE = Memory;
CREATE TABLE t_no_policy (tenant String DEFAULT 'a', payload String) ENGINE = Memory;
INSERT INTO t_memory VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
INSERT INTO t_file VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
INSERT INTO t_order_sensitive_policy VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
INSERT INTO t_stateful_policy VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
INSERT INTO t_no_policy VALUES ('a', '5'), ('a', '20'), ('b', 'secret'), ('a', '30');
CREATE TABLE m_child_policy AS t_memory ENGINE = Merge(currentDatabase(), '^t_memory$');
CREATE TABLE m_own_policy AS t_no_policy ENGINE = Merge(currentDatabase(), '^t_no_policy$');

CREATE ROW POLICY p_05453_memory ON t_memory USING tenant = 'a' TO ALL;
CREATE ROW POLICY p_05453_file ON t_file USING tenant = 'a' TO ALL;
CREATE ROW POLICY p_05453_merge ON m_own_policy USING tenant = 'a' TO ALL;
CREATE ROW POLICY p_05453_order_sensitive ON t_order_sensitive_policy USING tenant = 'a' AND rand() >= 0 TO ALL;
CREATE ROW POLICY p_05453_stateful ON t_stateful_policy USING tenant = 'a' AND rowNumberInBlock() < 1000000 TO ALL;

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

SELECT '-- a stateful policy';
SELECT count() FROM t_stateful_policy WHERE toUInt64(payload) > 10 SETTINGS short_circuit_function_evaluation = 'disable';

SELECT '-- without a policy the row is in scope';
SELECT count() FROM t_no_policy WHERE toUInt64(payload) > 10; -- { serverError CANNOT_PARSE_TEXT }

DROP ROW POLICY p_05453_memory ON t_memory;
DROP ROW POLICY p_05453_file ON t_file;
DROP ROW POLICY p_05453_merge ON m_own_policy;
DROP ROW POLICY p_05453_order_sensitive ON t_order_sensitive_policy;
DROP ROW POLICY p_05453_stateful ON t_stateful_policy;
DROP TABLE m_child_policy;
DROP TABLE m_own_policy;
DROP TABLE t_memory;
DROP TABLE t_file;
DROP TABLE t_order_sensitive_policy;
DROP TABLE t_stateful_policy;
DROP TABLE t_no_policy;
