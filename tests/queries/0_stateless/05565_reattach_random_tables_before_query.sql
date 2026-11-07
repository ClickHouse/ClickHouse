-- Tags: no-replicated-database
-- Tag no-replicated-database: tables of `Replicated` databases are not reattached.

-- `reattach_tables_before_query_execution_probability` reattaches tables of the current database before every query,
-- of any kind. A `Buffer` table with high thresholds shows it: it flushes into its destination only on shutdown.

CREATE TABLE dst (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE buf (x UInt64) ENGINE = Buffer(currentDatabase(), dst, 1, 100000, 100000, 1000000, 1000000, 1000000000, 1000000000);

-- These tables must not be reattached: their data or state would be lost.
CREATE TABLE mem (x UInt64) ENGINE = Memory;
CREATE TABLE set_not_persistent (x UInt64) ENGINE = Set SETTINGS persistent = 0;
CREATE TABLE stopped (x UInt64) ENGINE = MergeTree ORDER BY x;

-- These tables are reattached and keep their data.
CREATE TABLE mt (x UInt64) ENGINE = MergeTree ORDER BY x;
CREATE TABLE log (x UInt64) ENGINE = TinyLog;
CREATE TABLE set_persistent (x UInt64) ENGINE = Set;
CREATE VIEW v AS SELECT x FROM mt;
CREATE MATERIALIZED VIEW mv ENGINE = MergeTree ORDER BY x AS SELECT x * 10 AS x FROM mt;
CREATE DICTIONARY dict (x UInt64) PRIMARY KEY x SOURCE(CLICKHOUSE(TABLE 'mt')) LAYOUT(FLAT()) LIFETIME(0);

INSERT INTO buf VALUES (1);
INSERT INTO mem VALUES (1);
INSERT INTO set_not_persistent VALUES (1);
SYSTEM STOP MERGES stopped;
INSERT INTO stopped VALUES (1);
INSERT INTO stopped VALUES (2);
INSERT INTO mt VALUES (1);
INSERT INTO log VALUES (1);
INSERT INTO set_persistent VALUES (1);

SELECT 'before', count() FROM dst;

SET reattach_tables_before_query_execution_probability = 1;

SELECT 'SELECT', count() FROM dst;

INSERT INTO buf SETTINGS reattach_tables_before_query_execution_probability = 0 VALUES (2);
EXPLAIN SYNTAX SELECT 1;
SELECT 'EXPLAIN', count() FROM dst SETTINGS reattach_tables_before_query_execution_probability = 0;

INSERT INTO buf SETTINGS reattach_tables_before_query_execution_probability = 0 VALUES (3);
SELECT throwIf(1); -- { serverError FUNCTION_THROW_IF_VALUE_IS_NON_ZERO }
SELECT 'failed query', count() FROM dst SETTINGS reattach_tables_before_query_execution_probability = 0;

INSERT INTO buf SETTINGS reattach_tables_before_query_execution_probability = 0 VALUES (4);
SELECT 'not reattached with zero probability', count() FROM dst SETTINGS reattach_tables_before_query_execution_probability = 0;

SELECT 'Memory', count() FROM mem;
SELECT 'Set, persistent = 0', 1 IN set_not_persistent;
SELECT 'stopped merges', count() FROM system.parts WHERE database = currentDatabase() AND table = 'stopped' AND active;

INSERT INTO mt VALUES (2);
SELECT 'MergeTree', count() FROM mt;
SELECT 'TinyLog', count() FROM log;
SELECT 'Set', 1 IN set_persistent;
SELECT 'View', count() FROM v;
SELECT 'MaterializedView', groupArray(x) FROM (SELECT x FROM mv ORDER BY x);
SELECT 'Dictionary', dictHas('dict', 2);
