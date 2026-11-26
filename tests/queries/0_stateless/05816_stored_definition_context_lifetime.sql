-- Tags: no-fasttest
-- no-fasttest: `transactionLatestSnapshot` needs the transaction log.
-- The function keeps its context weakly and is not constant-folded in a distributed query.

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS t_alias;
CREATE TABLE t (id Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1), (2), (3);
CREATE TABLE t_alias (id Int64, a UInt64 ALIAS transactionLatestSnapshot()) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_alias VALUES (1), (2), (3);
CREATE ROW POLICY 05816_p ON t USING transactionLatestSnapshot() >= 0 TO ALL;

SELECT count() FROM remote('127.0.0.1', currentDatabase(), t) SETTINGS enable_table_namespaces = 0;
SELECT count() FROM remote('127.0.0.1', currentDatabase(), t) SETTINGS enable_table_namespaces = 1;
SELECT count() FROM remote('127.0.0.1', currentDatabase(), t_alias) WHERE a >= 0 SETTINGS enable_table_namespaces = 0;
SELECT count() FROM remote('127.0.0.1', currentDatabase(), t_alias) WHERE a >= 0 SETTINGS enable_table_namespaces = 1;

DROP ROW POLICY 05816_p ON t;
DROP TABLE t;
DROP TABLE t_alias;
