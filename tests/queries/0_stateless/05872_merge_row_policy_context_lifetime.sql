-- Tags: no-fasttest
-- no-fasttest: `transactionLatestSnapshot` needs the transaction log.
-- The function keeps its context weakly and is not constant-folded in a distributed query.

DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS m;
CREATE TABLE t (id Int64) ENGINE = MergeTree ORDER BY id;
INSERT INTO t VALUES (1), (2), (3);
CREATE TABLE m AS t ENGINE = Merge(currentDatabase(), '^t$');
CREATE ROW POLICY 05872_p ON t USING transactionLatestSnapshot() >= 0 TO ALL;

SELECT count() FROM remote('127.0.0.1', currentDatabase(), m);

DROP ROW POLICY 05872_p ON t;
DROP TABLE m;
DROP TABLE t;
