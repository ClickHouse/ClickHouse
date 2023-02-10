-- Tags: no-replicated-database, no-parallel-replicas
-- Tag no-replicated-database: Requires investigation

EXPLAIN ESTIMATE SELECT count() FROM test.hits WHERE CounterID = 29103473;
EXPLAIN ESTIMATE SELECT count() FROM test.hits WHERE CounterID != 29103473;
EXPLAIN ESTIMATE SELECT count() FROM test.hits WHERE CounterID > 29103473;
EXPLAIN ESTIMATE SELECT count() FROM test.hits WHERE CounterID < 29103473;
EXPLAIN ESTIMATE SELECT count() FROM test.hits WHERE CounterID = 29103473 UNION ALL SELECT count() FROM test.hits WHERE CounterID = 1704509;
