DROP TABLE IF EXISTS test.view;
CREATE VIEW test.view AS SELECT CounterID, count() AS c FROM test.hits GROUP BY CounterID;
SELECT count() FROM test.view;
SELECT c, count() FROM test.view GROUP BY c ORDER BY count() DESC LIMIT 10;
SELECT * FROM test.view ORDER BY c DESC LIMIT 10;
SELECT * FROM test.view SAMPLE 0.1 ORDER BY c DESC LIMIT 10;
DROP TABLE test.view;
