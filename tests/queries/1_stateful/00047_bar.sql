SELECT CounterID, count() AS c, bar(c, 0, 523264) FROM test.hits GROUP BY CounterID ORDER BY c DESC, CounterID ASC LIMIT 100;
SELECT CounterID, count() AS c, bar(c, 0, 523264) FROM test.hits GROUP BY CounterID ORDER BY c DESC, CounterID ASC LIMIT 100 SETTINGS optimize_aggregation_in_order = 1
