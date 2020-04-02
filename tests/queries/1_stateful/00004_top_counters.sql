SELECT CounterID, count() AS c FROM test.hits GROUP BY CounterID ORDER BY c DESC LIMIT 10
