SELECT CounterID, min(WatchID), max(WatchID) FROM test.hits GROUP BY CounterID ORDER BY count() DESC LIMIT 20
