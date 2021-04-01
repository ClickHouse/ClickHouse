SET max_rows_to_read = 1000;
SELECT CounterID, uniq(UserID) FROM test.hits WHERE 0 != 0 GROUP BY CounterID;
SELECT CounterID, uniq(UserID) FROM test.hits WHERE 0 != 0 GROUP BY CounterID SETTINGS optimize_aggregation_in_order = 1;
SELECT CounterID, uniq(UserID) FROM test.hits WHERE 0 AND CounterID = 1704509 GROUP BY CounterID;
SELECT CounterID, uniq(UserID) FROM test.hits WHERE 0 AND CounterID = 1704509 GROUP BY CounterID SETTINGS optimize_aggregation_in_order = 1;
