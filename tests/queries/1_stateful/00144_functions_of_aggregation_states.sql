SELECT EventDate, finalizeAggregation(state), runningAccumulate(state) FROM (SELECT EventDate, uniqState(UserID) AS state FROM test.hits GROUP BY EventDate ORDER BY EventDate);
