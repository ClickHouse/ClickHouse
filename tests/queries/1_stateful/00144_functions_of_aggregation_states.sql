SET allow_deprecated_error_prone_window_functions = 1;

SELECT EventDate, finalizeAggregation(state), runningAccumulate(state) FROM (SELECT EventDate, uniqState(UserID) AS state FROM test.hits GROUP BY EventDate ORDER BY EventDate);
