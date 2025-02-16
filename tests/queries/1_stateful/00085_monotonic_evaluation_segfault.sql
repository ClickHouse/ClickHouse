SELECT any(0) FROM test.visits WHERE (toInt32(toDateTime(StartDate))) > 1000000000;
