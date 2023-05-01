SELECT uniq(UserID), uniqIf(UserID, CounterID = 800784), uniqIf(FUniqID, RegionID = 213) FROM test.hits
