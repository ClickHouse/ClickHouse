SELECT EventDate, uniqExact(UserID), length(groupUniqArray(UserID)), arrayUniq(groupArray(UserID)) FROM test.hits WHERE CounterID = 1704509 GROUP BY EventDate ORDER BY EventDate;
