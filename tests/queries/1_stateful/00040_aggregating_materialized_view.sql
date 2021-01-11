DROP TABLE IF EXISTS test.basic_00040;

CREATE MATERIALIZED VIEW test.basic_00040
ENGINE = AggregatingMergeTree(StartDate, (CounterID, StartDate), 8192)
POPULATE AS
SELECT
    CounterID,
    StartDate,
    sumState(Sign) 		AS Visits,
    uniqState(UserID)	AS Users
FROM test.visits
GROUP BY CounterID, StartDate;


SELECT
    StartDate,
    sumMerge(Visits)	AS Visits,
    uniqMerge(Users)	AS Users
FROM test.basic_00040
GROUP BY StartDate
ORDER BY StartDate;


SELECT
    StartDate,
    sumMerge(Visits)	AS Visits,
    uniqMerge(Users)	AS Users
FROM test.basic_00040
WHERE CounterID = 942285
GROUP BY StartDate
ORDER BY StartDate;


SELECT
    StartDate,
    sum(Sign) 			AS Visits,
    uniq(UserID)		AS Users
FROM test.visits
WHERE CounterID = 942285
GROUP BY StartDate
ORDER BY StartDate;


DROP TABLE test.basic_00040;
