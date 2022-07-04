DROP TABLE IF EXISTS test.basic;
DROP TABLE IF EXISTS test.visits_null;

CREATE TABLE test.visits_null
(
    CounterID UInt32,
    StartDate Date,
    Sign Int8,
    UserID UInt64
) ENGINE = Null;

CREATE MATERIALIZED VIEW test.basic
ENGINE = AggregatingMergeTree(StartDate, (CounterID, StartDate), 8192)
AS SELECT
    CounterID,
    StartDate,
    sumState(Sign)                  AS Visits,
    uniqState(UserID)               AS Users
FROM test.visits_null
GROUP BY CounterID, StartDate;

INSERT INTO test.visits_null
SELECT
    CounterID,
    StartDate,
    Sign,
    UserID
FROM test.visits;


SELECT
    StartDate,
    sumMerge(Visits)                AS Visits,
    uniqMerge(Users)                AS Users
FROM test.basic
GROUP BY StartDate
ORDER BY StartDate;


SELECT
    StartDate,
    sumMerge(Visits)                AS Visits,
    uniqMerge(Users)                AS Users
FROM test.basic
WHERE CounterID = 942285
GROUP BY StartDate
ORDER BY StartDate;


SELECT
    StartDate,
    sum(Sign)                       AS Visits,
    uniq(UserID)                    AS Users
FROM test.visits
WHERE CounterID = 942285
GROUP BY StartDate
ORDER BY StartDate;


OPTIMIZE TABLE test.basic;
OPTIMIZE TABLE test.basic;
OPTIMIZE TABLE test.basic;
OPTIMIZE TABLE test.basic;
OPTIMIZE TABLE test.basic;
OPTIMIZE TABLE test.basic;
OPTIMIZE TABLE test.basic;
OPTIMIZE TABLE test.basic;
OPTIMIZE TABLE test.basic;
OPTIMIZE TABLE test.basic;


DROP TABLE test.visits_null;
DROP TABLE test.basic;
