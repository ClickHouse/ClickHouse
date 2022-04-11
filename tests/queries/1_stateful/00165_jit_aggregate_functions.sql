SET compile_aggregate_expressions = 1;
SET min_count_to_compile_aggregate_expression = 0;

SELECT 'Aggregation using JIT compilation';

SELECT 'Simple functions';

SELECT
    CounterID,
    min(WatchID),
    max(WatchID),
    sum(WatchID),
    avg(WatchID),
    avgWeighted(WatchID, CounterID),
    count(WatchID),
    groupBitOr(WatchID),
    groupBitAnd(WatchID),
    groupBitXor(WatchID)
FROM test.hits
GROUP BY CounterID ORDER BY count() DESC LIMIT 20;

SELECT 'Simple functions with non compilable function';

SELECT
    CounterID,
    min(WatchID),
    max(WatchID),
    sum(WatchID),
    sum(toUInt128(WatchID)),
    avg(WatchID),
    avgWeighted(WatchID, CounterID),
    count(WatchID),
    groupBitOr(WatchID),
    groupBitAnd(WatchID),
    groupBitXor(WatchID)
FROM test.hits
GROUP BY CounterID ORDER BY count() DESC LIMIT 20;

SELECT 'Simple functions if combinator';

WITH (WatchID % 2 == 0) AS predicate
SELECT
    CounterID,
    minIf(WatchID,predicate),
    maxIf(WatchID, predicate),
    sumIf(WatchID, predicate),
    avgIf(WatchID, predicate),
    avgWeightedIf(WatchID, CounterID, predicate),
    countIf(WatchID, predicate),
    groupBitOrIf(WatchID, predicate),
    groupBitAndIf(WatchID, predicate),
    groupBitXorIf(WatchID, predicate)
FROM test.hits
GROUP BY CounterID ORDER BY count() DESC LIMIT 20;

SELECT 'Simple functions without key';

SELECT
    min(WatchID) AS min_watch_id,
    max(WatchID),
    sum(WatchID),
    avg(WatchID),
    avgWeighted(WatchID, CounterID),
    count(WatchID),
    groupBitOr(WatchID),
    groupBitAnd(WatchID),
    groupBitXor(WatchID)
FROM test.hits
ORDER BY min_watch_id DESC LIMIT 20;

SELECT 'Simple functions with non compilable function without key';

SELECT
    min(WatchID) AS min_watch_id,
    max(WatchID),
    sum(WatchID),
    sum(toUInt128(WatchID)),
    avg(WatchID),
    avgWeighted(WatchID, CounterID),
    count(WatchID),
    groupBitOr(WatchID),
    groupBitAnd(WatchID),
    groupBitXor(WatchID)
FROM test.hits
ORDER BY min_watch_id DESC LIMIT 20;

SELECT 'Simple functions if combinator without key';

WITH (WatchID % 2 == 0) AS predicate
SELECT
    minIf(WatchID, predicate) as min_watch_id,
    maxIf(WatchID, predicate),
    sumIf(WatchID, predicate),
    avgIf(WatchID, predicate),
    avgWeightedIf(WatchID, CounterID, predicate),
    countIf(WatchID, predicate),
    groupBitOrIf(WatchID, predicate),
    groupBitAndIf(WatchID, predicate),
    groupBitXorIf(WatchID, predicate)
FROM test.hits
ORDER BY min_watch_id
DESC LIMIT 20;

SET compile_aggregate_expressions = 0;

SELECT 'Aggregation without JIT compilation';

SELECT 'Simple functions';

SELECT
    CounterID,
    min(WatchID),
    max(WatchID),
    sum(WatchID),
    avg(WatchID),
    avgWeighted(WatchID, CounterID),
    count(WatchID),
    groupBitOr(WatchID),
    groupBitAnd(WatchID),
    groupBitXor(WatchID)
FROM test.hits
GROUP BY CounterID ORDER BY count() DESC LIMIT 20;

SELECT 'Simple functions with non compilable function';
SELECT
    CounterID,
    min(WatchID),
    max(WatchID),
    sum(WatchID),
    sum(toUInt128(WatchID)),
    avg(WatchID),
    avgWeighted(WatchID, CounterID),
    count(WatchID),
    groupBitOr(WatchID),
    groupBitAnd(WatchID),
    groupBitXor(WatchID)
FROM test.hits
GROUP BY CounterID ORDER BY count() DESC LIMIT 20;

SELECT 'Simple functions if combinator';

WITH (WatchID % 2 == 0) AS predicate
SELECT
    CounterID,
    minIf(WatchID,predicate),
    maxIf(WatchID, predicate),
    sumIf(WatchID, predicate),
    avgIf(WatchID, predicate),
    avgWeightedIf(WatchID, CounterID, predicate),
    countIf(WatchID, predicate),
    groupBitOrIf(WatchID, predicate),
    groupBitAndIf(WatchID, predicate),
    groupBitXorIf(WatchID, predicate)
FROM test.hits
GROUP BY CounterID ORDER BY count() DESC LIMIT 20;

SELECT 'Simple functions without key';

SELECT
    min(WatchID) AS min_watch_id,
    max(WatchID),
    sum(WatchID),
    avg(WatchID),
    avgWeighted(WatchID, CounterID),
    count(WatchID),
    groupBitOr(WatchID),
    groupBitAnd(WatchID),
    groupBitXor(WatchID)
FROM test.hits
ORDER BY min_watch_id DESC LIMIT 20;

SELECT 'Simple functions with non compilable function without key';

SELECT
    min(WatchID) AS min_watch_id,
    max(WatchID),
    sum(WatchID),
    sum(toUInt128(WatchID)),
    avg(WatchID),
    avgWeighted(WatchID, CounterID),
    count(WatchID),
    groupBitOr(WatchID),
    groupBitAnd(WatchID),
    groupBitXor(WatchID)
FROM test.hits
ORDER BY min_watch_id DESC LIMIT 20;

SELECT 'Simple functions if combinator without key';

WITH (WatchID % 2 == 0) AS predicate
SELECT
    minIf(WatchID, predicate) as min_watch_id,
    maxIf(WatchID, predicate),
    sumIf(WatchID, predicate),
    avgIf(WatchID, predicate),
    avgWeightedIf(WatchID, CounterID, predicate),
    countIf(WatchID, predicate),
    groupBitOrIf(WatchID, predicate),
    groupBitAndIf(WatchID, predicate),
    groupBitXorIf(WatchID, predicate)
FROM test.hits
ORDER BY min_watch_id
DESC LIMIT 20;
