SET aggregate_functions_null_for_empty = 0;

DROP TABLE IF EXISTS aggregate_merge_or_null;
CREATE TABLE aggregate_merge_or_null (a Int32, s AggregateFunction(sum, Int32), n AggregateFunction(sumOrNull, Int32)) ENGINE = Memory;
INSERT INTO aggregate_merge_or_null SELECT number, sumState(toInt32(number)), sumOrNullState(toInt32(number)) FROM numbers(101) GROUP BY number;

-- sum states
SELECT sumMergeOrNull(s) FROM aggregate_merge_or_null WHERE a < 0;
SELECT sumMergeOrNull(s) FROM aggregate_merge_or_null;
SELECT toTypeName(sumMergeStateOrNull(s)), finalizeAggregation(sumMergeStateOrNull(s)) FROM aggregate_merge_or_null WHERE a < 0;
SELECT toTypeName(sumMergeStateOrNull(s)), finalizeAggregation(sumMergeStateOrNull(s)) FROM aggregate_merge_or_null;
SELECT sumMergeIfOrNull(s, a < 0), sumMergeOrNullIf(s, a < 0) FROM aggregate_merge_or_null;

SET aggregate_functions_null_for_empty = 1;

SELECT sumMergeOrNull(s) FROM aggregate_merge_or_null WHERE a < 0;
SELECT sumMergeOrNull(s) FROM aggregate_merge_or_null;
SELECT toTypeName(sumMergeStateOrNull(s)), finalizeAggregation(sumMergeStateOrNull(s)) FROM aggregate_merge_or_null;
SELECT sumMerge(s) FROM aggregate_merge_or_null WHERE a < 0;
SELECT sumMerge(s) FROM aggregate_merge_or_null;

-- sumOrNull states
SET aggregate_functions_null_for_empty = 0;

SELECT sumMergeOrNull(n) FROM aggregate_merge_or_null WHERE a < 0;
SELECT sumMergeOrNull(n) FROM aggregate_merge_or_null;
SELECT toTypeName(sumMergeStateOrNull(n)), finalizeAggregation(sumMergeStateOrNull(n)) FROM aggregate_merge_or_null WHERE a < 0;
SELECT toTypeName(sumMergeStateOrNull(n)), finalizeAggregation(sumMergeStateOrNull(n)) FROM aggregate_merge_or_null;

SET aggregate_functions_null_for_empty = 1;

SELECT sumMerge(n) FROM aggregate_merge_or_null WHERE a < 0;
SELECT sumMergeOrNull(n) FROM aggregate_merge_or_null;
SELECT sumMergeOrNull(s) FROM (SELECT sumState(number) AS s FROM numbers(0));
SELECT sumMergeOrNull(s) FROM (SELECT sumState(number) AS s FROM numbers(3));
SELECT finalizeAggregation(sumMergeStateOrNull(s)) FROM (SELECT sumState(number) AS s FROM numbers(3));

-- explicit OrNull producers, producer gains another OrNull
SELECT sumOrNullMerge(s) FROM (SELECT sumOrNullState(number) AS s FROM numbers(0));
SELECT sumOrNullMerge(s) FROM (SELECT sumOrNullState(number) AS s FROM numbers(3));
SELECT finalizeAggregation(sumOrNullMergeState(s)) FROM (SELECT sumOrNullState(number) AS s FROM numbers(3));
SELECT sumOrNullMerge(s) OVER () FROM (SELECT sumOrNullState(number) AS s FROM numbers(3));
SELECT avgOrNullMerge(s) FROM (SELECT avgOrNullState(number) AS s FROM numbers(3));
SELECT singleValueOrNullMerge(s) FROM (SELECT singleValueOrNullState(number) AS s FROM numbers(1));
SELECT medianMerge(s) FROM (SELECT medianState(number) AS s FROM numbers(3));

-- State wrapper keeps sumOrNull state representation
SET aggregate_functions_null_for_empty = 0;
CREATE TABLE aggregate_merge_or_null_state (s AggregateFunction(sumOrNullState, UInt64)) ENGINE = Memory;
INSERT INTO aggregate_merge_or_null_state SELECT sumOrNullState(number) FROM numbers(3);

SELECT sumMergeOrNull(s), finalizeAggregation(sumMergeStateOrNull(s)) FROM aggregate_merge_or_null_state;
SELECT sumMergeOrNull(s) OVER () FROM aggregate_merge_or_null_state;

SET aggregate_functions_null_for_empty = 1;
SELECT sumMerge(s) FROM aggregate_merge_or_null_state;

DROP TABLE aggregate_merge_or_null_state;

-- quantile consumes quantiles states, parameters come from consumer
SET aggregate_functions_null_for_empty = 0;
SELECT quantileMergeOrNull(0.5)(s), finalizeAggregation(quantileMergeStateOrNull(0.5)(s))
FROM (SELECT quantilesState(0.1, 0.9)(number) AS s FROM numbers(10));
SET aggregate_functions_null_for_empty = 1;
SELECT quantileMerge(0.5)(s) FROM (SELECT quantilesState(0.1, 0.9)(number) AS s FROM numbers(10));

DROP TABLE aggregate_merge_or_null;
