SET aggregate_functions_null_for_empty = 0;
SET enable_analyzer = 1;

DROP TABLE IF EXISTS aggregate_merge_or_null;
CREATE TABLE aggregate_merge_or_null (a Int32, s AggregateFunction(sum, Int32)) ENGINE = Memory;
INSERT INTO aggregate_merge_or_null SELECT number, sumState(toInt32(number)) FROM numbers(101) GROUP BY number;

SELECT sumMergeOrNull(s) FROM aggregate_merge_or_null WHERE a < 0;
SELECT sumMergeOrNull(s) FROM aggregate_merge_or_null;
SELECT toTypeName(sumMergeStateOrNull(s)), finalizeAggregation(sumMergeStateOrNull(s)) FROM aggregate_merge_or_null WHERE a < 0;
SELECT toTypeName(sumMergeStateOrNull(s)), finalizeAggregation(sumMergeStateOrNull(s)) FROM aggregate_merge_or_null;
SELECT sumMergeIfOrNull(s, a < 0), sumMergeOrNullIf(s, a < 0) FROM aggregate_merge_or_null;
SELECT toTypeName(sumStateOrNull(number)), finalizeAggregation(sumStateOrNull(number)) FROM numbers(0);

SET aggregate_functions_null_for_empty = 1;

SELECT sumMergeOrNull(s) FROM aggregate_merge_or_null WHERE a < 0;
SELECT sumMergeOrNull(s) FROM aggregate_merge_or_null;
SELECT toTypeName(sumMergeStateOrNull(s)), finalizeAggregation(sumMergeStateOrNull(s)) FROM aggregate_merge_or_null WHERE a < 0;
SELECT toTypeName(sumMergeStateOrNull(s)), finalizeAggregation(sumMergeStateOrNull(s)) FROM aggregate_merge_or_null;
SELECT sumMerge(s), finalizeAggregation(sumMergeState(s)) FROM (SELECT sumState(number) AS s FROM numbers(0));
SELECT sumMerge(s), finalizeAggregation(sumMergeState(s)) FROM (SELECT sumState(number) AS s FROM numbers(101));

SET aggregate_functions_null_for_empty = 0;
SET enable_analyzer = 0;

SELECT sumMergeOrNull(s) FROM aggregate_merge_or_null WHERE a < 0;
SELECT sumMergeOrNull(s) FROM aggregate_merge_or_null;
SELECT toTypeName(sumMergeStateOrNull(s)), finalizeAggregation(sumMergeStateOrNull(s)) FROM aggregate_merge_or_null WHERE a < 0;
SELECT toTypeName(sumMergeStateOrNull(s)), finalizeAggregation(sumMergeStateOrNull(s)) FROM aggregate_merge_or_null;
SELECT sumMergeIfOrNull(s, a < 0), sumMergeOrNullIf(s, a < 0) FROM aggregate_merge_or_null;
SELECT toTypeName(sumStateOrNull(number)), finalizeAggregation(sumStateOrNull(number)) FROM numbers(0);

SET aggregate_functions_null_for_empty = 1;

SELECT sumMergeOrNull(s) FROM aggregate_merge_or_null WHERE a < 0;
SELECT sumMergeOrNull(s) FROM aggregate_merge_or_null;
SELECT toTypeName(sumMergeStateOrNull(s)), finalizeAggregation(sumMergeStateOrNull(s)) FROM aggregate_merge_or_null WHERE a < 0;
SELECT toTypeName(sumMergeStateOrNull(s)), finalizeAggregation(sumMergeStateOrNull(s)) FROM aggregate_merge_or_null;
SELECT sumMerge(s), finalizeAggregation(sumMergeState(s)) FROM (SELECT sumState(number) AS s FROM numbers(0));
SELECT sumMerge(s), finalizeAggregation(sumMergeState(s)) FROM (SELECT sumState(number) AS s FROM numbers(101));

DROP TABLE aggregate_merge_or_null;
