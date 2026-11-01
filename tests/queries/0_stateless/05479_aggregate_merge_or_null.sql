SET aggregate_functions_null_for_empty = 0;

DROP TABLE IF EXISTS aggregate_merge_or_null;
CREATE TABLE aggregate_merge_or_null (a Int32, s AggregateFunction(sum, Int32), n AggregateFunction(sumOrNull, Int32)) ENGINE = Memory;
INSERT INTO aggregate_merge_or_null SELECT number, sumState(toInt32(number)), sumOrNullState(toInt32(number)) FROM numbers(101) GROUP BY number;

-- sum states, analyzer only
SET enable_analyzer = 1;

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

-- sumOrNull states, both analyzers
SET enable_analyzer = 0;
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

SET enable_analyzer = 1;
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

DROP TABLE aggregate_merge_or_null;
