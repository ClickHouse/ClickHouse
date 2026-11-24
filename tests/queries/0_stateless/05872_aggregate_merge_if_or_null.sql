-- -MergeIf and -MergeStateIf must merge -OrNull states, the same as -Merge does.
SET enable_analyzer = 1;

SET aggregate_functions_null_for_empty = 1;

-- States produced under the same setting are sumOrNull states.
SELECT sumMerge(s) FROM (SELECT number % 3 AS k, sumState(number) AS s FROM numbers(9) GROUP BY k);
SELECT sumMergeIf(s, k = 1), sumMergeIf(s, k = 5) FROM (SELECT number % 3 AS k, sumState(number) AS s FROM numbers(9) GROUP BY k);
SELECT sumMergeIfOrNull(s, k = 1), sumMergeIfOrNull(s, k = 5) FROM (SELECT number % 3 AS k, sumState(number) AS s FROM numbers(9) GROUP BY k);
SELECT finalizeAggregation(sumMergeStateIf(s, k = 1)), finalizeAggregation(sumMergeStateIf(s, k = 5)) FROM (SELECT number % 3 AS k, sumState(number) AS s FROM numbers(9) GROUP BY k);
SELECT sumMergeIf(s, k = 1) OVER () FROM (SELECT number % 3 AS k, sumState(number) AS s FROM numbers(9) GROUP BY k);
SELECT sumOrNullMergeIf(s, k = 1), sumOrNullMergeIf(s, k = 5) FROM (SELECT number % 3 AS k, sumState(number) AS s FROM numbers(9) GROUP BY k);

DROP TABLE IF EXISTS aggregate_merge_if_or_null;
CREATE TABLE aggregate_merge_if_or_null (k UInt64, s AggregateFunction(sum, UInt64), n AggregateFunction(sumOrNull, UInt64)) ENGINE = Memory;
INSERT INTO aggregate_merge_if_or_null SELECT number % 3, sumState(number), sumOrNullState(number) FROM numbers(9) GROUP BY number % 3 SETTINGS aggregate_functions_null_for_empty = 0;

-- Plain sum states keep working.
SELECT sumMergeIf(s, k = 1), sumMergeIf(s, k = 5) FROM aggregate_merge_if_or_null;
SELECT sumMergeIf(n, k = 1), sumMergeIf(n, k = 5) FROM aggregate_merge_if_or_null;

SET aggregate_functions_null_for_empty = 0;

SELECT sumMergeIfOrNull(s, k = 1), sumMergeIfOrNull(s, k = 5) FROM aggregate_merge_if_or_null;
SELECT sumMergeIfOrNull(n, k = 1), sumMergeIfOrNull(n, k = 5) FROM aggregate_merge_if_or_null;
SELECT finalizeAggregation(sumMergeStateIfOrNull(n, k = 1)), finalizeAggregation(sumMergeStateIfOrNull(n, k = 5)) FROM aggregate_merge_if_or_null;

DROP TABLE aggregate_merge_if_or_null;
