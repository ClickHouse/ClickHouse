-- `deltaSumMerge` merges two states as if the right one follows the left one. Here the right state starts at the
-- last value of the left one, so the step between them is 0, and the merge gives 5, as `deltaSum` over the rows
-- 0, 1, 2, 2, 3, 5 in order does. `arrayReduce` merges the states in the order of the array.
SELECT arrayReduce('deltaSumMerge', [
    (SELECT deltaSumState(x) FROM (SELECT arrayJoin([0, 1, 2]) AS x)),
    (SELECT deltaSumState(x) FROM (SELECT arrayJoin([2, 3, 5]) AS x))]);
