-- Tags: no-fasttest, no-parallel
-- no-fasttest: needs a build with libfiu to enable the failpoint.
-- no-parallel: the failpoint is server-wide and fires once, so a concurrent query could consume it.

-- `auto` builds a hash join and hands its blocks to `MergeJoin` once the right side exceeds `max_rows_in_join`.
SET join_algorithm = 'auto';
SET max_rows_in_join = 10;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET query_plan_join_swap_table = 0;

SYSTEM ENABLE FAILPOINT hash_join_throw_after_data_release;
SELECT n, j FROM (SELECT number AS n FROM numbers(4)) AS nums
ANY LEFT JOIN (SELECT number * 2 AS n, number + 10 AS j FROM numbers(4000)) AS js2
USING n ORDER BY n; -- { serverError FAULT_INJECTED }
SYSTEM DISABLE FAILPOINT hash_join_throw_after_data_release;
