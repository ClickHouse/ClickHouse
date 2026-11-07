-- Tags: no-parallel
-- Tag no-parallel: Messes with internal cache

SET enable_analyzer = 1;

SYSTEM DROP QUERY CACHE;

-- A subquery which opts into the query result cache only for reading cannot store a truncated result,
-- so a non-THROW overflow mode is allowed, like for the top-level query result cache.
SELECT * FROM (SELECT 1 SETTINGS use_query_cache = 1) SETTINGS enable_writes_to_query_cache = 0, read_overflow_mode = 'break';
SELECT * FROM (SELECT 2 SETTINGS use_query_cache = 1, enable_writes_to_query_cache = 0, read_overflow_mode = 'break');

-- The sources of a subquery also enforce the limits inherited from the outer query. A non-THROW inherited limit
-- would truncate the cached subquery result, even if the subquery overrides the limit for itself.
SELECT * FROM
(
    SELECT sum(number) FROM numbers(1000000)
    SETTINGS use_query_cache = 1, max_rows_to_read = 0, read_overflow_mode = 'throw'
)
SETTINGS max_rows_to_read = 10, read_overflow_mode = 'break', max_block_size = 100; -- { serverError QUERY_CACHE_USED_WITH_NON_THROW_OVERFLOW_MODE }

-- Nothing was cached so far.
SELECT count() FROM system.query_cache;

-- The same subquery under an outer query without such limits is computed and cached completely.
SELECT * FROM
(
    SELECT sum(number) FROM numbers(1000000)
    SETTINGS use_query_cache = 1, max_rows_to_read = 0, read_overflow_mode = 'throw'
);
SELECT count() FROM system.query_cache WHERE is_subquery;

SYSTEM DROP QUERY CACHE;
