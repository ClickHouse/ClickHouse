SET allow_experimental_shuffle_query = 1;
SET use_query_cache = 0, query_cache_for_subqueries = 0;
SET enable_reads_from_query_cache = 0, enable_writes_to_query_cache = 1;
SET query_cache_min_query_runs = 0, query_cache_min_query_duration = 0;
SET query_cache_nondeterministic_function_handling = 'throw';
SET query_cache_tag = '05488_shuffle_query_cache';

-- Direct and nested `SHUFFLE` must follow the nondeterministic-function policy.
SELECT number FROM numbers(10) SHUFFLE LIMIT 3 SETTINGS use_query_cache = 1 FORMAT Null; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count() FROM (SELECT number FROM numbers(10) SHUFFLE LIMIT 3) SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
WITH shuffled AS (SELECT number FROM numbers(10) SHUFFLE LIMIT 3)
SELECT count() FROM shuffled SETTINGS use_query_cache = 1; -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }

SET query_cache_nondeterministic_function_handling = 'ignore';
SELECT number FROM numbers(10) SHUFFLE LIMIT 3 SETTINGS use_query_cache = 1 FORMAT Null;
SELECT count() FROM system.query_cache WHERE tag = '05488_shuffle_query_cache';
SELECT count() FROM (SELECT number FROM numbers(10) SHUFFLE LIMIT 3) SETTINGS use_query_cache = 1;
SELECT count() FROM system.query_cache WHERE tag = '05488_shuffle_query_cache';

-- Explicit subquery caching uses the planner's reconstructed AST.
SELECT count() FROM
(
    SELECT number FROM numbers(10) SHUFFLE LIMIT 3
    SETTINGS use_query_cache = 1, query_cache_nondeterministic_function_handling = 'throw'
); -- { serverError QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS }
SELECT count() FROM
(
    SELECT number FROM numbers(10) SHUFFLE LIMIT 3
    SETTINGS use_query_cache = 1, query_cache_nondeterministic_function_handling = 'ignore'
);
SELECT count() FROM system.query_cache WHERE tag = '05488_shuffle_query_cache';
