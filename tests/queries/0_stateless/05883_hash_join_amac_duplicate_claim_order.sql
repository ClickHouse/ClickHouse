SET join_algorithm = 'hash', hash_join_layout = 'serial', any_join_distinct_right_table_keys = 0;
SET max_threads = 1, max_block_size = 4097, query_plan_join_swap_table = 0, enable_join_runtime_filters = 0;
SET enable_amac_in_hash_join = 'on', enable_software_prefetch_in_join = 1, enable_join_fixed_hash_table_conversion = 0;

-- In each group of 64 left rows, each key recurs 31 rows later, across the 32-slot AMAC ring.
-- The single probe stream must retain the first left payload for each key, including zero.
SELECT count(), countIf(l.i != 64 * intDiv(r.id, 31) + r.id % 31)
FROM
(
    SELECT number AS i, 31 * intDiv(number, 64) + number % 64 % 31 AS id,
        id * toUInt64(11400714819323198485) AS k
    FROM numbers(8195)
) AS l
INNER ANY JOIN
(
    SELECT number AS id, number * toUInt64(11400714819323198485) AS k FROM numbers(2097152)
) AS r ON l.k = r.k;
