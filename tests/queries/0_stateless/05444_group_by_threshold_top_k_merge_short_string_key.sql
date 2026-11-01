-- The top-K threshold merge probes the bucket tables of `StringHashMap` (the legacy string key
-- method) with the keys of other tables. A key of up to 8 bytes is a view into a cell, and the
-- probe must not read the word before the cell (heap-buffer-overflow under ASan).

SET enable_adaptive_aggregator = 0;
SET enable_packed_string_keys_in_aggregation = 0;
-- Keep `toString` in the key, otherwise the aggregation is by a number.
SET optimize_injective_functions_in_group_by = 0;
SET max_threads = 4;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;
SET max_block_size = 1000;
SET max_rows_to_group_by = 0;

SELECT 'max DESC';
SELECT toString(number % 12800) AS s, max(number) AS m FROM numbers_mt(1000000) GROUP BY s ORDER BY m DESC LIMIT 5 SETTINGS query_plan_aggregation_bucket_top_k = 1;
SELECT toString(number % 12800) AS s, max(number) AS m FROM numbers_mt(1000000) GROUP BY s ORDER BY m DESC LIMIT 5 SETTINGS query_plan_aggregation_bucket_top_k = 0;

SELECT 'min ASC';
SELECT toString(number % 12800) AS s, min(number) AS m FROM numbers_mt(1000000) GROUP BY s ORDER BY m ASC LIMIT 5 SETTINGS query_plan_aggregation_bucket_top_k = 1;
SELECT toString(number % 12800) AS s, min(number) AS m FROM numbers_mt(1000000) GROUP BY s ORDER BY m ASC LIMIT 5 SETTINGS query_plan_aggregation_bucket_top_k = 0;

SELECT 'keys of various lengths';
SELECT repeat('x', number % 24) || toString(number % 12800) AS s, max(number) AS m FROM numbers_mt(1000000) GROUP BY s ORDER BY m DESC LIMIT 5 SETTINGS query_plan_aggregation_bucket_top_k = 1;
SELECT repeat('x', number % 24) || toString(number % 12800) AS s, max(number) AS m FROM numbers_mt(1000000) GROUP BY s ORDER BY m DESC LIMIT 5 SETTINGS query_plan_aggregation_bucket_top_k = 0;
