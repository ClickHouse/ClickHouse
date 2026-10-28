-- An empty `String` group key among the winners of the top-K threshold merge: the merge probes
-- the other partial tables with the key, which must not pass a null pointer to `memcpy`.

-- Several partial tables with two-level states, as in `05042_group_by_threshold_top_k_merge`.
SET enable_adaptive_aggregator = 0;
SET max_threads = 4;
SET group_by_two_level_threshold = 1;
SET group_by_two_level_threshold_bytes = 1;
SET max_block_size = 1024;
SET max_rows_to_group_by = 0;
SET query_plan_enable_optimizations = 1;
SET query_plan_push_down_limit = 1;

DROP TABLE IF EXISTS threshold_top_k_empty_key;
CREATE TABLE threshold_top_k_empty_key (s String, v UInt64) ENGINE = MergeTree ORDER BY ();

-- Every part has the empty key with the largest values, so it is present in every partial table.
INSERT INTO threshold_top_k_empty_key SELECT if(number % 100 = 0, '', toString(number)), if(number % 100 = 0, 1000000 + number, number) FROM numbers(20000);
INSERT INTO threshold_top_k_empty_key SELECT if(number % 100 = 0, '', toString(number)), if(number % 100 = 0, 1000000 + number, number) FROM numbers(20000, 20000);
INSERT INTO threshold_top_k_empty_key SELECT if(number % 100 = 0, '', toString(number)), if(number % 100 = 0, 1000000 + number, number) FROM numbers(40000, 20000);

SELECT s, max(v) AS m, count() FROM threshold_top_k_empty_key GROUP BY s ORDER BY m DESC LIMIT 1 SETTINGS query_plan_aggregation_bucket_top_k = 1;
SELECT s, max(v) AS m, count() FROM threshold_top_k_empty_key GROUP BY s ORDER BY m DESC LIMIT 1 SETTINGS query_plan_aggregation_bucket_top_k = 0;
SELECT s, max(v) AS m, count() FROM threshold_top_k_empty_key GROUP BY s ORDER BY m DESC LIMIT 3 SETTINGS query_plan_aggregation_bucket_top_k = 1;
SELECT s, max(v) AS m, count() FROM threshold_top_k_empty_key GROUP BY s ORDER BY m DESC LIMIT 3 SETTINGS query_plan_aggregation_bucket_top_k = 0;

DROP TABLE threshold_top_k_empty_key;
