SET explain_query_plan_default = 'pretty';
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 2;
SET collect_hash_table_stats_during_joins = 0;
SET enable_parallel_replicas = 0;
SET join_algorithm = 'hash';

DROP TABLE IF EXISTS join_input_width_wide;
DROP TABLE IF EXISTS join_input_width_narrow;
CREATE TABLE join_input_width_wide (k UInt64, payload String) ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE join_input_width_narrow (k UInt64) ENGINE = MergeTree ORDER BY k
    SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO join_input_width_wide SELECT number, repeat('x', 160) FROM numbers(15000);
INSERT INTO join_input_width_narrow SELECT number FROM numbers(1, 16000);

-- The left side has fewer rows but carries a wide `String` into the join.
SELECT extract(explain, 'Type: (left|right)') AS join_type FROM (
    EXPLAIN SELECT count(), sum(length(reverse(payload)))
    FROM join_input_width_wide LEFT ALL JOIN join_input_width_narrow USING (k)
    SETTINGS query_plan_join_swap_table = 'auto'
) WHERE join_type != '';

-- An explicit setting still swaps the sides.
SELECT extract(explain, 'Type: (left|right)') AS join_type FROM (
    EXPLAIN SELECT count(), sum(length(reverse(payload)))
    FROM join_input_width_wide LEFT ALL JOIN join_input_width_narrow USING (k)
    SETTINGS query_plan_join_swap_table = 'true'
) WHERE join_type != '';

SELECT count(), sum(length(reverse(payload)))
FROM join_input_width_wide LEFT ALL JOIN join_input_width_narrow USING (k)
SETTINGS query_plan_join_swap_table = 'auto';

-- The narrow input now has many more keys, so building from it also needs more hash table entries.
TRUNCATE TABLE join_input_width_wide;
TRUNCATE TABLE join_input_width_narrow;
INSERT INTO join_input_width_wide SELECT number, repeat('x', 64) FROM numbers(15000);
INSERT INTO join_input_width_narrow SELECT number FROM numbers(120000);

SELECT extract(explain, 'Type: (left|right)') AS join_type FROM (
    EXPLAIN SELECT count(), sum(length(reverse(payload)))
    FROM join_input_width_wide LEFT ALL JOIN join_input_width_narrow USING (k)
    SETTINGS query_plan_join_swap_table = 'auto'
) WHERE join_type != '';

-- A Compact part lacks per-column sizes, so use the row counts for this input.
TRUNCATE TABLE join_input_width_wide;
TRUNCATE TABLE join_input_width_narrow;
ALTER TABLE join_input_width_wide MODIFY SETTING min_rows_for_wide_part = 1000;
SYSTEM STOP MERGES join_input_width_wide;
INSERT INTO join_input_width_wide SELECT number, repeat('x', 160) FROM numbers(15000);
INSERT INTO join_input_width_wide VALUES (15000, 'compact');
INSERT INTO join_input_width_narrow SELECT number FROM numbers(16000);
SELECT part_type, rows FROM system.parts
WHERE active AND database = currentDatabase() AND table = 'join_input_width_wide'
ORDER BY rows;

SELECT extract(explain, 'Type: (left|right)') AS join_type FROM (
    EXPLAIN SELECT count(), sum(length(reverse(payload)))
    FROM join_input_width_wide LEFT ALL JOIN join_input_width_narrow USING (k)
    SETTINGS query_plan_join_swap_table = 'auto'
) WHERE join_type != '';
SYSTEM START MERGES join_input_width_wide;

DROP TABLE join_input_width_wide;
DROP TABLE join_input_width_narrow;
