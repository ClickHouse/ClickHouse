-- The number of distinct values of a `LowCardinality` column is taken from the dictionary of a part only when the
-- dictionary has all values of the part and is written in the type of the table, see the setting
-- `estimate_distinct_values_from_metadata`.

SET use_statistics = 1;
SET query_plan_optimize_join_order_limit = 10;
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 'auto';
SET query_plan_optimize_join_order_randomize = 0;

DROP TABLE IF EXISTS single_dictionary;
DROP TABLE IF EXISTS single_granule;
DROP TABLE IF EXISTS full_dictionary;
DROP TABLE IF EXISTS pending_type_change;

SELECT 'A single dictionary for the part: the keys that do not fit are written beside the indexes';
CREATE TABLE single_dictionary (id UInt64, s LowCardinality(String))
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0;
INSERT INTO single_dictionary SETTINGS low_cardinality_max_dictionary_size = 16, low_cardinality_use_single_dictionary_for_part = 1
SELECT number, toString(intHash64(number)) FROM numbers(100000);
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) > 10000
FROM (EXPLAIN SELECT count() FROM single_dictionary AS a JOIN single_dictionary AS b ON a.s = b.s);

SELECT 'A single granule with more keys than fit into the dictionary';
CREATE TABLE single_granule (id UInt64, s LowCardinality(String))
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0;
INSERT INTO single_granule SETTINGS low_cardinality_max_dictionary_size = 16, low_cardinality_use_single_dictionary_for_part = 0
SELECT number, toString(intHash64(number)) FROM numbers(1000);
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) > 100
FROM (EXPLAIN SELECT count() FROM single_granule AS a JOIN single_granule AS b ON a.s = b.s);

SELECT 'A full dictionary without additional keys: the number of its keys';
CREATE TABLE full_dictionary (id UInt64, s LowCardinality(String))
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0;
INSERT INTO full_dictionary SETTINGS low_cardinality_max_dictionary_size = 16, low_cardinality_use_single_dictionary_for_part = 1
SELECT number, toString(intHash64(number % 15)) FROM numbers(100000);
SELECT 1 / toFloat64OrZero(extract(arrayStringConcat(groupArray(explain), '\n'), 'Selectivity: estimated \\(NDV\\) ([0-9.e+-]+)')) BETWEEN 14 AND 17
FROM (EXPLAIN SELECT count() FROM full_dictionary AS a JOIN full_dictionary AS b ON a.s = b.s);

SELECT 'A pending change of the type: the dictionary of the part is in the old type';
CREATE TABLE pending_type_change (id UInt64, s LowCardinality(FixedString(1)))
ENGINE = MergeTree ORDER BY id
SETTINGS auto_statistics_types = '', min_bytes_for_wide_part = 0;
INSERT INTO pending_type_change SELECT number, if(number % 2 = 0, 'A', 'B') FROM numbers(1000);
SYSTEM STOP MERGES pending_type_change;
ALTER TABLE pending_type_change MODIFY COLUMN s LowCardinality(String) SETTINGS alter_sync = 0;
SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM pending_type_change AS a JOIN pending_type_change AS b ON a.s = b.s);
SELECT count() FROM pending_type_change AS a JOIN pending_type_change AS b ON a.s = b.s;
SYSTEM START MERGES pending_type_change;

DROP TABLE single_dictionary;
DROP TABLE single_granule;
DROP TABLE full_dictionary;
DROP TABLE pending_type_change;
