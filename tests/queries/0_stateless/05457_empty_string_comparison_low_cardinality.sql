-- Comparisons with an empty string over LowCardinality(String) and LowCardinality(Nullable(String))
-- are rewritten to `empty`/`notEmpty`, while bloom filter and primary key analysis keep working.

SET enable_analyzer = 1;
SET optimize_empty_string_comparisons = 1;
SET use_skip_indexes = 1;
SET use_skip_indexes_on_data_read = 0;

DROP TABLE IF EXISTS t_lc_empty;
DROP TABLE IF EXISTS t_lc_fixed_empty;
DROP TABLE IF EXISTS t_lc_key_empty;
DROP TABLE IF EXISTS t_computed_key_empty;
DROP TABLE IF EXISTS t_lc_computed_key_empty;

CREATE TABLE t_lc_empty
(
    id UInt64,
    s LowCardinality(String),
    n LowCardinality(Nullable(String)),
    INDEX bf_s s TYPE bloom_filter GRANULARITY 1,
    INDEX bf_n n TYPE bloom_filter GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';

INSERT INTO t_lc_empty SELECT number, if(number = 0, '', 'x'), multiIf(number = 1, '', number = 2, NULL, 'y') FROM numbers(100);

SELECT 'rewrite';
SELECT replaceRegexpAll(explain, ' id: \\d+', '') FROM (EXPLAIN QUERY TREE SELECT id FROM t_lc_empty WHERE s = '' AND '' != n) WHERE explain LIKE '%function_name: %Empty%' OR explain LIKE '%function_name: empty%';
SELECT replaceRegexpAll(explain, ' id: \\d+', '') FROM (EXPLAIN QUERY TREE SELECT id FROM t_lc_empty WHERE s = '' SETTINGS optimize_empty_string_comparisons = 0) WHERE explain LIKE '%function_name: equals%';

SELECT 'results';
SELECT id FROM t_lc_empty WHERE s = '' ORDER BY id;
SELECT id FROM t_lc_empty WHERE '' = n ORDER BY id;
SELECT count() FROM t_lc_empty WHERE s != '';
SELECT count() FROM t_lc_empty WHERE '' != n;
SELECT count() FROM t_lc_empty WHERE n != '' SETTINGS optimize_empty_string_comparisons = 0;

SELECT 'bloom filter';
SELECT id FROM t_lc_empty WHERE s = '' ORDER BY id SETTINGS force_data_skipping_indices = 'bf_s';
SELECT id FROM t_lc_empty WHERE n = '' ORDER BY id SETTINGS force_data_skipping_indices = 'bf_n';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT id FROM t_lc_empty WHERE s = '') WHERE explain LIKE '%Granules:%';

SELECT 'fixed string';
CREATE TABLE t_lc_fixed_empty (s LowCardinality(FixedString(3))) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_lc_fixed_empty SELECT if(number < 4, '', 'abc') FROM numbers(100);
SELECT replaceRegexpAll(explain, ' id: \\d+', '') FROM (EXPLAIN QUERY TREE SELECT count() FROM t_lc_fixed_empty WHERE s = '') WHERE explain LIKE '%function_name: equals%';
SELECT count() FROM t_lc_fixed_empty WHERE s = '' SETTINGS force_primary_key = 1;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lc_fixed_empty WHERE s = '') WHERE explain LIKE '%Granules:%';

SELECT 'primary key';
CREATE TABLE t_lc_key_empty (s LowCardinality(String)) ENGINE = MergeTree ORDER BY s SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_lc_key_empty SELECT if(number < 4, '', toString(number)) FROM numbers(100);
SELECT count() FROM t_lc_key_empty WHERE s = '' SETTINGS force_primary_key = 1;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT count() FROM t_lc_key_empty WHERE s = '') WHERE explain LIKE '%Granules:%';

SELECT 'computed primary key';
CREATE TABLE t_computed_key_empty (s String) ENGINE = MergeTree ORDER BY cityHash64(s) SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_computed_key_empty SELECT if(number < 4, '', toString(number)) FROM numbers(100);
SELECT count(), max(s) FROM t_computed_key_empty WHERE s = '' SETTINGS force_primary_key = 1;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT s FROM t_computed_key_empty WHERE s = '') WHERE explain LIKE '%Granules:%';
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT s FROM t_computed_key_empty WHERE s = '' SETTINGS optimize_empty_string_comparisons = 0) WHERE explain LIKE '%Granules:%';
SELECT count() FROM t_computed_key_empty WHERE s != '';

CREATE TABLE t_lc_computed_key_empty (s LowCardinality(String)) ENGINE = MergeTree ORDER BY cityHash64(s) SETTINGS index_granularity = 4, index_granularity_bytes = '10Mi';
INSERT INTO t_lc_computed_key_empty SELECT if(number < 4, '', toString(number)) FROM numbers(100);
SELECT count(), max(s) FROM t_lc_computed_key_empty WHERE s = '' SETTINGS force_primary_key = 1;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT s FROM t_lc_computed_key_empty WHERE s = '') WHERE explain LIKE '%Granules:%';
SELECT count() FROM t_lc_computed_key_empty WHERE s != '';

DROP TABLE t_lc_empty;
DROP TABLE t_lc_fixed_empty;
DROP TABLE t_lc_key_empty;
DROP TABLE t_computed_key_empty;
DROP TABLE t_lc_computed_key_empty;
