-- A store_source NAIVE_BAYES dictionary must load when a source column is read with automatic LowCardinality
-- serialization: dictionary loading materializes the non-native LowCardinality columns, so that blocks of a part
-- in Default serialization and of a part in LowCardinality serialization can be accumulated together.
-- The first part has a high-cardinality ngram column (Default), the second a low-cardinality one (LowCardinality).

SET allow_experimental_statistics = 1;
SET materialize_statistics_on_insert = 1;

DROP DICTIONARY IF EXISTS nb_auto_lc_src_dict;
DROP TABLE IF EXISTS nb_auto_lc_src;

CREATE TABLE nb_auto_lc_src (ngram String STATISTICS(uniq), class_id UInt32, count UInt64)
ENGINE = MergeTree ORDER BY ngram
SETTINGS max_uniq_number_for_low_cardinality = 1000, ratio_of_defaults_for_sparse_serialization = 1,
    min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = '10Mi';

SYSTEM STOP MERGES nb_auto_lc_src;

INSERT INTO nb_auto_lc_src SELECT 'a' || toString(number), 7, 1 FROM numbers(20000); -- high cardinality -> Default
INSERT INTO nb_auto_lc_src SELECT 'b' || toString(number % 5), 3, 1 FROM numbers(20000); -- low cardinality -> LowCardinality

SELECT name, serialization_kind FROM system.parts_columns
WHERE database = currentDatabase() AND table = 'nb_auto_lc_src' AND active AND column = 'ngram'
ORDER BY name;

CREATE DICTIONARY nb_auto_lc_src_dict (ngram String, class_id UInt32 DEFAULT 0, count UInt64 DEFAULT 0)
PRIMARY KEY ngram
SOURCE(CLICKHOUSE(TABLE 'nb_auto_lc_src'))
LAYOUT(NAIVE_BAYES(class_attribute 'class_id' n 1 mode 'token' store_source 1))
LIFETIME(0)
SETTINGS(max_threads = 1, max_block_size = 4096);

-- store_source retains every source row of both parts, with correct values.
SELECT count(), countIf(class_id = 7), countIf(class_id = 3), countIf(startsWith(ngram, 'a')), countIf(startsWith(ngram, 'b')), uniqExact(ngram) FROM nb_auto_lc_src_dict;
SELECT ngram, class_id, count() FROM nb_auto_lc_src_dict WHERE startsWith(ngram, 'b') GROUP BY ngram, class_id ORDER BY ngram;
-- Classification works for tokens from both parts.
SELECT naiveBayesClassifier('nb_auto_lc_src_dict', 'a1');
SELECT naiveBayesClassifier('nb_auto_lc_src_dict', 'b1');

DROP DICTIONARY nb_auto_lc_src_dict;
DROP TABLE nb_auto_lc_src;
