-- Tags: no-random-settings, no-random-merge-tree-settings, no-parallel-replicas

-- A partial index is used only if the query calculates the indexed expression like the writer did.

SET allow_experimental_partial_skip_indexes = 1;
SET use_query_condition_cache = 0;

DROP TABLE IF EXISTS t_expr_set;
DROP TABLE IF EXISTS t_expr_minmax;
DROP TABLE IF EXISTS t_expr_eq;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_nested_eq;
DROP TABLE IF EXISTS t_zero_minmax;
DROP TABLE IF EXISTS t_zero_set;

CREATE TABLE t_expr_set (n UInt64, p UInt8, INDEX i formatRow('JSONEachRow', n) TYPE set(10) WHERE p) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_expr_set VALUES (1, 1);
CREATE TABLE t_expr_minmax (n UInt64, p UInt8, INDEX i formatRow('JSONEachRow', n) TYPE minmax WHERE p) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_expr_minmax VALUES (1, 1);
CREATE TABLE t_expr_eq (n UInt64, p UInt8, INDEX i formatRow('JSONEachRow', n) TYPE set(10) WHERE p = 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_expr_eq VALUES (1, 1);
CREATE TABLE t_nested (n UInt64, p UInt8, INDEX i cityHash64(formatRow('JSONEachRow', n)) TYPE minmax WHERE p) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested VALUES (1, 1);
CREATE TABLE t_nested_eq (n UInt64, p UInt8, INDEX i cityHash64(formatRow('JSONEachRow', n)) TYPE minmax WHERE p = 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_nested_eq VALUES (1, 1);
CREATE TABLE t_zero_minmax (id UInt64, f Float64, INDEX i id TYPE minmax WHERE 1. / (f + 0.) < 0.) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_zero_minmax VALUES (1, -0.), (2, -1.);
CREATE TABLE t_zero_set (id UInt64, f Float64, INDEX i id TYPE set(10) WHERE 1. / (f + 0.) < 0.) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_zero_set VALUES (1, -0.), (2, -1.);

SELECT 'format setting of the indexed expression';
SELECT count() FROM t_expr_set WHERE p AND formatRow('JSONEachRow', n) = '{"n":"1"}\n' SETTINGS output_format_json_quote_64bit_integers = 1, use_skip_indexes = 0;
SELECT count() FROM t_expr_set WHERE p AND formatRow('JSONEachRow', n) = '{"n":"1"}\n' SETTINGS output_format_json_quote_64bit_integers = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_expr_set WHERE p AND formatRow('JSONEachRow', n) = '{"n":"1"}\n' SETTINGS output_format_json_quote_64bit_integers = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
SELECT count() FROM t_expr_minmax WHERE p AND formatRow('JSONEachRow', n) = '{"n":"1"}\n' SETTINGS output_format_json_quote_64bit_integers = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_expr_minmax WHERE p AND formatRow('JSONEachRow', n) = '{"n":"1"}\n' SETTINGS output_format_json_quote_64bit_integers = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'field name given by an alias';
SELECT count() FROM t_expr_eq WHERE p = 1 AND formatRow('JSONEachRow', n AS x) = '{"x":1}\n' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_expr_eq WHERE p = 1 AND formatRow('JSONEachRow', n AS x) = '{"x":1}\n' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_expr_eq WHERE p = 1 AND formatRow('JSONEachRow', n AS x) = '{"x":1}\n' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'subexpression of the indexed expression';
SELECT count() FROM t_nested WHERE p AND formatRow('JSONEachRow', n) = '{"n":"1"}\n' SETTINGS output_format_json_quote_64bit_integers = 1, use_skip_indexes = 0;
SELECT count() FROM t_nested WHERE p AND formatRow('JSONEachRow', n) = '{"n":"1"}\n' SETTINGS output_format_json_quote_64bit_integers = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_nested WHERE p AND formatRow('JSONEachRow', n) = '{"n":"1"}\n' SETTINGS output_format_json_quote_64bit_integers = 1, use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
SELECT count() FROM t_nested_eq WHERE p = 1 AND formatRow('JSONEachRow', n AS x) = '{"x":1}\n' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_nested_eq WHERE p = 1 AND formatRow('JSONEachRow', n AS x) = '{"x":1}\n' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_nested_eq WHERE p = 1 AND formatRow('JSONEachRow', n AS x) = '{"x":1}\n' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'signed zero in a constant of the condition';
SELECT count() FROM t_zero_minmax WHERE id = 1 AND 1. / (f + -0.) < 0. SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_zero_minmax WHERE id = 1 AND 1. / (f + -0.) < 0. SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_zero_minmax WHERE id = 1 AND 1. / (f + -0.) < 0. SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
SELECT count() FROM t_zero_set WHERE id = 1 AND 1. / (f + -0.) < 0. SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_zero_set WHERE id = 1 AND 1. / (f + -0.) < 0. SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;

SELECT 'same calculation: the index is used';
SELECT count() FROM t_expr_set WHERE p AND formatRow('JSONEachRow', n) = '{"n":1}\n' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i';
SELECT count() FROM t_expr_minmax WHERE p AND formatRow('JSONEachRow', n) = '{"n":1}\n' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i';
SELECT count() FROM t_expr_set WHERE p AND formatRow('JSONEachRow', n) = '{"n":2}\n' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i';
SELECT count() FROM t_nested WHERE p AND formatRow('JSONEachRow', n) = '{"n":1}\n' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i';
SELECT count() FROM t_nested WHERE p AND formatRow('JSONEachRow', n) = '{"n":2}\n' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i';
SELECT count() FROM t_zero_minmax WHERE id = 2 AND 1. / (f + 0.) < 0. SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i';

SELECT 'a column named like the indexed expression';
DROP TABLE IF EXISTS t_name_set;
DROP TABLE IF EXISTS t_name_bloom;
DROP TABLE IF EXISTS t_name_column;
CREATE TABLE t_name_set (n UInt64, p UInt8, `toString(n)` String, INDEX i toString(n) TYPE set(10) WHERE p = 1) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_name_bloom (n UInt64, p UInt8, `toString(n)` String, INDEX i toString(n) TYPE bloom_filter WHERE p = 1) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_name_column (n UInt64, p UInt8, `toString(n)` String, INDEX i `toString(n)` TYPE set(10) WHERE p = 1) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_name_set VALUES (1, 1, 'x');
INSERT INTO t_name_bloom VALUES (1, 1, 'x');
INSERT INTO t_name_column VALUES (1, 1, 'x');
SELECT count() FROM t_name_set WHERE p = 1 AND `toString(n)` = 'x' SETTINGS use_skip_indexes = 0;
SELECT count() FROM t_name_set WHERE p = 1 AND `toString(n)` = 'x' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_name_set WHERE p = 1 AND `toString(n)` = 'x' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
SELECT count() FROM t_name_bloom WHERE p = 1 AND `toString(n)` = 'x' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0;
SELECT count() FROM t_name_bloom WHERE p = 1 AND `toString(n)` = 'x' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 1;
SELECT 'the column itself is indexed: the index is used';
SELECT count() FROM t_name_column WHERE p = 1 AND `toString(n)` = 'x' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i';
SELECT count() FROM t_name_column WHERE p = 1 AND `toString(n)` = 'y' SETTINGS use_skip_indexes = 1, use_skip_indexes_on_data_read = 0, force_data_skipping_indices = 'i';
DROP TABLE t_name_set;
DROP TABLE t_name_bloom;
DROP TABLE t_name_column;

DROP TABLE t_expr_set;
DROP TABLE t_expr_minmax;
DROP TABLE t_expr_eq;
DROP TABLE t_nested;
DROP TABLE t_nested_eq;
DROP TABLE t_zero_minmax;
DROP TABLE t_zero_set;
