-- `if`, `multiIf` and `transform` with constant string branches build their `LowCardinality` result directly
-- as indexes into a dictionary of the branches. Check that the values are the same as with a plain `String` result.

SET optimize_if_transform_strings_to_enum = 0;
SET max_block_size = 1000;

SET optimize_if_transform_const_strings_to_lowcardinality = 1;
SELECT 'optimize_if_transform_const_strings_to_lowcardinality = 1';
SELECT 'if_plain', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, if(number % 3 = 0, 'a', 'b') AS x FROM numbers(10000));
SELECT 'if_nullable_condition', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, if(nullIf(number % 3, 2) = 0, 'a', 'b') AS x FROM numbers(10000));
SELECT 'if_low_cardinality_condition', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, if(toLowCardinality(number % 2 = 0), 'x', 'y') AS x FROM numbers(10000));
SELECT 'if_null_branch', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, if(number % 2 = 0, 'a', NULL) AS x FROM numbers(10000));
SELECT 'if_default_value_branch', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, if(number % 2 = 0, '', 'z') AS x FROM numbers(10000));
SELECT 'multiif_plain', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, multiIf(number % 5 = 0, 'five', number % 3 = 0, 'three', 'other') AS x FROM numbers(10000));
SELECT 'multiif_nullable', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, multiIf(nullIf(number % 5, 1) = 0, 'five', number % 7 = 0, NULL, '') AS x FROM numbers(10000));
SELECT 'multiif_duplicate_branches', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, multiIf(number % 5 = 0, 'same', number % 3 = 0, 'same', 'other') AS x FROM numbers(10000));
SELECT 'transform_number', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(number % 5, [0, 1, 2], ['a', 'b', 'c'], 'other') AS x FROM numbers(10000));
SELECT 'transform_string', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(toString(number % 5), ['0', '1'], ['a', 'b'], 'z') AS x FROM numbers(10000));
SELECT 'transform_fixed_string', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(toFixedString(toString(number % 5), 1), ['0', '1'], ['a', 'b'], 'z') AS x FROM numbers(10000));
SELECT 'transform_nullable_input', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(nullIf(number % 5, 3), [0, 1, 2], ['a', 'b', 'c'], 'd') AS x FROM numbers(10000));
SELECT 'transform_low_cardinality_input', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(toLowCardinality(toString(number % 5)), ['0', '1'], ['a', 'b'], 'z') AS x FROM numbers(10000));
SELECT 'transform_decimal_input', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(toDecimal32(number % 5, 2), [toDecimal32(1, 2)], ['one'], 'x') AS x FROM numbers(10000));
SELECT 'transform_null_value', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(number % 5, [0, 1], ['a', NULL], 'd') AS x FROM numbers(10000));
SELECT 'transform_null_default', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(number % 5, [0, 1], ['a', 'b'], NULL) AS x FROM numbers(10000));
SELECT 'transform_duplicate_values', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(number % 5, [0, 1, 2], ['a', 'a', 'other'], 'other') AS x FROM numbers(10000));
SELECT 'transform_large_mapping', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(number % 1000, range(600), arrayMap(x -> toString(x), range(600)), 'big') AS x FROM numbers(10000));

SET optimize_if_transform_const_strings_to_lowcardinality = 0;
SELECT 'optimize_if_transform_const_strings_to_lowcardinality = 0';
SELECT 'if_plain', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, if(number % 3 = 0, 'a', 'b') AS x FROM numbers(10000));
SELECT 'if_nullable_condition', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, if(nullIf(number % 3, 2) = 0, 'a', 'b') AS x FROM numbers(10000));
SELECT 'if_low_cardinality_condition', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, if(toLowCardinality(number % 2 = 0), 'x', 'y') AS x FROM numbers(10000));
SELECT 'if_null_branch', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, if(number % 2 = 0, 'a', NULL) AS x FROM numbers(10000));
SELECT 'if_default_value_branch', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, if(number % 2 = 0, '', 'z') AS x FROM numbers(10000));
SELECT 'multiif_plain', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, multiIf(number % 5 = 0, 'five', number % 3 = 0, 'three', 'other') AS x FROM numbers(10000));
SELECT 'multiif_nullable', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, multiIf(nullIf(number % 5, 1) = 0, 'five', number % 7 = 0, NULL, '') AS x FROM numbers(10000));
SELECT 'multiif_duplicate_branches', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, multiIf(number % 5 = 0, 'same', number % 3 = 0, 'same', 'other') AS x FROM numbers(10000));
SELECT 'transform_number', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(number % 5, [0, 1, 2], ['a', 'b', 'c'], 'other') AS x FROM numbers(10000));
SELECT 'transform_string', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(toString(number % 5), ['0', '1'], ['a', 'b'], 'z') AS x FROM numbers(10000));
SELECT 'transform_fixed_string', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(toFixedString(toString(number % 5), 1), ['0', '1'], ['a', 'b'], 'z') AS x FROM numbers(10000));
SELECT 'transform_nullable_input', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(nullIf(number % 5, 3), [0, 1, 2], ['a', 'b', 'c'], 'd') AS x FROM numbers(10000));
SELECT 'transform_low_cardinality_input', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(toLowCardinality(toString(number % 5)), ['0', '1'], ['a', 'b'], 'z') AS x FROM numbers(10000));
SELECT 'transform_decimal_input', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(toDecimal32(number % 5, 2), [toDecimal32(1, 2)], ['one'], 'x') AS x FROM numbers(10000));
SELECT 'transform_null_value', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(number % 5, [0, 1], ['a', NULL], 'd') AS x FROM numbers(10000));
SELECT 'transform_null_default', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(number % 5, [0, 1], ['a', 'b'], NULL) AS x FROM numbers(10000));
SELECT 'transform_duplicate_values', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(number % 5, [0, 1, 2], ['a', 'a', 'other'], 'other') AS x FROM numbers(10000));
SELECT 'transform_large_mapping', any(toTypeName(x)), count(), groupBitXor(sipHash64(number, x)) FROM (SELECT number, transform(number % 1000, range(600), arrayMap(x -> toString(x), range(600)), 'big') AS x FROM numbers(10000));

-- The results of many blocks go through inserts and aggregation.
SET optimize_if_transform_const_strings_to_lowcardinality = 1;
DROP TABLE IF EXISTS t_if_lowcardinality_result;
CREATE TABLE t_if_lowcardinality_result (s LowCardinality(String), n LowCardinality(Nullable(String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_if_lowcardinality_result SELECT transform(number % 5, [0, 1], ['a', 'b'], 'c'), multiIf(number % 3 = 0, 'x', number % 3 = 1, NULL, 'y') FROM numbers(10000);
INSERT INTO t_if_lowcardinality_result SELECT if(number % 2 = 0, 'a', 'd'), if(number % 2 = 0, NULL, 'x') FROM numbers(10000);
SELECT s, count() FROM t_if_lowcardinality_result GROUP BY s ORDER BY s;
SELECT n, count() FROM t_if_lowcardinality_result GROUP BY n ORDER BY n;
SELECT transform(number % 5, [0, 1], ['a', 'b'], 'c') AS x, count() FROM numbers(10000) GROUP BY x ORDER BY x;
SELECT if(number % 7 = 0, 'a', 'b') AS x, count() FROM numbers(10000) GROUP BY x ORDER BY x;
DROP TABLE t_if_lowcardinality_result;
