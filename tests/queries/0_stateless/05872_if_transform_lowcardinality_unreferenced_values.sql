-- The `LowCardinality` result of `if`, `multiIf` and `transform` with constant string branches must hold in its
-- dictionary only the values that some row has chosen. A function of a `LowCardinality` argument is evaluated over
-- the whole dictionary, so a branch that no row takes would otherwise reach it, and here `match` would throw
-- CANNOT_COMPILE_REGEXP for the pattern `[`.

SET optimize_if_transform_const_strings_to_lowcardinality = 1;
SET optimize_if_transform_strings_to_enum = 0;
SET optimize_multiif_to_if = 0;

SELECT 'if', toTypeName(if(number = 0, 'a', '[')), match('a', if(number = 0, 'a', '[')) FROM numbers(1);
SELECT 'if_then_not_taken', match('a', if(number = 1, '[', 'a')) FROM numbers(1);
SELECT 'if_nullable_condition', match('a', if(nullIf(number, 0) = 5, '[', 'a')) FROM numbers(1);
SELECT 'multiIf', toTypeName(multiIf(number = 0, 'a', number = 1, 'b', '[')), match('a', multiIf(number = 0, 'a', number = 1, 'b', '[')) FROM numbers(1);
SELECT 'multiIf_first_not_taken', match('a', multiIf(number = 5, '[', number = 6, '(', 'a')) FROM numbers(3);
SELECT 'transform', toTypeName(transform(number, [0], ['a'], '[')), match('a', transform(number, [0], ['a'], '[')) FROM numbers(1);
SELECT 'transform_value_not_taken', match('a', transform(number, [0, 5], ['a', '['], 'a')) FROM numbers(3);
SELECT 'transform_large_mapping', sum(match('a', transform(number, range(1000), arrayMap(x -> if(x < 10, 'a', '['), range(1000)), '('))) FROM numbers(10);

-- The values that are chosen still reach the function.
SELECT 'if_taken', match('a', if(number = 0, 'a', '[')) FROM numbers(2); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT 'multiIf_taken', match('a', multiIf(number = 0, 'a', number = 1, 'b', '[')) FROM numbers(3); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT 'transform_taken', match('a', transform(number, [0], ['a'], '[')) FROM numbers(2); -- { serverError CANNOT_COMPILE_REGEXP }

-- The dictionary of every block has only the values of this block.
SET max_block_size = 5;
SELECT 'if_blocks', sum(match('a', if(number < 5, 'a', '.'))) FROM numbers(10);
SELECT 'multiIf_blocks', sum(match('a', multiIf(number < 5, 'a', number < 10, '.', '['))) FROM numbers(10);
SELECT 'transform_blocks', sum(match('a', transform(number, [0, 1, 2, 3, 4, 20], ['a', 'a', 'a', 'a', 'a', '['], '.'))) FROM numbers(10);

-- A `LowCardinality` input of `transform` is processed by the default implementation, over its dictionary.
SELECT 'transform_low_cardinality_input', toTypeName(x), groupArray(x) FROM (SELECT transform(toLowCardinality(toString(number % 3)), ['0', '1'], ['a', 'b'], 'z') AS x FROM numbers(6));
