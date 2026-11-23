-- The `LowCardinality` result of `if`, `multiIf` and `transform` with constant string branches must hold in its
-- dictionary only the values that some row has chosen. A function that takes the result is executed over the whole
-- dictionary, so a branch that no row has chosen must not reach it: here it is an invalid regular expression.

SET optimize_if_transform_strings_to_enum = 0;
SET optimize_if_transform_const_strings_to_lowcardinality = 1;
SELECT toTypeName(if(number = 0, 'a', '[')), match('a', if(number = 0, 'a', '[')) FROM numbers(1);
SELECT match('a', if(number = 1, '[', 'a')) FROM numbers(1);
SELECT match('a', if(nullIf(number, 0) = 1, '[', 'a')) FROM numbers(1);
SELECT toTypeName(multiIf(number = 0, 'a', number = 1, '[', '(')), match('a', multiIf(number = 0, 'a', number = 1, '[', '(')) FROM numbers(1);
SELECT toTypeName(transform(number, [0, 1], ['a', '['], '(')), match('a', transform(number, [0, 1], ['a', '['], '(')) FROM numbers(1);
SELECT match('a', transform(toString(number), ['0', '1'], ['a', '['], '(')) FROM numbers(1);
SELECT match('a', if(number = 0, 'a', '[')) FROM numbers(2); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('a', multiIf(number = 0, 'a', number = 1, '[', '(')) FROM numbers(2); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('a', transform(number, [0, 1], ['a', '['], '(')) FROM numbers(2); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT number, if(number % 2 = 0, 'a', NULL) AS x, multiIf(number = 0, 'a', number = 1, NULL, '') AS y, transform(number, [0, 1], ['a', NULL], '') AS z, toTypeName(x), toTypeName(y), toTypeName(z) FROM numbers(3) SETTINGS max_block_size = 1;
