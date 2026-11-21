-- With `aggregate_function_input_format` = `value`, a value that the input does not have keeps the empty state of the
-- aggregate function instead of a state built from the default of its argument type.

SET aggregate_function_input_format = 'value';

-- `NativeWithDefaults`: row 1 is marked in the defaults mask, row 2 has a value (7 for `uniq`, 5 for `avg`).
SELECT finalizeAggregation(x) FROM format(NativeWithDefaults, 'x AggregateFunction(uniq, UInt64)', x'010201780655496e7436340101000700000000000000');
SELECT finalizeAggregation(x) FROM format(NativeWithDefaults, 'x AggregateFunction(avg, UInt32)', x'010201780655496e74333201010005000000');
-- Both rows are marked; `count` takes an empty `Tuple`.
SELECT finalizeAggregation(x) FROM format(NativeWithDefaults, 'x AggregateFunction(count)', x'01020178075475706c652829010101');
-- An aggregate function in a `Tuple`.
SELECT finalizeAggregation(t.1), t.2 FROM format(NativeWithDefaults, 't Tuple(AggregateFunction(uniq, UInt64), String)', x'01020174155475706c652855496e7436342c20537472696e672901010007000000000000000162');
-- The `DEFAULT` expression of the column is still computed for the marked row.
SELECT finalizeAggregation(x) FROM format(NativeWithDefaults, 'x AggregateFunction(uniq, UInt64) DEFAULT arrayReduce(\'uniqState\', [toUInt64(42), toUInt64(43)])', x'010201780655496e7436340101000700000000000000');

-- The default flag of `RowBinaryWithDefaults` and fields missing in `JSONEachRow`.
SELECT finalizeAggregation(x) FROM format(RowBinaryWithDefaults, 'x AggregateFunction(uniq, UInt64)', x'01000700000000000000');
SELECT countMerge(c), avgMerge(a) FROM format(JSONEachRow, 'k UInt8, c AggregateFunction(count), a AggregateFunction(avg, UInt32)', '{"k":1}');

-- In the `array` mode, row 1 is marked, row 2 has the values 7 and 8.
SET aggregate_function_input_format = 'array';
SELECT finalizeAggregation(x) FROM format(NativeWithDefaults, 'x AggregateFunction(uniq, UInt64)', x'010201780d41727261792855496e74363429010100020000000000000007000000000000000800000000000000');
