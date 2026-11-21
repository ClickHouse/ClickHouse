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

-- Without `input_format_defaults_for_omitted_fields`, a value masked in `NativeWithDefaults` takes the default of the column
-- type, which is the empty state, and no `DEFAULT` expression is computed: neither of the aggregate function column nor of `k`.
SET input_format_defaults_for_omitted_fields = 0;
SELECT finalizeAggregation(x) FROM format(NativeWithDefaults, 'x AggregateFunction(uniq, UInt64)', x'010201780655496e7436340101000700000000000000');
SELECT finalizeAggregation(x) FROM format(NativeWithDefaults, 'x AggregateFunction(uniq, UInt64) DEFAULT arrayReduce(\'uniqState\', [toUInt64(42), toUInt64(43)])', x'010201780655496e7436340101000700000000000000');
SELECT k, finalizeAggregation(x) FROM format(NativeWithDefaults, 'k UInt32 DEFAULT 42, x AggregateFunction(uniq, UInt64)', x'0201016b0655496e743332010101780655496e743634000700000000000000');
-- An explicit `DEFAULT` in `Values` takes the `DEFAULT` expression also without the setting, next to an aggregate function column too.
SELECT x, finalizeAggregation(y) FROM format(Values, 'x UInt64 DEFAULT 42, y AggregateFunction(sum, UInt64)', '(DEFAULT, 7), (1, DEFAULT)');
SET input_format_defaults_for_omitted_fields = 1;

-- In the `array` mode, row 1 is marked, row 2 has the values 7 and 8.
SET aggregate_function_input_format = 'array';
SELECT finalizeAggregation(x) FROM format(NativeWithDefaults, 'x AggregateFunction(uniq, UInt64)', x'010201780d41727261792855496e74363429010100020000000000000007000000000000000800000000000000');
SELECT x, finalizeAggregation(y) FROM format(Values, 'x UInt64 DEFAULT 42, y AggregateFunction(sum, UInt64)', '(DEFAULT, [7])') SETTINGS input_format_defaults_for_omitted_fields = 0;
