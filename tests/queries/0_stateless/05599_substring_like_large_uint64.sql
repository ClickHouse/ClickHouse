-- Regression for https://github.com/ClickHouse/ClickHouse/issues/125943
-- UInt64 values above Int64::max() must never become negative slicing arguments.
-- Preserve signed negative arguments and valid positive boundary values.

SELECT left('abcdefghij', toUInt64(9223372036854775807));
SELECT right('abcdefghij', toUInt64(9223372036854775807));
SELECT substring('abcdefghij', 3, toUInt64(9223372036854775807));
SELECT leftUTF8('Привет', toUInt64(9223372036854775807));
SELECT rightUTF8('Привет', toUInt64(9223372036854775807));
SELECT substringUTF8('Привет', 2, toUInt64(9223372036854775807));
SELECT byteSlice('hello', 2, toUInt64(9223372036854775807));
SELECT hex(bitSlice('AB', 1, toUInt64(9223372036854775807)));
SELECT bitSlice('AB', 2, toUInt64(9223372036854775807)) = bitSlice('AB', 2);
SELECT bitSlice('AB', materialize(toUInt64(2)), materialize(toUInt64(9223372036854775807))) = bitSlice('AB', 2);
SELECT left('abcdefghij', materialize(toUInt64(5)));
SELECT right('abcdefghij', materialize(toUInt64(5)));
SELECT substring('abcdefghij', materialize(toUInt64(3)), materialize(toUInt64(8)));
SELECT hex(bitSlice('AB', materialize(toUInt64(1)), materialize(toUInt64(8))));
SELECT left('abcdefghij', toInt64(-1));
SELECT right('abcdefghij', toInt64(-1));
SELECT substring('abcdefghij', toInt64(-1));
SELECT substring('abcdefghij', 3, toInt64(-1));

-- Both constant and dynamic arguments must reject out-of-range UInt64 values.
SELECT left('abcdefghij', toUInt64(9223372036854775808)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT leftUTF8('Привет', toUInt64(9223372036854775809)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT right('abcdefghij', toUInt64(18446744073709551615)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT rightUTF8('Привет', toUInt64(9223372036854775808)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring('abcdefghij', toUInt64(18446744073709551615)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring('abcdefghij', 3, toUInt64(9223372036854775809)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substringUTF8('Привет', toUInt64(9223372036854775808)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substringUTF8('Привет', 2, toUInt64(18446744073709551615)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT byteSlice('hello', 2, toUInt64(18446744073709551615)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT bitSlice('abcdefghij', toUInt64(9223372036854775808)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT bitSlice('abcdefghij', 1, toUInt64(9223372036854775808)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT bitSlice('abcdefghij', toUInt64(18446744073709551615), 1); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT left('abcdefghij', materialize(toUInt64(18446744073709551615))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT leftUTF8('Привет', materialize(toUInt64(9223372036854775808))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT right('abcdefghij', materialize(toUInt64(9223372036854775809))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT rightUTF8('Привет', materialize(toUInt64(18446744073709551615))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring('abcdefghij', materialize(toUInt64(18446744073709551615))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring('abcdefghij', 3, materialize(toUInt64(9223372036854775808))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring('abcdefghij', materialize(toUInt64(3)), toUInt64(9223372036854775808)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring('abcdefghij', toUInt64(3), materialize(toUInt64(18446744073709551615))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substringUTF8('Привет', materialize(toUInt64(9223372036854775808))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substringUTF8('Привет', 2, materialize(toUInt64(18446744073709551615))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT byteSlice('hello', materialize(toUInt64(2)), materialize(toUInt64(9223372036854775808))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT bitSlice('abcdefghij', materialize(toUInt64(9223372036854775808))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT bitSlice('abcdefghij', 1, materialize(toUInt64(9223372036854775808))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT bitSlice('abcdefghij', materialize(toUInt64(1)), toUInt64(18446744073709551615)); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT bitSlice('abcdefghij', toUInt64(1), materialize(toUInt64(18446744073709551615))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT bitSlice('abcdefghij', materialize(toUInt64(1)), materialize(toUInt64(9223372036854775808))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT left('abcdefghij', arrayJoin([toUInt64(5), toUInt64(9223372036854775808)])); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT right('abcdefghij', toNullable(materialize(toUInt64(18446744073709551615)))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- Array slicing retains its old constant and dynamic UInt64 semantics.
SELECT arraySlice([1, 2, 3, 4], toUInt64(18446744073709551615)) = arraySlice([1, 2, 3, 4], materialize(toUInt64(18446744073709551615)));
SELECT arraySlice([1, 2, 3, 4], toUInt64(18446744073709551615), 2) = arraySlice([1, 2, 3, 4], materialize(toUInt64(18446744073709551615)), materialize(toUInt64(2)));
SELECT arraySlice([1, 2, 3, 4], toUInt64(9223372036854775808)) = arraySlice([1, 2, 3, 4], materialize(toUInt64(9223372036854775808)));

-- FixedString, Enum, shifted bit offsets, and masked nullable inputs.
SELECT left(toFixedString('abcd', 4), toUInt64(9223372036854775807)) = 'abcd';
SELECT right(materialize(toFixedString('abcd', 4)), materialize(toUInt64(3))) = 'bcd';
SELECT substring(materialize(toFixedString('abcd', 4)), materialize(toUInt64(2)), materialize(toUInt64(3))) = 'bcd';
SELECT hex(bitSlice(toFixedString('AB', 2), 1, 8)) = '41';
SELECT substring(materialize(CAST('alpha', 'Enum8(\'alpha\' = 1)')), materialize(toUInt64(2)), materialize(toUInt64(3))) = 'lph';
SELECT bitSlice('AB', 2, toUInt64(9223372036854775807)) = bitSlice('AB', 2);
SELECT bitSlice('AB', -2, toUInt64(9223372036854775807)) = bitSlice('AB', -2);
SELECT bitSlice('AB', materialize(toInt64(-2)), materialize(toUInt64(9223372036854775807))) = bitSlice('AB', -2);
SELECT bitSlice('AB', materialize(toUInt64(2)), materialize(toUInt64(9223372036854775807))) = bitSlice('AB', 2);
SELECT arraySort(groupArray(ifNull(left('abcd', nullIf(if(number = 0, toUInt64(18446744073709551615), toUInt64(3)), toUInt64(18446744073709551615))), '<null>'))) = ['<null>', 'abc'] FROM numbers(2) SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT arraySort(groupArray(ifNull(substring('abcd', 2, nullIf(if(number = 0, toUInt64(18446744073709551615), toUInt64(2)), toUInt64(18446744073709551615))), '<null>'))) = ['<null>', 'bc'] FROM numbers(2) SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT arraySort(groupArray(ifNull(hex(bitSlice('AB', nullIf(if(number = 0, toUInt64(18446744073709551615), toUInt64(1)), toUInt64(18446744073709551615)), 8)), '<null>'))) = ['41', '<null>'] FROM numbers(2) SETTINGS short_circuit_function_evaluation_for_nulls = 0;

-- Out-of-range UInt64 remains rejected for these string sources.
SELECT left(toFixedString('abcd', 4), materialize(toUInt64(18446744073709551615))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring(materialize(toFixedString('abcd', 4)), materialize(toUInt64(9223372036854775808))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT bitSlice(toFixedString('AB', 2), 1, materialize(toUInt64(18446744073709551615))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring(materialize(CAST('alpha', 'Enum8(\'alpha\' = 1)')), materialize(toUInt64(18446744073709551615))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT substring(materialize(CAST('alpha', 'Enum8(\'alpha\' = 1)')), 2, materialize(toUInt64(9223372036854775808))); -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

-- Check NULL-masked length and offset values across both arguments.
SELECT arraySort(groupArray(ifNull(right('abcd', nullIf(if(number = 0, toUInt64(18446744073709551615), toUInt64(2)), toUInt64(18446744073709551615))), '<null>'))) = ['<null>', 'cd'] FROM numbers(2) SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT arraySort(groupArray(ifNull(substring('abcd', nullIf(if(number = 0, toUInt64(18446744073709551615), toUInt64(2)), toUInt64(18446744073709551615)), 2), '<null>'))) = ['<null>', 'bc'] FROM numbers(2) SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT arraySort(groupArray(ifNull(hex(bitSlice('AB', 1, nullIf(if(number = 0, toUInt64(18446744073709551615), toUInt64(8)), toUInt64(18446744073709551615)))), '<null>'))) = ['41', '<null>'] FROM numbers(2) SETTINGS short_circuit_function_evaluation_for_nulls = 0;
