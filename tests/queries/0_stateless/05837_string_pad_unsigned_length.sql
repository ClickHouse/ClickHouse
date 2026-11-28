SELECT leftPad('abc', toUInt64('18446744073709551615'), 'x'); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT rightPad('abc', toInt128('18446744073709551618'), 'x'); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT rightPadUTF8('äbc', materialize(toUInt256('18446744073709551618')), 'x'); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT leftPadUTF8('äbc', toInt256('-18446744073709551614'), 'x'); -- { serverError INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE }
SELECT leftPad('abc', -1, 'x'); -- { serverError INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE }
SELECT leftPad('abc', toUInt128(2), 'x'), rightPadUTF8('äb', toInt256(3), 'x');
