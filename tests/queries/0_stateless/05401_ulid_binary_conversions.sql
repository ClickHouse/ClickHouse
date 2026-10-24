-- Tags: no-fasttest
-- The fast build disables the optional ULID library.

-- Known vectors test byte order independently of round trips, including both endpoints.
SELECT hex(ULIDStringToNum(ulid)), toTypeName(ULIDStringToNum(ulid))
FROM (SELECT arrayJoin(['00000000000000000000000000', '00000000000000000000000001', '01ARZ3NDEKTSV4RRFFQ69G5FAV', '7ZZZZZZZZZZZZZZZZZZZZZZZZZ']) AS ulid);
SELECT ULIDNumToString(unhex(bytes)), toTypeName(ULIDNumToString(unhex(bytes)))
FROM (SELECT arrayJoin(['00000000000000000000000000000000', '00000000000000000000000000000001', '01563E3AB5D3D6764C61EFB99302BD5B', 'FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF']) AS bytes);

-- Constants and non-constant fixed-width columns use the same representation.
SELECT hex(ULIDStringToNum('01ARZ3NDEKTSV4RRFFQ69G5FAV'));
SELECT ULIDNumToString(unhex('01563E3AB5D3D6764C61EFB99302BD5B'));
SELECT hex(ULIDStringToNum(toFixedString('01ARZ3NDEKTSV4RRFFQ69G5FAV', 26)));
SELECT ULIDNumToString(toFixedString(unhex('01563E3AB5D3D6764C61EFB99302BD5B'), 16));
SELECT hex(ULIDStringToNum(toFixedString(materialize('01ARZ3NDEKTSV4RRFFQ69G5FAV'), 26))) FROM numbers(2);
SELECT ULIDNumToString(toFixedString(materialize(unhex('01563E3AB5D3D6764C61EFB99302BD5B')), 16)) FROM numbers(2);

-- Case and Crockford aliases normalize to uppercase canonical text.
SELECT ULIDNumToString(ULIDStringToNum('01arz3ndektsv4rrffq69g5fav'));
SELECT ULIDStringToNum(repeat('O', 26)) = ULIDStringToNum(repeat('0', 26));
SELECT ULIDStringToNum(repeat('I', 26)) = ULIDStringToNum(repeat('1', 26));
SELECT ULIDStringToNum(repeat('l', 26)) = ULIDStringToNum(repeat('1', 26));

-- Cover every possible byte at every binary position, including embedded zeros.
SELECT countIf(ULIDStringToNum(ULIDNumToString(bytes)) != bytes)
FROM (SELECT concat(repeat(char(0), intDiv(number, 256)), char(number % 256), repeat(char(0), 15 - intDiv(number, 256))) AS bytes FROM numbers(4096));
SELECT countIf(ULIDStringToNum(ULIDNumToString(bytes)) != bytes)
FROM (SELECT toFixedString(concat(repeat(char(0), intDiv(number, 256)), char(number % 256), repeat(char(0), 15 - intDiv(number, 256))), 16) AS bytes FROM numbers(4096));

-- Nullable data from external sources and low-cardinality columns.
SELECT hex(ULIDStringToNum(ulid)), toTypeName(ULIDStringToNum(ulid))
FROM values('ulid Nullable(String)', ('00000000000000000000000001'), (NULL))
SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT ULIDNumToString(unhex(bytes)), toTypeName(ULIDNumToString(unhex(bytes)))
FROM values('bytes Nullable(String)', ('00000000000000000000000000000001'), (NULL))
SETTINGS short_circuit_function_evaluation_for_nulls = 0;
SELECT isNull(ULIDStringToNum(NULL)), isNull(ULIDNumToString(NULL));
SELECT hex(ULIDStringToNum(toLowCardinality(materialize('00000000000000000000000001'))));
SELECT ULIDNumToString(toLowCardinality(materialize(unhex('00000000000000000000000000000001'))));

-- No rows must not access input or output buffers.
SELECT ULIDStringToNum(toString(number)) FROM numbers(0);
SELECT ULIDNumToString(toString(number)) FROM numbers(0);
SELECT ULIDStringToNum(toFixedString(toString(number), 26)) FROM numbers(0);
SELECT ULIDNumToString(toFixedString(toString(number), 16)) FROM numbers(0);

-- Normal short-circuit evaluation must skip invalid input in an unselected branch.
SELECT hex(if(id = 0, toFixedString('', 16), ULIDStringToNum(ulid)))
FROM values('id UInt8, ulid String', (0, 'invalid'), (1, '00000000000000000000000001'))
SETTINGS short_circuit_function_evaluation = 'enable';
SELECT if(id = 0, toFixedString(repeat('0', 26), 26), ULIDNumToString(bytes))
FROM values('id UInt8, bytes String', (0, 'invalid'), (1, unhex('00000000000000000000000000000001')))
SETTINGS short_circuit_function_evaluation = 'enable';

-- Reject malformed text before calling the decoder, for both string representations.
SELECT ULIDStringToNum(''); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToNum(repeat('0', 25)); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToNum(repeat('0', 27)); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToNum(concat('8', repeat('0', 25))); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToNum(concat('!', repeat('0', 25))); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToNum(concat(repeat('0', 25), 'U')); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToNum(concat(repeat('0', 25), char(0))); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToNum(concat(char(255), repeat('0', 25))); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToNum(concat(repeat('0', 25), char(128))); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToNum(toFixedString(concat(repeat('0', 25), char(255)), 26)); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToNum(toFixedString('8', 26)); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToNum(materialize(repeat('0', 25))); -- { serverError BAD_ARGUMENTS }

-- Binary strings are exact-width byte sequences, not integers or hex text.
SELECT ULIDNumToString(''); -- { serverError BAD_ARGUMENTS }
SELECT ULIDNumToString(repeat(char(0), 15)); -- { serverError BAD_ARGUMENTS }
SELECT ULIDNumToString(repeat(char(0), 17)); -- { serverError BAD_ARGUMENTS }
SELECT ULIDNumToString(materialize(repeat(char(0), 17))); -- { serverError BAD_ARGUMENTS }
SELECT ULIDStringToNum(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ULIDNumToString(1); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ULIDStringToNum(toFixedString('', 25)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ULIDNumToString(toFixedString('', 15)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT ULIDStringToNum(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT ULIDNumToString(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT ULIDStringToNum(repeat('0', 26), 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT ULIDNumToString(repeat(char(0), 16), 1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
