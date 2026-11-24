-- `to_timestamp(seconds)` of PostgreSQL is `toDateTime64(seconds, 6)`.
SET session_timezone = 'UTC';

SELECT to_timestamp(1700000000), TO_TIMESTAMP(1700000000.5);
SELECT to_timestamp(0), to_timestamp(-1), toTypeName(to_timestamp(0));
SELECT to_timestamp(number) FROM numbers(2);
SELECT to_timestamp(NULL), to_timestamp(toNullable(1)), to_timestamp(CAST(NULL, 'Nullable(Int64)'));
SELECT to_timestamp(1700000000) = toDateTime64(1700000000, 6), to_timestamp(1.25) = toDateTime64(1.25, 6);

-- The form with a format string is not implemented.
SELECT to_timestamp('2023-11-14 22:13:20'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT to_timestamp('2023-11-14', 'YYYY-MM-DD'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
