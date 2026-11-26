SELECT toInt64(84) IN (toInt64(63), CAST(NULL AS Nullable(Int64))),
    toInt64(84) NOT IN (toInt64(63), CAST(NULL AS Nullable(Int64)));

SET in_null_as_unknown = 1;

SELECT toTypeName(toInt64(84) IN (toInt64(63), CAST(NULL AS Nullable(Int64))));
SELECT toInt64(84) IN (toInt64(63), CAST(NULL AS Nullable(Int64))),
    toInt64(84) NOT IN (toInt64(63), CAST(NULL AS Nullable(Int64)));
SELECT toInt64(63) IN (toInt64(63), CAST(NULL AS Nullable(Int64))),
    toInt64(63) NOT IN (toInt64(63), CAST(NULL AS Nullable(Int64)));
SELECT CAST(NULL AS Nullable(Int64)) IN (toInt64(63)),
    CAST(NULL AS Nullable(Int64)) NOT IN (toInt64(63));
SELECT CAST(NULL AS Nullable(Int64)) IN (), CAST(NULL AS Nullable(Int64)) NOT IN ();

SELECT number,
    number IN (number + toUInt64(1), if(number = 0, CAST(NULL AS Nullable(UInt64)), toNullable(number + toUInt64(2)))),
    number NOT IN (number + toUInt64(1), if(number = 0, CAST(NULL AS Nullable(UInt64)), toNullable(number + toUInt64(2)))),
    number IN (number + toUInt64(1), toNullable(number))
FROM numbers(2)
ORDER BY number;

SELECT number IN (materialize(toNullable(number))) FROM numbers(2) ORDER BY number;
SELECT number IN (materialize(CAST(NULL AS Nullable(UInt64)))) FROM numbers(2) ORDER BY number;

SET transform_null_in = 1;
SELECT toInt64(84) IN (toInt64(63), CAST(NULL AS Nullable(Int64)));
