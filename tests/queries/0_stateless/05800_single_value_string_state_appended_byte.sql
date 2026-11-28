-- A serialized `String` state of `any`/`min`/`max` from certain old versions ends with a non-zero byte
-- instead of the zero terminator, and that byte is appended to the value. A value whose length without
-- that byte is a power of two needs one more byte than allocated for the string. The adjacent state of
-- the next row would show a corruption of the appended byte.

SELECT
    number,
    finalizeAggregation(s) = if(number = 0, concat(repeat('A', 64), 'B'), '')
FROM
(
    SELECT
        number,
        CAST(if(number = 0, concat(unhex('41000000'), repeat('A', 64), 'B'), unhex('FFFFFFFF')),
            'AggregateFunction(any, String)') AS s
    FROM numbers(2)
)
ORDER BY number;

SELECT length(finalizeAggregation(CAST(concat(unhex('01010000'), repeat('A', 256), 'B'), 'AggregateFunction(max, String)')));
