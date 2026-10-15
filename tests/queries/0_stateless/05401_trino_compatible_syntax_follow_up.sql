-- Only `TRY_CAST(x AS T)` is the syntax of `TRY_CAST`. An ordinary call is still resolved as a lambda or a SQL UDF.
WITH (x, y) -> x + y AS try_cast SELECT try_cast(1, 2);
WITH x -> x * 10 AS TRY_CAST SELECT TRY_CAST(3), TRY_CAST('5' AS UInt8);
WITH (x, y) -> concat(x, y) AS try_cast SELECT try_cast('a' AS p, 'b'), p;
SELECT try_cast(1, 'UInt8'); -- { serverError UNKNOWN_FUNCTION }
SELECT TRY_CAST(greatest(1, 2) AS Decimal(10, 1)), TRY_CAST('300' AS Nullable(UInt8)), TRY_CAST('x' AS t AS UInt8), t;

-- The edit distance of UTF-8 strings counts code points also when one of the strings is empty.
SELECT editDistanceUTF8('', 'é'), editDistanceUTF8('é', ''), levenshtein_distance('', 'éé'), editDistanceUTF8('', ''), editDistanceUTF8('é', 'e');
