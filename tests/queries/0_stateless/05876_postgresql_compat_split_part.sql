-- `split_part(s, delimiter, position)` of PostgreSQL. The expected values are the ones PostgreSQL 14 returns.
SELECT split_part('a,b,c', ',', 2), SPLIT_PART('a--b--c', '--', 2);
SELECT [split_part('a,b,c', ',', 1), split_part('a,b,c', ',', 3), split_part('a,b,c', ',', 4)];

-- A negative position counts the fields from the end.
SELECT [split_part('a,b,c', ',', -1), split_part('a,b,c', ',', -3), split_part('a,b,c', ',', -4)];

-- With an empty delimiter the whole string is the only field. An empty string has no fields.
SELECT [split_part('abc', '', 1), split_part('abc', '', -1), split_part('abc', '', 2)];
SELECT [split_part('', ',', 1), split_part('', '', 1)];

-- The fields are the ones of a scan from the left, also when they are counted from the end.
SELECT [split_part('aaa', 'aa', 1), split_part('aaa', 'aa', 2), split_part('aaa', 'aa', -1), split_part('aaa', 'aa', -2), split_part('aaa', 'aa', 3)];
SELECT [split_part(',a,', ',', 1), split_part(',a,', ',', 3), split_part(',a,', ',', -1), split_part('abc', ',', 1), split_part('abc', ',', -1), split_part('abc', ',', 2)];

SELECT split_part(NULL, ',', 1), split_part('a', NULL, 1), split_part('a', ',', NULL);
SELECT split_part(s, ',', n) FROM (SELECT 'x,y,z' AS s, arrayJoin([1, 2, 3, -1]) AS n);
-- The row behind a `NULL` holds a zero position, which must not be reported as an error.
SELECT split_part(s, ',', n) FROM (SELECT materialize('x,y') AS s, arrayJoin([1, NULL, 2]) AS n);
SELECT [s, d, split_part(s, d, 2)] FROM (SELECT arrayJoin(['a,b', 'c;d', 'e']) AS s, arrayJoin([',', ';']) AS d) ORDER BY s, d;
SELECT toTypeName(split_part('a', ',', 1)), toTypeName(split_part(toNullable('a'), ',', 1));
SELECT [split_part('a,b,c', ',', 18446744073709551615)];

SELECT split_part('a,b,c', ',', 0); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT split_part('a,b,c', ',', materialize(0)); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT split_part('a', ','); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT split_part('a', ',', 'x'); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
