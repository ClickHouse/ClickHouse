-- The edit distance to an empty string is the length of the other string in code points, not in bytes
SELECT editDistanceUTF8('', 'é'), editDistanceUTF8('é', ''), editDistanceUTF8('', '我是谁'), levenshteinDistanceUTF8('我是谁', '');
SELECT editDistanceUTF8('', ''), editDistanceUTF8('', 'abc'), editDistanceUTF8('abc', '');

-- The same as the length in code points
SELECT editDistanceUTF8('', '我是谁') = lengthUTF8('我是谁'), editDistanceUTF8('', 'é') = lengthUTF8('é');

-- Column arguments
SELECT s, editDistanceUTF8(s, ''), editDistanceUTF8('', s) FROM (SELECT arrayJoin(['', 'é', '我是谁', 'abc']) AS s) ORDER BY s;

-- A truncated UTF-8 sequence is rejected as it is when both strings are non-empty
SELECT editDistanceUTF8('', '\xC3'); -- { serverError BAD_ARGUMENTS }
SELECT editDistanceUTF8('a', '\xC3'); -- { serverError BAD_ARGUMENTS }

-- editDistance counts bytes and is unchanged
SELECT editDistance('', 'é'), editDistance('', '我是谁');
