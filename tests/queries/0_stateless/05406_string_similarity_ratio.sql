-- editDistanceRatio: 1 - editDistance(s1, s2) / max(length(s1), length(s2))
SELECT editDistanceRatio('kitten', 'sitting'), levenshteinRatio('kitten', 'sitting');
SELECT editDistanceRatio('', ''), editDistanceRatio('abc', ''), editDistanceRatio('', 'abc'), editDistanceRatio('abc', 'abc'), editDistanceRatio('abc', 'xyz');
SELECT editDistanceRatioUTF8('我是谁', '我是我'), levenshteinRatioUTF8('我是谁', '我是我'), editDistanceRatio('我是谁', '我是我');
-- An empty string against a multi-byte one: the lengths are code points, not bytes
SELECT editDistanceRatioUTF8('', 'é'), editDistanceRatioUTF8('我是谁', ''), stringSimilarityRatioUTF8('', 'é'), stringSimilarityRatioUTF8('我是谁', '');

-- stringSimilarityRatio: 2 * LCS / (length(s1) + length(s2)), the same value as Python's Levenshtein.ratio
SELECT stringSimilarityRatio('kitten', 'sitting'), stringSimilarityRatio('clickhouse', 'mouse'), stringSimilarityRatio('mouse', 'clickhouse');
SELECT stringSimilarityRatio('', ''), stringSimilarityRatio('abc', ''), stringSimilarityRatio('', 'abc'), stringSimilarityRatio('abc', 'abc'), stringSimilarityRatio('abc', 'xyz');
SELECT stringSimilarityRatioUTF8('我是谁', '我是我'), stringSimilarityRatio('我是谁', '我是我');

-- Strings longer than 64 symbols: the bit vector spans several words and the carry crosses word boundaries
SELECT stringSimilarityRatio(repeat('ab', 100), repeat('ba', 100));
SELECT stringSimilarityRatio(repeat('a', 1000), repeat('a', 999) || 'b');
SELECT stringSimilarityRatio(repeat('a', 64) || 'b', 'b' || repeat('a', 64));
SELECT stringSimilarityRatioUTF8(repeat('ő', 130), repeat('ő', 65) || repeat('ű', 65));

-- Constant and column arguments in every combination
SELECT s, stringSimilarityRatio(s, 'mouse'), stringSimilarityRatio('mouse', s), stringSimilarityRatio(s, s || 'x'), editDistanceRatio(s, 'mouse'), editDistanceRatio('mouse', s)
FROM (SELECT arrayJoin(['clickhouse', 'mouse', '', 'house']) AS s) ORDER BY s;

-- Errors: a truncated UTF-8 sequence, also when the other string is empty, and too long strings
SELECT stringSimilarityRatioUTF8('\xC3', 'a'); -- { serverError BAD_ARGUMENTS }
SELECT stringSimilarityRatioUTF8('', '\xC3'); -- { serverError BAD_ARGUMENTS }
SELECT editDistanceRatioUTF8('\xC3', 'a'); -- { serverError BAD_ARGUMENTS }
SELECT editDistanceRatioUTF8('', '\xC3'); -- { serverError BAD_ARGUMENTS }
SELECT stringSimilarityRatio(repeat('a', 70000), 'a'); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT editDistanceRatio(repeat('a', 70000), 'a'); -- { serverError TOO_LARGE_STRING_SIZE }

-- A byte from 0x80 to 0xBF without a lead byte does not throw: it is one symbol, as in editDistanceUTF8
SELECT editDistanceRatioUTF8('a\xB0', 'b');
SELECT stringSimilarityRatioUTF8('\x80', 'a');
SELECT stringSimilarityRatioUTF8('', '\x80');
SELECT editDistanceRatioUTF8('\x80', 'a');
SELECT editDistanceRatioUTF8('', '\x80');

-- The size limit protects the computation, which does not run when one string is empty
SELECT editDistanceRatio(repeat('a', 70000), '');
SELECT stringSimilarityRatio(repeat('a', 70000), '');
