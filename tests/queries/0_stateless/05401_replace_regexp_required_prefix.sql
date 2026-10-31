-- Rows that do not start with the literal following `^` cannot match and are copied through unchanged.
-- Keep the RE2 path: the JIT-compiled matcher does not use the prefix check.
SET min_count_to_compile_regular_expression = 1000000000;

SELECT replaceRegexpOne(arrayJoin(['', 'h', 'http://a.com/x', 'https://www.b.com/y', 'ftp://c.com/z', 'xhttp://d.com/']), '^https?://(?:www\\.)?([^/]+)/.*$', '\\1');
SELECT replaceRegexpAll(arrayJoin(['abcabc', 'xabc', 'ab']), '^abc', '<\\0>');

-- `(?m)` makes `^` match after a newline as well, and `(?i)` makes the literal case-insensitive.
SELECT replaceRegexpAll('x\nabc', '(?m)^abc', '<\\0>');
SELECT replaceRegexpOne('ABCd', '(?i)^abc', '<\\0>');
-- An alternation has no single required prefix.
SELECT replaceRegexpOne(arrayJoin(['abc', 'xyz', 'q']), '^abc|^x', '<\\0>');

SELECT replaceRegexpOne(toFixedString(arrayJoin(['abcd', 'zbcd']), 4), '^ab', '<\\0>');

-- Long runs of prefix rejects between matching rows, across the heuristics' windows and block boundaries.
SELECT
    count(),
    countIf(r = h),
    countIf(r = 'example.com')
FROM
(
    SELECT
        if(number % 64 = 0, concat('https://example.com/', repeat('p/', number % 5)), concat('nonmatching:', toString(number))) AS h,
        replaceRegexpOne(h, '^https?://([^/]+)/.*$', '\\1') AS r
    FROM numbers(20000)
)
SETTINGS max_block_size = 1000;
