-- Tags: no-fasttest, use-vectorscan

-- `multiMatch*` compiles patterns with `HS_FLAG_UTF8`, where vectorscan keeps a character class as a
-- `boost::icl::interval_set`. Built against libc++ 22, `boost::icl` before 1.92 lost intervals in every
-- operation that spanned more than one of them, so a negated class kept only some of its members
-- and matched strings that `match` rejects: https://github.com/ClickHouse/ClickHouse/issues/117522

SELECT pattern, haystack, multiMatchAny(haystack, [pattern]) AS vectorscan, match(haystack, pattern) AS re2
FROM VALUES('pattern String, haystack String',
    ('[^A-Za-z]', 'abc'),
    ('[^A-Za-z]', 'a'),
    ('[^A-Za-z]', 'Z'),
    ('[^A-Za-z]', '!'),
    ('[^a-zA-Z]', 'A'),
    ('[^ac]', 'c'),
    ('[^Xa]', 'a'),
    ('[^aX]', 'X'),
    ('[^XaY]', 'a'),
    ('[^XYa]', 'a'),
    ('[^0-9a]', 'a'),
    ('[^A-Za-z0-9]', '5'),
    ('[^[:alnum:]]', 'a'),
    ('[^[:alnum:]]', '0'),
    ('[^[:alnum:]]', '_'),
    ('[[:^alnum:]]', 'z'),
    ('q[^Xa]', 'qa'),
    ('q[^Xa]', 'qb'),
    ('^[^\\p{L}]$', 'ê'),
    ('^[^\\p{L}]$', '5'),
    ('[^éx]', 'é'),
    ('[^\\p{L}\\p{N}]', '5'),
    ('[^\\w]', '_'),
    ('\\W', 'a'),
    ('[^\\s\\d]', '7'),
    ('[^bdfhjlnpa-z]', 'q'),
    ('[^bdfhjlnpa-z]', '!'),
    ('[^13579\\d]', '2'),
    ('[^acegikmoqsuwya-z0-9]', 'b'),
    ('[^acegikmoqsuwya-z0-9]', '5'),
    ('(?i)[^a-z]', 'A'),
    ('[^\\d\\s\\w]', 'x'),
    ('[^\\w\\-.]', '-'),
    ('[^\\p{Lu}\\p{Ll}]', 'a'),
    ('(?i)[^bdfa-z]', 'C'),
    ('[A-Za-z]', 'a'),
    ('[^ab]', 'b'));

SELECT multiMatchAnyIndex('5', ['[^A-Za-z0-9]', '[^13579\\d]', '\\d']);
SELECT arraySort(multiMatchAllIndices('a', ['[^A-Za-z]', 'a', '[^ac]', '[a-c]', '[^0-9a]']));
SELECT haystack, multiMatchAny(haystack, ['[^A-Za-z0-9]']) FROM (SELECT arrayJoin(['a', 'Z', '5', '!', '_']) AS haystack);
