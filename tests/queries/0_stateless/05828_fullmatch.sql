-- Tests of the fullMatch function (the regexp must match the whole string)

SELECT '-- Basic: the whole string must match';
SELECT fullMatch('abcde', 'a.*e');
SELECT fullMatch('abcde', 'b.*d');
SELECT match('abcde', 'b.*d'); -- for contrast: match is unanchored

SELECT '-- Literal patterns';
SELECT fullMatch('abc', 'abc');
SELECT fullMatch('abcd', 'abc');
SELECT fullMatch('ab', 'abc');
SELECT fullMatch('xabc', 'abc');

SELECT '-- Explicit anchors are harmless';
SELECT fullMatch('abc', '^abc$');
SELECT fullMatch('abc', '^abc');
SELECT fullMatch('abc', 'abc$');

SELECT '-- A quantifier cannot make the start anchor optional';
SELECT fullMatch('abc', '^?abc');
SELECT fullMatch('xabc', '^?abc');
SELECT fullMatch('abc', '^*abc');
SELECT fullMatch('xabc', '^*abc');
SELECT fullMatch('abc', '?abc'); -- { serverError CANNOT_COMPILE_REGEXP }

SELECT '-- Anchors do not repair an invalid pattern';
SELECT fullMatch('$rest', unhex('5c')); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT fullMatch('a', '+a'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT fullMatch('a', '{1}a'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT fullMatch('a', materialize('+a')); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT fullMatch('{a}b', '{a}b');

SELECT '-- Top-level alternation is anchored as a whole';
SELECT fullMatch('zzz', 'zzz|gold.*');
SELECT fullMatch('gold-1', 'zzz|gold.*');
SELECT fullMatch('app-gold-1', 'zzz|gold.*');
SELECT match('app-gold-1', 'zzz|gold.*');

SELECT '-- The end of a range is a single character, so `[` there does not start a named class';
SELECT fullMatch('!abc', '[!-[:]|x:]'), fullMatch('zzx:]', '[!-[:]|x:]'), fullMatch('!', '[!-[:]|x:]'), fullMatch('x:]', '[!-[:]|x:]');
SELECT fullMatch('!abc', materialize('[!-[:]|x:]')), fullMatch('!abc', '[\\!-[:]|x:]'), fullMatch('!abc', '[\\x21-[:]|x:]');
SELECT fullMatch('!x:]\nzzz', '[!-[:](?m)x:]$'), fullMatch('ax', '[!-\\](]|x');
SELECT fullMatch('ax', '[[:alpha:]-[:digit:](]|x'), fullMatch('-x', '[-[:digit:](]|x'), fullMatch('1x', '[\\d-[:digit:](]|x'), fullMatch('ax', '[\\pL-[:digit:](]|x'), fullMatch('αx', '[\\p{Greek}-[:digit:](]|x');

SELECT '-- Empty haystack and empty pattern';
SELECT fullMatch('', '');
SELECT fullMatch('a', '');
SELECT fullMatch('', 'a');
SELECT fullMatch('', 'a*');

SELECT '-- . matches a line break unless (?-s) is given';
SELECT fullMatch('a\nb', 'a.b');
SELECT fullMatch('a\nb', '(?-s)a.b');

SELECT '-- UTF-8: a multi-byte codepoint is matched as a whole';
SELECT fullMatch('¥', '.');

SELECT '-- FixedString haystack';
SELECT fullMatch(toFixedString('abc', 3), 'abc');
SELECT fullMatch(toFixedString('abc', 3), 'ab');
SELECT fullMatch(materialize(toFixedString('abcde', 5)), 'a.*e');
SELECT fullMatch(materialize(toFixedString('abcde', 5)), 'b.*d');

SELECT '-- Aliases';
SELECT regexp_full_match('abcde', 'a.*e');
SELECT REGEXP_FULL_MATCH('abcde', 'b.*d');
SELECT Regexp_Full_Match('abcde', 'a.*e');
SELECT name, alias_to, case_insensitive FROM system.functions WHERE name = 'regexp_full_match';

SELECT '-- Non-constant haystacks and patterns';
DROP TABLE IF EXISTS tab;
CREATE TABLE tab (haystack String, pattern String) ENGINE = Memory;
INSERT INTO tab VALUES ('abcde', 'a.*e'), ('abcde', 'b.*d'), ('gold-1', 'zzz|gold.*'), ('app-gold-1', 'zzz|gold.*'), ('', ''), ('x', '.');
SELECT haystack, pattern, fullMatch(haystack, pattern) FROM tab ORDER BY ALL;
SELECT fullMatch(haystack, 'a.*e') FROM tab ORDER BY haystack;
SELECT fullMatch('abcde', pattern) FROM tab ORDER BY pattern;
SELECT fullMatch(materialize(toFixedString('abcde', 5)), pattern) FROM tab ORDER BY pattern;
DROP TABLE tab;

SELECT '-- An invalid pattern is an error';
SELECT fullMatch('a', '('); -- { serverError CANNOT_COMPILE_REGEXP }
