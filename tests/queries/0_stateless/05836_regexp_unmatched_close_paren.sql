-- A `)` that closes no group is an invalid RE2 regexp and must not be answered by a substring search.

SELECT match('a', 'a)'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('xa)y', 'a)'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('a_suffix', 'a)|b('); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('b', 'a)|b('); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match(materialize('a'), 'a)'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT match('a', materialize('a)')); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT 'a' REGEXP 'a)'; -- { serverError CANNOT_COMPILE_REGEXP }
SELECT extract('abc', 'b)'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT extractAll('abc', 'b)'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT countMatches('abcb', 'b)'); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT splitByRegexp('b)', 'abc'); -- { serverError CANNOT_COMPILE_REGEXP }

-- The anchors and the group that `fullMatch` adds must not balance it.
SELECT fullMatch('a_suffix', 'a)|b('); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT fullMatch('b', 'a)|b('); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT fullMatch('b', materialize('a)|b(')); -- { serverError CANNOT_COMPILE_REGEXP }
SELECT fullMatch('a', 'a)'); -- { serverError CANNOT_COMPILE_REGEXP }

-- A `)` that is escaped, quoted, inside a character class or closing a group is valid.
SELECT match('a)', 'a\\)');
SELECT match('a)', '[)]');
SELECT match('ab', '(a)b');
SELECT match('a)', '\\Qa)\\E');
SELECT fullMatch('a)', 'a\\)'), fullMatch('xa)', 'a\\)');
SELECT fullMatch(')', '[)]'), fullMatch('x)', '[)]');
SELECT fullMatch(')', '[]a)]'), fullMatch('x)', '[]a)]');
SELECT fullMatch(')', '[[:alpha:])]'), fullMatch('x)', '[[:alpha:])]');
SELECT fullMatch('ab', '(a)b'), fullMatch('xab', '(a)b');
SELECT fullMatch('a)', '\\Qa)\\E'), fullMatch('xa)', '\\Qa)\\E');
SELECT fullMatch('b', 'a|(b)'), fullMatch('xb', 'a|(b)');

-- The invalid regexp must not be hidden by a skip index that prunes every granule.
DROP TABLE IF EXISTS t;
CREATE TABLE t (s String, INDEX idx s TYPE ngrambf_v1(3, 512, 2, 0)) ENGINE = MergeTree ORDER BY tuple() SETTINGS index_granularity = 1;
INSERT INTO t VALUES ('nothing here');
SELECT count() FROM t WHERE match(s, 'abc)') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError CANNOT_COMPILE_REGEXP }
SELECT count() FROM t WHERE fullMatch(s, 'abc)|xyz(') SETTINGS force_data_skipping_indices = 'idx'; -- { serverError CANNOT_COMPILE_REGEXP }
DROP TABLE t;
