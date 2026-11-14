-- hasAnyTokens / hasAllTokens with one needle and the default splitByNonAlpha tokenizer, checked against tokens().

SELECT 'all bytes';
SELECT
    countIf(hasAnyTokens(s, ['a']) != has(tokens(s), 'a')),
    countIf(hasAllTokens(s, ['b']) != has(tokens(s), 'b')),
    countIf(has(tokens(s), 'a'))
FROM (SELECT concat('a', char(number), 'a', char(number), 'b') AS s FROM numbers(256));

SELECT 'rows';
-- One block, so that a match can cross into the next row ('a' + 'b', 'x a' + 'b x').
SELECT s, hasAnyTokens(s, ['ab']), hasAllTokens(s, ['ab']), hasAllTokens(s, ['ab', 'ab']), hasAnyTokens(s, 'ab'), has(tokens(s), 'ab')
FROM (SELECT arrayJoin(['ab', 'xab', 'abx', 'x ab', 'ab x', 'a', 'b', '', 'ab_c', 'c-ab', 'пab', 'abп', 'ab€', 'aab ab', 'xa', 'b c', 'ab.ab', 'a\0b', 'x a', 'b x']) AS s)
SETTINGS max_block_size = 1000;

SELECT 'corpus';
DROP TABLE IF EXISTS t;
CREATE TABLE t (s String) ENGINE = Memory;
INSERT INTO t SELECT arrayStringConcat(arrayMap(j -> ['a', 'ab', 'b', '_', '-', ' ', '\t', '.', 'п', 'ä', '€', '1', '', 'ba'][1 + cityHash64(number, j) % 14], range(cityHash64(number) % 12))) FROM numbers(30000);

SELECT 'a', countIf(hasAnyTokens(s, ['a']) != has(tokens(s), 'a')), countIf(hasAllTokens(s, ['a']) != has(tokens(s), 'a')), countIf(hasAnyTokens(s, ['a'])) FROM t;
SELECT 'ab', countIf(hasAnyTokens(s, ['ab']) != has(tokens(s), 'ab')), countIf(hasAllTokens(s, ['ab']) != has(tokens(s), 'ab')), countIf(hasAnyTokens(s, ['ab'])) FROM t;
SELECT 'п', countIf(hasAnyTokens(s, ['п']) != has(tokens(s), 'п')), countIf(hasAllTokens(s, ['п']) != has(tokens(s), 'п')), countIf(hasAnyTokens(s, ['п'])) FROM t;
SELECT 'ä', countIf(hasAnyTokens(s, ['ä']) != has(tokens(s), 'ä')), countIf(hasAllTokens(s, ['ä']) != has(tokens(s), 'ä')), countIf(hasAnyTokens(s, ['ä'])) FROM t;
SELECT '€', countIf(hasAnyTokens(s, ['€']) != has(tokens(s), '€')), countIf(hasAllTokens(s, ['€']) != has(tokens(s), '€')), countIf(hasAnyTokens(s, ['€'])) FROM t;
SELECT '1a', countIf(hasAnyTokens(s, ['1a']) != has(tokens(s), '1a')), countIf(hasAllTokens(s, ['1a']) != has(tokens(s), '1a')), countIf(hasAnyTokens(s, ['1a'])) FROM t;
SELECT 'a b', countIf(hasAnyTokens(s, ['a b']) != has(tokens(s), 'a b')), countIf(hasAllTokens(s, ['a b']) != has(tokens(s), 'a b')), countIf(hasAnyTokens(s, ['a b'])) FROM t;
SELECT 'a_b', countIf(hasAnyTokens(s, ['a_b']) != has(tokens(s), 'a_b')), countIf(hasAllTokens(s, ['a_b']) != has(tokens(s), 'a_b')), countIf(hasAnyTokens(s, ['a_b'])) FROM t;
SELECT 'empty', countIf(hasAnyTokens(s, ['']) != has(tokens(s), '')), countIf(hasAllTokens(s, ['']) != has(tokens(s), '')), countIf(hasAnyTokens(s, [''])) FROM t;

SELECT 'two needles', countIf(hasAllTokens(s, ['ab', 'a']) != (has(tokens(s), 'ab') AND has(tokens(s), 'a'))) FROM t;
SELECT 'ngrams', countIf(hasAnyTokens(s, ['ab'], 'ngrams(2)') != has(tokens(s, 'ngrams', 2), 'ab')) FROM t;

SELECT 'wrappers';
SELECT countIf(hasAnyTokens(toNullable(s), ['ab']) != has(tokens(s), 'ab')), countIf(hasAllTokens(toLowCardinality(s), ['ab']) != has(tokens(s), 'ab')) FROM t;
SELECT hasAnyTokens('x ab', ['ab']), hasAllTokens('xab', ['ab']), hasAnyTokens('a.b', ['b']), hasAllTokens('ab', ['b']);

DROP TABLE t;
