-- The splitByString tokenizer with multi-byte separators of at most 8 bytes, and with a longer one.

SELECT tokens('a, b="c" d=e', 'splitByString', [', ', '="', ' ', '=']);

SELECT tokens('a=', 'splitByString', ['="', '=']);
SELECT tokens('x,', 'splitByString', [', ']);

SELECT tokens('xaby', 'splitByString', ['ab', 'a']);
SELECT tokens('xaby', 'splitByString', ['a', 'ab']);

SELECT tokens('a12345678b12345678c', 'splitByString', ['12345678', '1']);
SELECT tokens('a123456789b', 'splitByString', ['123456789', '1']);
SELECT tokens('a123456789b', 'splitByString', ['1', '123456789']);

SELECT tokens('a→b c', 'splitByString', ['→', ' ']);

SELECT s, tokens(s, 'splitByString', ['="', '=']) FROM values('s String', 'k=', '"v', 'k="v', '');

DROP TABLE IF EXISTS tab;
CREATE TABLE tab (s String, INDEX idx s TYPE text(tokenizer = splitByString([', ', '="', ' ', '=']))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO tab VALUES ('a, b="c" d=e'), ('k='), ('"v'), ('k="v'), ('');
SELECT count() FROM tab WHERE hasAllTokens(s, ['b', 'c"']);
SELECT count() FROM tab WHERE hasAnyTokens(s, ['k', '"v']);
DROP TABLE tab;
