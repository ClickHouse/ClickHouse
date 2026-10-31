-- a unary sign before a number normalizes like the number
SELECT a, b,
    normalizeQuery(a) = normalizeQuery(b) AS text,
    normalizedQueryHash(a) = normalizedQueryHash(b) AS hash
FROM values('a String, b String',
    ('SELECT x FROM t WHERE a > 1', 'SELECT x FROM t WHERE a > -1'),
    ('SELECT x FROM t WHERE a IN (1, 2)', 'SELECT x FROM t WHERE a IN (1, -2)'),
    ('SELECT x FROM t WHERE a IN (1, 2)', 'SELECT x FROM t WHERE a IN (-1, +2)'),
    ('SELECT f(1, 2)', 'SELECT f(-1, - 2)'),
    ('SELECT x FROM t WHERE a = 1 AND b = 2', 'SELECT x FROM t WHERE a = -1 AND b = -2'),
    ('SELECT a / 2 FROM t', 'SELECT a / -2 FROM t'))
FORMAT TSV;

-- a binary sign stays, also after a wildcard
SELECT a, b,
    normalizeQuery(a) = normalizeQuery(b) AS text,
    normalizedQueryHash(a) = normalizedQueryHash(b) AS hash
FROM values('a String, b String',
    ('SELECT a - 1 FROM t', 'SELECT a + 1 FROM t'),
    ('SELECT a - 1 FROM t', 'SELECT a 1 FROM t'),
    ('SELECT x - y FROM t', 'SELECT x y FROM t'),
    ('SELECT * - 1 FROM numbers(1)', 'SELECT * + 1 FROM numbers(1)'),
    ('SELECT t.* - 1 FROM t', 'SELECT t.* + 1 FROM t'),
    ('SELECT TOP 1 * - 1 FROM numbers(1)', 'SELECT TOP 1 * + 1 FROM numbers(1)'),
    ('SELECT TOP (1) * - 1 FROM numbers(1)', 'SELECT TOP (1) * + 1 FROM numbers(1)'),
    ('SELECT DISTINCT ON (number) * - 1 FROM numbers(1)', 'SELECT DISTINCT ON (number) * + 1 FROM numbers(1)'),
    ('SELECT TOP 1 WITH TIES * - 1 FROM numbers(1) ORDER BY number', 'SELECT TOP 1 WITH TIES * + 1 FROM numbers(1) ORDER BY number'))
FORMAT TSV;

-- normalizedQueryHashUnordered keeps the sign, so the order of the projections does not change it
SELECT normalizedQueryHashUnordered('SELECT -1, number FROM numbers(1)') = normalizedQueryHashUnordered('SELECT number, -1 FROM numbers(1)');

SELECT normalizeQuery(q) FROM values('q String',
    'SELECT x FROM t WHERE a > -1 AND b IN (-1, 2, 3)',
    'SELECT x FROM t WHERE a > - 1',
    'SELECT a - 1 FROM t',
    'SELECT TOP 1 * - 1 FROM t');
