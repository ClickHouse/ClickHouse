-- a unary sign before a number normalizes like the number
SELECT a, b,
    normalizeQuery(a) = normalizeQuery(b) AS text,
    normalizedQueryHash(a) = normalizedQueryHash(b) AS hash,
    normalizedQueryHashUnordered(a) = normalizedQueryHashUnordered(b) AS unordered
FROM values('a String, b String',
    ('SELECT x FROM t WHERE a > 1', 'SELECT x FROM t WHERE a > -1'),
    ('SELECT x FROM t WHERE a IN (1, 2)', 'SELECT x FROM t WHERE a IN (1, -2)'),
    ('SELECT x FROM t WHERE a IN (1, 2)', 'SELECT x FROM t WHERE a IN (-1, +2)'),
    ('SELECT f(1, 2)', 'SELECT f(-1, - 2)'),
    ('SELECT x FROM t WHERE a = 1 AND b = 2', 'SELECT x FROM t WHERE a = -1 AND b = -2'),
    ('SELECT a * 2 FROM t', 'SELECT a * -2 FROM t'))
FORMAT TSV;

-- a binary minus stays
SELECT a, b,
    normalizeQuery(a) = normalizeQuery(b) AS text,
    normalizedQueryHash(a) = normalizedQueryHash(b) AS hash,
    normalizedQueryHashUnordered(a) = normalizedQueryHashUnordered(b) AS unordered
FROM values('a String, b String',
    ('SELECT a - 1 FROM t', 'SELECT a + 1 FROM t'),
    ('SELECT a - 1 FROM t', 'SELECT a 1 FROM t'),
    ('SELECT x - y FROM t', 'SELECT x y FROM t'))
FORMAT TSV;

SELECT normalizeQuery(q) FROM values('q String',
    'SELECT x FROM t WHERE a > -1 AND b IN (-1, 2, 3)',
    'SELECT x FROM t WHERE a > - 1',
    'SELECT a - 1 FROM t');
