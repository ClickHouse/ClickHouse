-- after a keyword that starts an expression, a sign before a number is unary too
SELECT a, b,
    normalizeQuery(a) = normalizeQuery(b) AS text,
    normalizedQueryHash(a) = normalizedQueryHash(b) AS hash,
    normalizedQueryHashUnordered(a) = normalizedQueryHashUnordered(b) AS unordered
FROM values('a String, b String',
    ('SELECT 1', 'SELECT -1'),
    ('SELECT CASE WHEN a THEN 1 ELSE 2 END', 'SELECT CASE WHEN a THEN -1 ELSE -2 END'),
    ('SELECT x FROM t WHERE 1 < a', 'SELECT x FROM t WHERE -1 < a'),
    ('SELECT x FROM t LIMIT 10', 'SELECT x FROM t LIMIT -10'))
FORMAT TSV;

-- after a column name the minus stays binary
SELECT a, b,
    normalizeQuery(a) = normalizeQuery(b) AS text,
    normalizedQueryHash(a) = normalizedQueryHash(b) AS hash,
    normalizedQueryHashUnordered(a) = normalizedQueryHashUnordered(b) AS unordered
FROM values('a String, b String',
    ('SELECT x - 1 FROM t', 'SELECT x + 1 FROM t'),
    ('SELECT x - 1 FROM t', 'SELECT x 1 FROM t'))
FORMAT TSV;

SELECT normalizeQuery(q) FROM values('q String',
    'SELECT -1',
    'SELECT CASE WHEN a THEN -1 ELSE -2 END');
