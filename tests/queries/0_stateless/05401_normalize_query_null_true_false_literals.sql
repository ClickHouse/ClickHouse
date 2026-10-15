-- NULL, TRUE and FALSE normalize like any other literal
SELECT a, b,
    normalizeQuery(a) = normalizeQuery(b) AS text,
    normalizedQueryHash(a) = normalizedQueryHash(b) AS hash,
    normalizedQueryHashUnordered(a) = normalizedQueryHashUnordered(b) AS unordered
FROM values('a String, b String',
    ('SELECT x FROM t WHERE a = 1', 'SELECT x FROM t WHERE a = NULL'),
    ('SELECT x FROM t WHERE a IN (1, 2)', 'SELECT x FROM t WHERE a IN (1, null)'),
    ('SELECT x FROM t WHERE a = 1', 'SELECT x FROM t WHERE a = true'),
    ('INSERT INTO t VALUES (1, 2)', 'INSERT INTO t VALUES (FALSE, NULL)'))
FORMAT TSV;

SELECT normalizeQuery(q) FROM values('q String',
    'SELECT x FROM t WHERE a IS NOT NULL',
    'SELECT x FROM t WHERE b IN (1, 2, NULL)',
    'SELECT true, False');
