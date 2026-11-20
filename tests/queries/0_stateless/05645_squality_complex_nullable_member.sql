SET in_null_as_unknown = 1;

SELECT a IN ([1], v), a NOT IN ([1], v)
FROM values('a Array(UInt8), v Nullable(UInt8)', ([1], NULL), ([2], NULL));

SELECT count() FROM values('a Array(UInt8), v Nullable(UInt8)', ([1], NULL), ([2], NULL))
WHERE a IN ([1], v);

SELECT a IN (materialize(v)), a NOT IN (materialize(v))
FROM values('a Array(UInt8), v Nullable(UInt8)', ([1], NULL), ([2], 7));

SELECT m IN (map('a', 1), v), m NOT IN (map('a', 1), v)
FROM values('m Map(String, UInt8), v Nullable(UInt8)', (map('a', 1), NULL), (map('b', 2), NULL));

SELECT a IN (v), a NOT IN (v)
FROM values('a Array(UInt8), v Nullable(String)',
    ([1], NULL), ([2], NULL), ([1], '[1]'), ([2], '[1]'), ([2], '[2]'));

SELECT a IN ([1], v), a NOT IN ([1], v)
FROM values('a Array(UInt8), v Nullable(String)',
    ([1], NULL), ([2], NULL), ([1], '[1]'), ([2], '[1]'), ([2], '[2]'));

SELECT count() FROM values('a Array(UInt8), v Nullable(String)',
    ([1], NULL), ([2], NULL), ([1], '[1]'), ([2], '[1]'), ([2], '[2]'))
WHERE a IN (materialize(v));

SELECT m IN (map('a', 1), v), m NOT IN (map('a', 1), v)
FROM values('m Map(String, UInt8), v Nullable(String)', (map('a', 1), NULL), (map('b', 2), NULL));

SELECT m IN (s), m NOT IN (s)
FROM (SELECT map('a', toUInt8(1)) AS m, materialize(toNullable(toString(m))) AS s);

SELECT m IN (s), m NOT IN (s)
FROM (SELECT map('a', toUInt8(1)) AS m,
             materialize(toNullable(toString(map('b', toUInt8(1))))) AS s);

SELECT m IN (map('b', toUInt8(1)), s, NULL),
       m NOT IN (map('b', toUInt8(1)), s, NULL)
FROM (SELECT map('a', toUInt8(1)) AS m, materialize(toNullable(toString(m))) AS s);

SELECT count()
FROM (SELECT map('a', toUInt8(1)) AS m, materialize(toNullable(toString(m))) AS s)
WHERE m IN (s);

SELECT m IN (s), m NOT IN (s)
FROM (SELECT map('a', toUInt8(1)) AS m,
             materialize(toNullable(replaceAll(toString(m), ':', ' : '))) AS s);

SELECT m IN (s), m NOT IN (s), m IN (s, NULL)
FROM (SELECT map('a', toUInt8(0)) AS m,
             materialize(toNullable(toString(map('a', toUInt16(256))))) AS s);

SELECT map('a', toUInt8(1)) IN [toString(map('a', toUInt8(1)))];
SELECT map('a', toUInt8(1)) IN materialize([toString(map('a', toUInt8(1)))]);
SELECT map('a', toUInt8(1)) IN materialize([toNullable(toString(map('a', toUInt8(1)))), NULL]);
SELECT m IN a, m NOT IN a
FROM values('m Map(String, UInt8), a Array(Nullable(String))',
    (map('a', 1), [toString(map('a', 1))]),
    (map('a', 1), [NULL]));
