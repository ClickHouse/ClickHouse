SET in_null_as_unknown = 1;

SELECT a IN ([1], v), a NOT IN ([1], v)
FROM values('a Array(UInt8), v Nullable(UInt8)', ([1], NULL), ([2], NULL));

SELECT count() FROM values('a Array(UInt8), v Nullable(UInt8)', ([1], NULL), ([2], NULL))
WHERE a IN ([1], v);

SELECT a IN (materialize(v)), a NOT IN (materialize(v))
FROM values('a Array(UInt8), v Nullable(UInt8)', ([1], NULL), ([2], 7));

SELECT m IN (map('a', 1), v), m NOT IN (map('a', 1), v)
FROM values('m Map(String, UInt8), v Nullable(UInt8)', (map('a', 1), NULL), (map('b', 2), NULL));
