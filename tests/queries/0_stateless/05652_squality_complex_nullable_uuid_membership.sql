SET in_null_as_unknown = 1;

SELECT a IN ([1], v), a NOT IN ([1], v)
FROM values('a Array(UInt8), v Nullable(UUID)', ([1], NULL), ([2], NULL));

SELECT a IN (v), a NOT IN (v)
FROM values('a Array(UInt8), v Nullable(UUID)', ([1], NULL));

SELECT a IN (materialize(CAST(NULL AS Nullable(UUID))))
FROM values('a Array(UInt8)', ([1]));
