SET in_null_as_unknown = 1;

SELECT a IN b, a IN materialize(b)
FROM values('a Array(UInt8), b Array(Dynamic)', ([1], [[1]]));

SELECT count() FROM values('a Array(UInt8), b Array(Dynamic)', ([1], [[1]]))
WHERE a IN b;

SELECT count() FROM values('a Array(UInt8), b Array(Dynamic)', ([1], [[1]]))
WHERE a IN materialize(b);
