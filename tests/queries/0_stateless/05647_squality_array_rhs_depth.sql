SET in_null_as_unknown = 1;

SELECT [1] IN [1], materialize([1]) IN materialize([1]);
SELECT [1] NOT IN [1], materialize([1]) NOT IN materialize([1]);
SELECT materialize([2]) IN materialize([1]);
SELECT materialize([1]) IN materialize([[1], [2]]);
SELECT count() FROM values('a Array(UInt8)', ([1]), ([2]))
WHERE a IN materialize([1]);
