-- BSONEachRow output can be read back when the paths of two nested documents join into the same dotted string.
SELECT * FROM format(BSONEachRow, 'm Map(String, Array(Array(UInt32)))', formatRowNoNewline('BSONEachRow', map('a', [[1]], 'a.0', [[2, 3]]) AS m));
SELECT * FROM format(BSONEachRow, 'a Array(Array(UInt32)), `a.0` Array(UInt32)', formatRowNoNewline('BSONEachRow', [[1]] AS a, [2, 3] AS `a.0`));
SELECT * FROM format(BSONEachRow, 't Tuple(`a.b` Array(UInt32), a Tuple(b Array(UInt32)))', formatRowNoNewline('BSONEachRow', tuple([1, 2, 3], tuple([4]))::Tuple(`a.b` Array(UInt32), a Tuple(b Array(UInt32))) AS t));
SELECT mapValues(m) FROM format(BSONEachRow, 'm Map(String, Map(String, UInt32))', formatRowNoNewline('BSONEachRow', map('a' || char(0), map('x', 1), 'a\\u0000', map('yy', 2, 'zz', 3)) AS m));
