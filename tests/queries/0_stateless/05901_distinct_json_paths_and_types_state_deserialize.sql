-- distinctJSONPathsAndTypes "Returns the sorted map of paths and types" of type
-- Map(String, Array(String)) -- see returned_value_distinctJSONPathsAndTypes in the
-- FunctionDocumentation block of src/AggregateFunctions/AggregateFunctionDistinctJSONPaths.cpp.
-- A state of that aggregate must decode the path count, every path and every path's type set.

DROP TABLE IF EXISTS t_djpat_src;
DROP TABLE IF EXISTS t_djpat_state;

CREATE TABLE t_djpat_src (id UInt8, j JSON) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_djpat_src VALUES (1, '{"a" : 42}'), (2, '{"b" : {"c" : true}}'), (3, '{"b" : {"c" : "Hello"}}');

CREATE TABLE t_djpat_state (s AggregateFunction(distinctJSONPathsAndTypes, JSON)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_djpat_state SELECT distinctJSONPathsAndTypesState(j) FROM t_djpat_src;

-- The state written to a part and read back decodes to the two paths, 'b.c' with both its types.
SELECT finalizeAggregation(s) FROM t_djpat_state;
-- ... which is the map the aggregate computes directly over the same rows.
SELECT finalizeAggregation(s) = (SELECT distinctJSONPathsAndTypes(j) FROM t_djpat_src) FROM t_djpat_state;

-- A state written by hand, paths and types out of order on the wire, is still returned sorted:
-- varint 2 | 'b.c' varint 2 'String' 'Bool' | 'a' varint 1 'Int64'
SELECT finalizeAggregation(CAST(unhex('0203622E630206537472696E6704426F6F6C01610105496E743634') AS AggregateFunction(distinctJSONPathsAndTypes, JSON))) AS m, toTypeName(m);

-- A type count above the 0xFFFFFF limit is rejected instead of reserved.
SELECT finalizeAggregation(CAST(unhex('010161FFFFFFFF0F') AS AggregateFunction(distinctJSONPathsAndTypes, JSON))); -- { serverError TOO_LARGE_ARRAY_SIZE }

DROP TABLE t_djpat_state;
DROP TABLE t_djpat_src;
