-- New elements of a two-argument arrayResize get the default of the element type.
SELECT arrayResize([CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')], 3);
SELECT arrayResize([CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')], -3);
SELECT arrayResize(materialize([CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)')]), materialize(3));
SELECT arrayResize(CAST(arrayWithConstant(number, 'y'), 'Array(Enum8(\'x\' = 1, \'y\' = 2))'), 2) FROM numbers(3);
SELECT arrayResize([CAST('y', 'Enum16(\'x\' = 1000, \'y\' = 2000)')], 2);
SELECT arrayResize([CAST('y', 'Enum8(\'a\' = -1, \'b\' = 0, \'y\' = 2)')], 2);
SELECT arrayResize([CAST(('y', 5), 'Tuple(e Enum8(\'x\' = 1, \'y\' = 2), n UInt8)')], 2);
SELECT arrayResize([CAST('{"a":"y"}', 'JSON(a Enum8(\'x\' = 1, \'y\' = 2))')], 2);
SELECT arrayResize([CAST('y', 'Nullable(Enum8(\'x\' = 1, \'y\' = 2))')], 2), arrayResize([1, 2], 4), arrayResize(['a'], 2);
SELECT arraySlice(arrayResize(CAST(arrayMap(i -> (toString(i), 'y'), range(300)), 'Array(Tuple(LowCardinality(String), Enum8(\'x\' = 1, \'y\' = 2)))'), 302), 300);
SELECT arrayResize([tuple(CAST('y', 'Enum8(\'x\' = 1, \'y\' = 2)'), NULL)], 2);
SELECT length(arrayResize([], 2)), arrayResize([], 0), length(arrayResize(materialize([]), 2)), length(arrayResize([tuple([][1])], 2));
