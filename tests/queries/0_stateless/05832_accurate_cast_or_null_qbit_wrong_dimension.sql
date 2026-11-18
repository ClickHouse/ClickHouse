-- A value that does not fit the QBit dimension is NULL (or the default), not an exception.
SELECT accurateCastOrNull(materialize([1., 2., 3.]), 'QBit(Float32, 2)');
SELECT accurateCastOrNull([1., 2., 3.], 'QBit(Float32, 2)');
SELECT accurateCastOrDefault(materialize([1., 2., 3.]), 'QBit(Float32, 2)', [7, 8]::QBit(Float32, 2));
SELECT accurateCastOrNull(materialize('[1,2,3]'), 'QBit(Float32, 2)'), accurateCastOrNull(materialize('garbage'), 'QBit(Float32, 2)');
SELECT accurateCastOrNull(materialize([1., 2., 3.]::QBit(Float32, 3)), 'QBit(Float32, 2)');
SELECT accurateCastOrNull(tuple(materialize([1., 2., 3.])), 'Nullable(Tuple(QBit(Float32, 2)))');
SELECT n, accurateCastOrNull(a, 'QBit(Float32, 2)') FROM (SELECT 1 AS n, [1., 2.] AS a UNION ALL SELECT 2, [1., 2., 3.] UNION ALL SELECT 3, [] UNION ALL SELECT 4, [3., 4.]) ORDER BY n;
SELECT accurateCastOrNull(s, 'QBit(Float32, 2)') FROM (SELECT arrayJoin([NULL, '[1,2]', '[1,2,3]']::Array(Nullable(String))) AS s);
-- So a Variant, Dynamic or JSON value holding a vector converts.
SELECT accurateCastOrNull(materialize([1., 2.]::Dynamic), 'QBit(Float32, 2)'), accurateCastOrNull(materialize([1., 2.]::Variant(Array(Float64))), 'QBit(Float32, 2)');
SELECT accurateCastOrNull(d, 'QBit(Float32, 2)') FROM (SELECT arrayJoin([[1., 2.]::Dynamic, [1., 2., 3.]::Dynamic, '[3,4]'::Dynamic, 'garbage'::Dynamic, 42::Dynamic]) AS d);
SELECT accurateCastOrNull(tuple(materialize([1., 2.]::Dynamic)), 'Nullable(Tuple(QBit(Float32, 2)))');
SELECT accurateCastOrNull(j.e, 'QBit(Float32, 2)'), accurateCastOrDefault(j.e, 'QBit(Float32, 2)') FROM (SELECT materialize('{"e":[1,2]}')::JSON AS j);
SELECT accurateCastOrNull(j.e, 'QBit(Float32, 2)') FROM (SELECT materialize('{"e":[1,null]}')::JSON AS j);
SELECT JSONExtract(materialize('{"e":[1,2]}')::JSON, 'e', 'QBit(Float32, 2)');
-- Casts that are not asked to report still throw.
SELECT accurateCast(materialize([1., 2., 3.]), 'QBit(Float32, 2)'); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT CAST(materialize([1., 2., 3.]), 'Nullable(QBit(Float32, 2))'); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
SELECT CAST(materialize('garbage'), 'Nullable(QBit(Float32, 2))'); -- { serverError CANNOT_PARSE_INPUT_ASSERTION_FAILED }
