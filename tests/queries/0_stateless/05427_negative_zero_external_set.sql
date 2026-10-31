-- Negative zero is equal to zero in a set on disk, as in a set in memory, for each kind of fixed-size key.

SET max_block_size = 1000;

SELECT 'Float64', groupArray(x IN (SELECT arrayJoin([0., 1.]))), groupArray(x IN (SELECT arrayJoin([-0., 1.])))
FROM (SELECT arrayJoin([-0., 0., 1., -1.]) AS x) SETTINGS max_bytes_before_external_set = 1;
SELECT 'Float32', groupArray(x IN (SELECT toFloat32(arrayJoin([0., 1.]))))
FROM (SELECT toFloat32(arrayJoin([-0., 0., 1., -1.])) AS x) SETTINGS max_bytes_before_external_set = 1;
SELECT 'BFloat16', groupArray(x IN (SELECT toBFloat16(arrayJoin([0., 1.]))))
FROM (SELECT toBFloat16(arrayJoin([-0., 0., 1., -1.])) AS x) SETTINGS max_bytes_before_external_set = 1;
SELECT 'Tuple', groupArray(x IN (SELECT (0., number) FROM numbers(2)))
FROM (SELECT (-toFloat64(number % 1), number) AS x FROM numbers(3)) SETTINGS max_bytes_before_external_set = 1;
SELECT 'Nullable', groupArray(x IN (SELECT toNullable(0.)))
FROM (SELECT toNullable(arrayJoin([-0., 0., 1.])) AS x) SETTINGS max_bytes_before_external_set = 1, transform_null_in = 1;
