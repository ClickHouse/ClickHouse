SET in_null_as_unknown = 1;

SELECT toUInt8(1) IN ['1', NULL], toUInt8(2) NOT IN ['1', NULL];
SELECT toUInt8(1) IN materialize(['1', NULL]), toUInt8(2) NOT IN materialize(['1', NULL]);
SELECT toUInt8(1) IN arr, toUInt8(2) NOT IN arr
FROM values('arr Array(Nullable(String))', (['1', NULL]));

SELECT toUInt8(1) IN ['256'], toUInt8(2) NOT IN ['256'];
SELECT toUInt8(1) IN materialize(['256']), toUInt8(2) NOT IN materialize(['256']);
SELECT toUInt8(1) IN arr, toUInt8(2) NOT IN arr
FROM values('arr Array(String)', (['256']));
