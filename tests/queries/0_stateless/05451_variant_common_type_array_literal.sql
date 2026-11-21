-- An array literal whose elements have no common type is an `Array(Variant(...))` with `use_variant_as_common_type`,
-- like the same array built with the `array` function. The planner used to throw `NO_COMMON_TYPE` while naming the
-- constant, also when the query is sent to remote servers.

SET use_variant_as_common_type = 1;

SELECT [1, 'ClickHouse', ['Another', 'Array']] AS array, toTypeName(array);
SELECT [1, 'ClickHouse', ['Another', 'Array']] AS array, array.UInt8, array.String, array.`Array(String)`;
SELECT [1, 'ClickHouse'] AS array, toTypeName(array) FROM remote('127.0.0.{1,2}', system.one);
