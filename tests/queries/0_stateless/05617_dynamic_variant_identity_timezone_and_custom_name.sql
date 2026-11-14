-- A `Dynamic` value keeps the timezone and the custom name (`Bool`) of its type: values whose types
-- differ only in those (which `IDataType::equals` ignores) must not share a stored variant.

SELECT arrayMap(x -> dynamicType(x), [CAST(toDateTime(0, 'UTC') AS Dynamic), CAST(toDateTime(0, 'Europe/Paris') AS Dynamic)]);
SELECT arrayMap(x -> toString(x), [CAST(toDateTime(0, 'UTC') AS Dynamic), CAST(toDateTime(0, 'Europe/Paris') AS Dynamic)]);
SELECT arrayMap(x -> dynamicType(x), [CAST(toDateTime64(0, 3, 'UTC') AS Dynamic), CAST(toDateTime64(0, 3, 'Asia/Tokyo') AS Dynamic)]);
SELECT arrayMap(x -> dynamicType(x), [CAST([toDateTime(0, 'UTC')] AS Dynamic), CAST([toDateTime(0, 'Europe/Paris')] AS Dynamic)]);
SELECT dynamicElement(true::Dynamic, 'UInt8'), dynamicElement(true::Dynamic, 'Bool');
SELECT dynamicElement(toUInt8(2)::Dynamic, 'Bool'), dynamicElement(toUInt8(2)::Dynamic, 'UInt8');
SELECT arrayMap(x -> dynamicType(x), [CAST(true AS Dynamic), CAST(toUInt8(1) AS Dynamic)]);
DROP TABLE IF EXISTS t_dyn_identity;
CREATE TABLE t_dyn_identity (d Dynamic) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dyn_identity VALUES (true::Bool), (toUInt8(2)), (toDateTime(0, 'UTC')), (toDateTime(0, 'Europe/Paris')), ([true]::Array(Bool)), ([3]::Array(UInt8));
SELECT dynamicType(d), d.UInt8, d.Bool, d.`Array(UInt8)`, d.`Array(Bool)` FROM t_dyn_identity ORDER BY dynamicType(d), toString(d);
DROP TABLE t_dyn_identity;
CREATE TABLE t_dyn_identity (d Dynamic(max_types = 0)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_dyn_identity VALUES (true::Bool), (toUInt8(2)), ([true]::Array(Bool)), ([3]::Array(UInt8));
SELECT dynamicType(d), d.UInt8, d.Bool, d.`Array(UInt8)`, d.`Array(Bool)` FROM t_dyn_identity ORDER BY dynamicType(d), toString(d);
DROP TABLE t_dyn_identity;
