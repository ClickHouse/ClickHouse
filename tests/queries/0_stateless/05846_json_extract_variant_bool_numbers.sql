-- Numbers and numeric strings must not go to the Bool alternative of a Variant in JSONExtract and JSON typed paths.
SET enable_variant_type = 1;

SELECT x, JSONExtract(x, 'Variant(Bool, Int64)') AS v, variantType(v) FROM (SELECT arrayJoin(['true', 'false', '0', '1', '2', '255', '256', '-1']) AS x) ORDER BY x;
SELECT JSONExtract('2.5', 'Variant(Bool, Float64)') AS v, variantType(v);
SELECT JSONExtract('2', 'Variant(Bool, Float64)') AS v, variantType(v);
SELECT JSONExtract('"7"', 'Variant(Bool, String)') AS v, variantType(v);
SELECT JSONExtract('"true"', 'Variant(Bool, String)') AS v, variantType(v);
SELECT JSONExtract('2', 'Variant(LowCardinality(Bool), Int64)') AS v, variantType(v) SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT j.a, arrayMap(x -> variantType(x), j.a) FROM (SELECT '{"a":[2,255,true]}'::JSON(a Array(Variant(Bool, Int64))) AS j);

DROP TABLE IF EXISTS t_json_variant_bool;
CREATE TABLE t_json_variant_bool (j JSON(a Variant(Bool, Int64), b Variant(Bool, String))) ENGINE = Memory;
INSERT INTO t_json_variant_bool VALUES ('{"a":2,"b":"7"}'), ('{"a":true,"b":false}');
SELECT j.a, variantType(j.a), j.b, variantType(j.b) FROM t_json_variant_bool ORDER BY toString(j.a);
DROP TABLE t_json_variant_bool;

-- Without another alternative a number still converts to Bool.
SELECT JSONExtract('2', 'Variant(Bool)') AS v, variantType(v);
SELECT JSONExtract('2', 'Bool');
