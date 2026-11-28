-- Numeric 1/0 inside containers of Variant(Bool, <integer>) must go to the integer alternative
-- unless allow_special_bool_values_inside_variant is enabled, like at top level and in JSON.
SET allow_special_bool_values_inside_variant = 0;

SELECT '[1,0,true]'::Array(Variant(Bool, Int64)) AS v, arrayMap(x -> variantType(x), v);
SELECT '(1,0)'::Tuple(Variant(Bool, Int64), Variant(Bool, UInt32)) AS v, variantType(v.1), variantType(v.2);
SELECT v, mapApply((k, x) -> (k, variantType(x)), v) FROM format(Values, 'v Map(String, Variant(Bool, Int64))', $$({'a':1,'b':false})$$);
SELECT v, arrayMap(x -> variantType(x), v) FROM format(Values, 'v Array(Variant(Bool, Int64))', '([1,0,true])');
SELECT v, arrayMap(x -> variantType(x), v) FROM format(CSV, 'v Array(Variant(Bool, Int64))', '"[1,0,true]"');
SELECT v, arrayMap(x -> variantType(x), v) FROM format(TSV, 'v Array(Variant(Bool, Int64))', '[1,0,true]');
SELECT v, variantType(v) FROM format(Values, 'v Variant(Bool, UInt32)', '(1), (0), (true)');
SELECT v, arrayMap(x -> variantType(x), v) FROM format(JSONEachRow, 'v Array(Variant(Bool, Int64))', '{"v":[1,0,true]}');
SELECT '1'::Variant(Bool, Int64) AS v, variantType(v);

SELECT '[1,0]'::Array(Bool);
SELECT * FROM format(Values, 'v Array(Bool)', '([1,0])');
SELECT '[1,0]'::Array(Variant(Bool, Int64)) AS v, arrayMap(x -> variantType(x), v) SETTINGS allow_special_bool_values_inside_variant = 1;
SELECT v, variantType(v) FROM format(Values, 'v Variant(Bool, UInt32)', '(1), (0)') SETTINGS allow_special_bool_values_inside_variant = 1;
