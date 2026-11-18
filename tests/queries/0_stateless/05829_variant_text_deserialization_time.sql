-- Text values that Time, Time64, BFloat16 or Interval can parse go to that variant, not to String, at the intended rank.

SET enable_time_time64_type = 1;
SET allow_suspicious_variant_types = 1;
SET session_timezone = 'UTC';
SET date_time_input_format = 'best_effort';
SET cast_string_to_variant_use_inference = 1;

SELECT 'TSV';
SELECT toString(v), variantType(v) FROM format(TSV, 'v Variant(String, Time)', '12:00:00\nstring') ORDER BY 1;
SELECT toString(v), variantType(v) FROM format(TSV, 'v Variant(String, Time64(3))', '12:00:00.5\nstring') ORDER BY 1;
SELECT toString(v), variantType(v) FROM format(TSV, 'v Variant(String, BFloat16)', '1.5\nstring') ORDER BY 1;
SELECT 'CSV';
SELECT toString(v), variantType(v) FROM format(CSV, 'v Variant(String, Time)', '"12:00:00"\n"string"') ORDER BY 1;
SELECT 'JSONEachRow';
SELECT toString(v), variantType(v) FROM format(JSONEachRow, 'v Variant(String, Time)', '{"v":"12:00:00"}\n{"v":"string"}') ORDER BY 1;
SELECT 'Values';
SELECT toString(v), variantType(v) FROM format(Values, 'v Variant(String, Time)', '(\'12:00:00\'), (\'string\')') ORDER BY 1;
SELECT 'CAST';
SELECT variantType('12:00:00'::Variant(String, Time)), variantType('12:00:00.5'::Variant(String, Time64(3)));
SELECT 'JSONExtract';
SELECT variantType(JSONExtract('{"a":"12:00:00"}', 'a', 'Variant(Time64(3), String)')), variantType(JSONExtract('{"a":"12:00:00"}', 'a', 'Variant(Time, String)'));
SELECT 'Time and DateTime';
SELECT toString(v), variantType(v) FROM format(TSV, 'v Variant(String, Time, Time64(3), DateTime64(3))', '12:00:00\n12:00:00.5\n2020-01-01 00:00:00\nstring') ORDER BY 1;
SELECT 'Nested';
SELECT toString(v), variantType(v) FROM format(TSV, 'v Variant(Array(String), Array(Time))', '[\'12:00:00\']') ORDER BY 1;
SELECT 'Ranks';
SELECT variantType(v) FROM format(TSV, 'v Variant(Decimal32(2), Float32, BFloat16)', '1.5\nnan') ORDER BY 1;
SELECT toString(v), variantType(v) FROM format(TSV, 'v Variant(Float64, Time, DateTime)', '12\n12:00:00\n2020-01-01 00:00:00') ORDER BY 1;
SELECT toString(v), variantType(v) FROM format(TSV, 'v Variant(String, Time)', '12') ORDER BY 1;
SELECT toString(v), variantType(v) FROM format(JSONEachRow, 'v Variant(String, Time)', '{"v":42}') ORDER BY 1;
SELECT 'Interval';
SELECT toString(v), variantType(v) FROM format(TSV, 'v Variant(String, Date, IntervalDay)', '42\n20200101\nstring') ORDER BY 1;
SELECT variantType('42'::Variant(String, IntervalDay)), variantType('42'::Variant(FixedString(2), IntervalDay));
