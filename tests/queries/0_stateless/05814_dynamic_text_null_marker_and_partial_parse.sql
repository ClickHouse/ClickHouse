-- A field that is exactly the null marker of the format is NULL in Dynamic,
-- even when the format does not apply null-as-default.
SET input_format_null_as_default = 0;

SELECT 'Values';
SELECT d, isNull(d), dynamicType(d) FROM format(Values, 'd Dynamic', '(42), (NULL), (null), (\'NULL\')');

SELECT 'CSV';
SELECT d, isNull(d), dynamicType(d) FROM format(CSV, 'd Dynamic', '42\n\\N\n"\\N"');

SELECT 'CSV with custom null representation';
SELECT d, isNull(d), dynamicType(d) FROM format(CSV, 'd Dynamic', '42\nmissing\nmissing_suffix') SETTINGS format_csv_null_representation = 'missing';

SELECT 'TSV';
SELECT d, isNull(d), dynamicType(d) FROM format(TSV, 'd Dynamic', '42\n\\N\nhello');

SELECT 'TSV without best-effort inference';
SELECT d, isNull(d), dynamicType(d) FROM format(TSV, 'd Dynamic', '42\n\\N') SETTINGS input_format_tsv_use_best_effort_in_schema_inference = 0;

SELECT 'TSVRaw';
SELECT d, isNull(d), dynamicType(d) FROM format(TSVRaw, 'd Dynamic', '42\n\\N\nhello');

SELECT 'JSONEachRow';
SELECT d, isNull(d), dynamicType(d) FROM format(JSONEachRow, 'd Dynamic', '{"d":42}\n{"d":null}\n{"d":"null"}');

SELECT 'Array(Dynamic)';
SELECT d, arrayMap(x -> isNull(x), d) FROM format(Values, 'd Array(Dynamic)', '([1, NULL, \'a\'])');

SELECT 'Whole text has no null marker';
SELECT 'NULL'::Dynamic AS d, isNull(d), dynamicType(d);

SELECT 'TSV escapes are decoded once';
SELECT d, hex(d::String), dynamicType(d) FROM format(TSV, 'd Dynamic', 'a\\\\tb');

SELECT 'A value of incomplete type is not parsed partially into an existing variant';
SELECT d, dynamicType(d) FROM format(Values, 'd Dynamic', '(42), ([])');
