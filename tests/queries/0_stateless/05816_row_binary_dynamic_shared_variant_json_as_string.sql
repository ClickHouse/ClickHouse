-- `RowBinary` writes a `Dynamic` value the same way whether it is stored in a regular variant or in the shared variant,
-- also when `output_format_binary_write_json_as_string` writes `JSON` as a string.
SELECT formatRowNoNewline('RowBinary', '{"a":1}'::JSON::Dynamic(max_types=0)) = formatRowNoNewline('RowBinary', '{"a":1}'::JSON::Dynamic) SETTINGS output_format_binary_write_json_as_string = 0;

SET output_format_binary_write_json_as_string = 1;
SET input_format_binary_read_json_as_string = 1;

SELECT formatRowNoNewline('RowBinary', '{"a":1}'::JSON::Dynamic(max_types=0)) = formatRowNoNewline('RowBinary', '{"a":1}'::JSON::Dynamic);
SELECT formatRowNoNewline('RowBinary', ['{"a":1}'::JSON::Dynamic(max_types=0)]) = formatRowNoNewline('RowBinary', ['{"a":1}'::JSON::Dynamic]);

SELECT d, dynamicType(d) FROM format(RowBinary, 'd Dynamic(max_types=0)', (SELECT formatRowNoNewline('RowBinary', '{"a":1}'::JSON::Dynamic(max_types=0))));
-- The `Array(JSON)` path of a `JSON(max_dynamic_types=0)` is in the shared variant of its `Dynamic` subcolumn.
SELECT d, dynamicType(d) FROM format(RowBinary, 'd Dynamic', (SELECT formatRowNoNewline('RowBinary', j.arr) FROM (SELECT '{"arr":[{"b":1}]}'::JSON(max_dynamic_types=0) AS j)));
-- Skipping a `Dynamic(max_types=0)` column `d` in `RowBinaryWithNamesAndTypes`.
SELECT * FROM format(RowBinaryWithNamesAndTypes, 'y UInt8', (SELECT concat(unhex('0201640179'), char(20), 'Dynamic(max_types=0)', char(5), 'UInt8', formatRowNoNewline('RowBinary', '{"a":1}'::JSON::Dynamic(max_types=0), 7::UInt8))));
-- A value of another type in the shared variant.
SELECT d, dynamicType(d) FROM format(RowBinary, 'd Dynamic(max_types=0)', (SELECT formatRowNoNewline('RowBinary', 42::Int64::Dynamic(max_types=0))));
