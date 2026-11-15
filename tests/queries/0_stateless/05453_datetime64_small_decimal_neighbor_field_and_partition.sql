SET date_time_input_format = 'basic', cast_string_to_date_time_mode = 'basic';

-- A small decimal `DateTime64` timestamp followed by a leading-dot numeric field must not be read as a
-- `YYYY.MM.DD` date across the field delimiter (the value is long enough to take the optimistic path).
SELECT * FROM format(TSV, 'a DateTime64(3, \'UTC\'), b Float64, c String', '1234.5\t.25\tpaddingpadding') SETTINGS date_time_input_format = 'basic';
SELECT * FROM format(TSV, 'a DateTime64(3, \'UTC\'), b Float64, c String', '-123.5\t.25\tpaddingpadding') SETTINGS date_time_input_format = 'basic';
SELECT * FROM format(CSV, 'a DateTime64(3, \'UTC\'), b Float64, c String', '1234.5,.25,paddingpadding') SETTINGS date_time_input_format = 'basic';
-- A real `YYYY.MM.DD` date is still read as a date.
SELECT * FROM format(TSV, 'a DateTime64(3, \'UTC\'), c String', '2024.02.19 01:02:03\tpaddingpadding') SETTINGS date_time_input_format = 'basic';

-- A partition literal in the same form as the timestamp is accepted.
DROP TABLE IF EXISTS t_dt64_small_decimal;
CREATE TABLE t_dt64_small_decimal (d DateTime64(3, 'UTC')) ENGINE = MergeTree PARTITION BY d ORDER BY d;
INSERT INTO t_dt64_small_decimal SELECT toDateTime64('1234.5', 3, 'UTC');
INSERT INTO t_dt64_small_decimal SELECT toDateTime64('2024-02-19 00:00:00', 3, 'UTC');
-- Extra non-zero fractional digits beyond the scale name another value.
ALTER TABLE t_dt64_small_decimal DROP PARTITION '1234.5001'; -- { serverError INVALID_PARTITION_VALUE }
-- An invalid calendar date in the `YYYY.MM.DD` layout is still rejected.
ALTER TABLE t_dt64_small_decimal DROP PARTITION '2024.02.30 00:00:00'; -- { serverError INVALID_PARTITION_VALUE }
ALTER TABLE t_dt64_small_decimal DROP PARTITION '1234.5';
SELECT d FROM t_dt64_small_decimal ORDER BY d;
DROP TABLE t_dt64_small_decimal;
