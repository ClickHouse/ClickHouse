-- `compatibility` with versions before 26.8 must restore the old behavior of `JSONExtract`, which parsed
-- `DateTime` values according to `date_time_input_format` rather than `cast_string_to_date_time_mode`.

SET date_time_input_format = 'best_effort', cast_string_to_date_time_mode = 'basic';

SET json_extract_use_cast_string_to_date_time_mode = 1;
SELECT JSONExtract('{"createdAt":"2026-10-19T07:30:26.743Z"}', 'createdAt', 'DateTime64(3, \'UTC\')');

SET json_extract_use_cast_string_to_date_time_mode = 0;
SELECT JSONExtract('{"createdAt":"2026-10-19T07:30:26.743Z"}', 'createdAt', 'DateTime64(3, \'UTC\')');
SELECT JSONExtract('{"createdAt":"2026-10-19T07:30:26Z"}', 'createdAt', 'DateTime(\'UTC\')');

-- `compatibility` resets the setting, but does not touch `date_time_input_format` set explicitly.
SET json_extract_use_cast_string_to_date_time_mode = DEFAULT;
SET compatibility = '23.9', date_time_input_format = 'best_effort';
SELECT JSONExtract('{"createdAt":"2026-10-19T07:30:26.743Z"}', 'createdAt', 'DateTime64(3, \'UTC\')');

SET compatibility = '26.8', cast_string_to_date_time_mode = 'basic';
SELECT JSONExtract('{"createdAt":"2026-10-19T07:30:26.743Z"}', 'createdAt', 'DateTime64(3, \'UTC\')');
