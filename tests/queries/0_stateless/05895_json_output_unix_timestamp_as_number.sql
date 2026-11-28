-- JSON formats write a Unix timestamp as a JSON number, so it is read back as the same timestamp.

SELECT toDateTime(1, 'UTC') AS d, toDateTime64(1.5, 3, 'UTC') AS d64, toDateTime64(-1.5, 3, 'UTC') AS d64_before_epoch
SETTINGS date_time_output_format = 'unix_timestamp'
FORMAT JSONEachRow;

SELECT toUnixTimestamp(d), toUnixTimestamp64Milli(d64)
FROM format(JSONEachRow, 'd DateTime(\'UTC\'), d64 DateTime64(3, \'UTC\')', '{"d":1,"d64":1.500}')
SETTINGS date_time_input_format = 'basic';

-- Typed paths of the JSON type read it back too.
SELECT toUnixTimestamp(j.d), toUnixTimestamp64Milli(j.d64)
FROM format(JSONEachRow, 'j JSON(d DateTime(\'UTC\'), d64 DateTime64(3, \'UTC\'))', '{"j":{"d":1,"d64":1.500}}')
SETTINGS date_time_input_format = 'basic';

-- Other date and time output formats stay strings.
SELECT toDateTime(1, 'UTC') AS d
SETTINGS date_time_output_format = 'iso'
FORMAT JSONEachRow;
