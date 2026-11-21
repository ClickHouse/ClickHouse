-- Schema inference with `date_time_input_format = 'basic'` infers a value with ':' between the date
-- parts as `DateTime`/`DateTime64`; reading the same value must succeed as well.

SET date_time_input_format = 'basic', session_timezone = 'UTC';

DESCRIBE TABLE format(JSONEachRow, '{"x":"2024:01:02 03:04:05"}');
SELECT x, toTypeName(x) FROM format(JSONEachRow, '{"x":"2024:01:02 03:04:05"}');

DESCRIBE TABLE format(JSONEachRow, '{"x":"2024:01:02 03:04:05.123"}');
SELECT x, toTypeName(x) FROM format(JSONEachRow, '{"x":"2024:01:02 03:04:05.123"}');

SELECT toDateTime('2024:01:02 03:04:05'), toDateTime64('2024:01:02 03:04:05.5', 1);
