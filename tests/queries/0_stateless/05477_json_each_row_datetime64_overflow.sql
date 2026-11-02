CREATE TABLE json_datetime64 (ts DateTime64(9, 'UTC')) ENGINE = Memory;

SET date_time_input_format = 'basic', input_format_parallel_parsing = 0;
SET input_format_allow_errors_num = 1, input_format_allow_errors_ratio = 0.5;

INSERT INTO json_datetime64 FORMAT JSONEachRow
{"ts":"2001-01-01 00:00:00"}
{"ts":"2262-04-11 23:47:16.854775808"}
{"ts":"2002-01-01 00:00:00"}
{"ts":"2262-04-11 23:47:16.854775807"}

SELECT ts FROM json_datetime64 ORDER BY ts;

SET input_format_allow_errors_num = 0, input_format_allow_errors_ratio = 0;
INSERT INTO json_datetime64 SELECT * FROM format(JSONEachRow, 'ts DateTime64(9, \'UTC\')', '{"ts":"2262-04-11 23:47:16.854775808"}'); -- { serverError CANNOT_PARSE_DATETIME }

DROP TABLE json_datetime64;
