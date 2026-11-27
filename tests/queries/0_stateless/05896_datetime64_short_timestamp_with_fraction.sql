-- A DateTime64 text with a fractional part is a Unix timestamp even when its whole part is shorter than 5 digits,
-- and the throwing and the non-throwing parsers agree on it.
SET date_time_input_format = 'basic';
SET cast_string_to_date_time_mode = 'basic';

SELECT s, toUnixTimestamp64Milli(toDateTime64(s, 3, 'UTC')) AS throwing, toUnixTimestamp64Milli(toDateTime64OrNull(s, 3, 'UTC')) AS non_throwing
FROM (SELECT arrayJoin(['0.5', '1.500', '999.5', '99999.5', '-1.5']) AS s);

-- Without a fractional part a short number is still not a timestamp, and four digits followed by a delimiter are a year.
SELECT toDateTime64OrNull('1', 3, 'UTC'), toDateTimeOrNull('1', 'UTC'), toDateTime64OrNull('9999.5', 3, 'UTC');

-- A typed path of the JSON type uses the non-throwing parser.
SELECT toUnixTimestamp64Milli(j.d) FROM format(JSONEachRow, 'j JSON(d DateTime64(3, \'UTC\'))', '{"j":{"d":"1.500"}}');
