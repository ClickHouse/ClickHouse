-- Regression for issue #101487: DateTime64 CSV parser consumed Bool column value
-- when epoch had fewer than 3 decimal digits (e.g. "0.0", "0", "100,true,...").

SET date_time_input_format = 'basic';

-- Test via format() function which avoids the multi-statement FORMAT CSV issue.
-- Each row: epoch value before a Bool column.
SELECT id, toUnixTimestamp64Micro(finished_at) AS ts_us, is_default_branch
FROM format(CSV, 'id UInt64, finished_at DateTime64(6, UTC), is_default_branch Bool, path String',
$$1,0.0,true,some/path/
2,0,true,some/path/
3,100,true,some/path/
4,0.00,true,some/path/
5,10.0,true,some/path/
6,1234.5,true,some/path/
7,0.000,true,some/path/
8,0.0,1,some/path/
9,0.0,0,some/path/
10,1772217900.12,true,some/path/
$$)
ORDER BY id;
