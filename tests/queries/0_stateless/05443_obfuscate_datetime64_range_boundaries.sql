-- `obfuscate` keeps the date of a `DateTime64` value. At the ends of the range of `DateTime64(9)`
-- (from `1677-09-21 00:12:43.145224192` to `2262-04-11 23:47:16.854775807` UTC), the source local day is
-- representable only in part, and the obfuscated time of day must stay in this part instead of wrapping
-- around into another century.

SET obfuscate_seed = 'seed';
SET max_threads = 1;

SELECT toDate32(x) FROM obfuscate(SELECT toDateTime64('2262-04-12 12:00:00', 9, 'Pacific/Kiritimati') AS x) LIMIT 1;

DROP VIEW IF EXISTS v_src;
CREATE VIEW v_src AS SELECT
    toDateTime64('2262-04-12 00:00:00', 9, 'Pacific/Kiritimati') + toIntervalSecond(number * 490) + toIntervalNanosecond(number * 1234567) AS kiritimati_max,
    toDateTime64('2262-04-11 00:00:00', 9, 'UTC') + toIntervalSecond(number * 800) + toIntervalNanosecond(number * 7654321) AS utc_max,
    toDateTime64('1677-09-21 00:12:44', 9, 'UTC') + toIntervalSecond(number * 800) + toIntervalNanosecond(number * 7654321) AS utc_min,
    toDateTime64('2020-01-01 00:00:00', 9, 'Europe/Amsterdam') + toIntervalSecond(number * 3607) + toIntervalNanosecond(number * 123456789) AS regular
FROM numbers(100);

DROP VIEW IF EXISTS v_obf;
CREATE VIEW v_obf AS SELECT * FROM obfuscate(SELECT * FROM v_src) LIMIT 100;

SELECT
    (SELECT groupArray(toDate32(kiritimati_max)) FROM v_src) = (SELECT groupArray(toDate32(kiritimati_max)) FROM v_obf),
    (SELECT groupArray(toDate32(utc_max)) FROM v_src) = (SELECT groupArray(toDate32(utc_max)) FROM v_obf),
    (SELECT groupArray(toDate32(utc_min)) FROM v_src) = (SELECT groupArray(toDate32(utc_min)) FROM v_obf),
    (SELECT groupArray(toDate32(regular)) FROM v_src) = (SELECT groupArray(toDate32(regular)) FROM v_obf),
    (SELECT groupArray(utc_max) FROM v_src) != (SELECT groupArray(utc_max) FROM v_obf),
    (SELECT groupArray(utc_min) FROM v_src) != (SELECT groupArray(utc_min) FROM v_obf);

DROP VIEW v_obf;
DROP VIEW v_src;
