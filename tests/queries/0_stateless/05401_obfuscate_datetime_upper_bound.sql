-- The obfuscated time of day is attached to the source date. The first and the last local day of the
-- `DateTime` range are only partially representable, and the result must not wrap around the range.

SELECT x, toDate(x)
FROM obfuscate(SELECT toDateTime(toUInt32(4294967295), 'UTC') AS x)
LIMIT 1
SETTINGS obfuscate_seed = 'test';

SELECT count(), countIf(toDate(x) = '2106-02-07')
FROM (SELECT x FROM obfuscate(SELECT toDateTime(toUInt32(4294967295 - number * 100), 'UTC') AS x FROM numbers(100)) LIMIT 100)
SETTINGS obfuscate_seed = 'test';

SELECT count(), countIf(toDate(x) = '1970-01-01')
FROM (SELECT x FROM obfuscate(SELECT toDateTime(toUInt32(number * 100), 'Asia/Tokyo') AS x FROM numbers(100)) LIMIT 100)
SETTINGS obfuscate_seed = 'test';
