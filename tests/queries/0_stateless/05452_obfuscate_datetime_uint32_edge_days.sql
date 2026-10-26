-- `DateTime` holds the instants from 0 to 4294967295. Outside of UTC, the local days of these instants,
-- `1970-01-01` in 'Asia/Tokyo' and `1969-12-31` in 'America/New_York', are representable only in part,
-- and so is the local day `2106-02-07` of the last instant in every time zone, including UTC.
-- `obfuscate` keeps the source date of `DateTime` values on these days, instead of wrapping the obfuscated
-- time of day around into another century. The rows on these days alternate with rows on an ordinary day,
-- which makes the obfuscated time of day fall anywhere in the day: with this seed, 368, 798, 742, 342 and 945
-- of the 1000 rows on these days get an obfuscated time of day outside of the representable part.

SET obfuscate_seed = 'seed';
SET max_threads = 1;

SELECT 'Asia/Tokyo, first day', arraySort(groupUniqArray(toDate32(x))) FROM (SELECT x FROM obfuscate(
    SELECT if(number % 2 = 0, toDateTime(0, 'Asia/Tokyo') + toIntervalSecond(number),
        toDateTime('2000-01-01 00:00:00', 'Asia/Tokyo') + toIntervalSecond(number * 37)) AS x FROM numbers(2000)) LIMIT 2000);
SELECT 'America/New_York, first day', arraySort(groupUniqArray(toDate32(x))) FROM (SELECT x FROM obfuscate(
    SELECT if(number % 2 = 0, toDateTime(0, 'America/New_York') + toIntervalSecond(number),
        toDateTime('2000-01-01 00:00:00', 'America/New_York') + toIntervalSecond(number * 37)) AS x FROM numbers(2000)) LIMIT 2000);

SELECT 'UTC, last day', arraySort(groupUniqArray(toDate32(x))) FROM (SELECT x FROM obfuscate(
    SELECT if(number % 2 = 0, toDateTime(4294967295, 'UTC') - toIntervalSecond(number),
        toDateTime('2000-01-01 00:00:00', 'UTC') + toIntervalSecond(number * 37)) AS x FROM numbers(2000)) LIMIT 2000);
SELECT 'Asia/Tokyo, last day', arraySort(groupUniqArray(toDate32(x))) FROM (SELECT x FROM obfuscate(
    SELECT if(number % 2 = 0, toDateTime(4294967295, 'Asia/Tokyo') - toIntervalSecond(number),
        toDateTime('2000-01-01 00:00:00', 'Asia/Tokyo') + toIntervalSecond(number * 37)) AS x FROM numbers(2000)) LIMIT 2000);
SELECT 'America/New_York, last day', arraySort(groupUniqArray(toDate32(x))) FROM (SELECT x FROM obfuscate(
    SELECT if(number % 2 = 0, toDateTime(4294967295, 'America/New_York') - toIntervalSecond(number),
        toDateTime('2000-01-01 00:00:00', 'America/New_York') + toIntervalSecond(number * 37)) AS x FROM numbers(2000)) LIMIT 2000);
