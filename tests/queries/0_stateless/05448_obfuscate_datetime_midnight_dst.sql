-- On a day when DST starts at midnight, the wall-clock times from 00:00 to 00:59 do not exist: the clock moves
-- from 00:00 to 01:00 in 'America/Havana' on 2016-03-13, in 'Africa/Cairo' on 2024-04-26 and in
-- 'America/Santiago' on 2024-09-08. `obfuscate` keeps the source date of `DateTime` and `DateTime64` values,
-- also when the obfuscated time of day falls into this gap, instead of moving them to the previous day.
-- With this seed, 12, 27 and 17 of the 2000 rows of these days get an obfuscated time of day in the gap.

SET obfuscate_seed = 'seed';
SET max_threads = 1;

SELECT 'DateTime, America/Havana', arraySort(groupUniqArray(toDate(x))) FROM (SELECT x FROM obfuscate(
    SELECT toDateTime('2016-03-13 01:00:00', 'America/Havana') + toIntervalSecond(number * 37) AS x FROM numbers(2000)) LIMIT 2000);
SELECT 'DateTime, Africa/Cairo', arraySort(groupUniqArray(toDate(x))) FROM (SELECT x FROM obfuscate(
    SELECT toDateTime('2024-04-26 01:00:00', 'Africa/Cairo') + toIntervalSecond(number * 37) AS x FROM numbers(2000)) LIMIT 2000);
SELECT 'DateTime, America/Santiago', arraySort(groupUniqArray(toDate(x))) FROM (SELECT x FROM obfuscate(
    SELECT toDateTime('2024-09-08 01:00:00', 'America/Santiago') + toIntervalSecond(number * 37) AS x FROM numbers(2000)) LIMIT 2000);

SELECT 'DateTime64, America/Havana', arraySort(groupUniqArray(toDate(x))) FROM (SELECT x FROM obfuscate(
    SELECT toDateTime64('2016-03-13 01:00:00', 3, 'America/Havana') + toIntervalSecond(number * 37) AS x FROM numbers(2000)) LIMIT 2000);
SELECT 'DateTime64, Africa/Cairo', arraySort(groupUniqArray(toDate(x))) FROM (SELECT x FROM obfuscate(
    SELECT toDateTime64('2024-04-26 01:00:00', 3, 'Africa/Cairo') + toIntervalSecond(number * 37) AS x FROM numbers(2000)) LIMIT 2000);
SELECT 'DateTime64, America/Santiago', arraySort(groupUniqArray(toDate(x))) FROM (SELECT x FROM obfuscate(
    SELECT toDateTime64('2024-09-08 01:00:00', 3, 'America/Santiago') + toIntervalSecond(number * 37) AS x FROM numbers(2000)) LIMIT 2000);
