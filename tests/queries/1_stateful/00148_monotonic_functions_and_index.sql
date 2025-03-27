SET max_rows_to_read = 60000;

SELECT count() FROM test.hits WHERE -CounterID = -1731;
SELECT count() FROM test.hits WHERE abs(-CounterID) = 1731;
SELECT count() FROM test.hits WHERE -abs(CounterID) = -1731;
SELECT count() FROM test.hits WHERE toUInt32(CounterID) = 1731;
SELECT count() FROM test.hits WHERE toInt32(CounterID) = 1731;
SELECT count() FROM test.hits WHERE toFloat32(CounterID) = 1731;

SET max_rows_to_read = 0;

SELECT count() FROM test.hits WHERE toInt16(CounterID) = 1731;
SELECT count() FROM test.hits WHERE toInt8(CounterID) = toInt8(1731);

SELECT count() FROM test.hits WHERE toDate(toUInt16(CounterID)) = toDate(1731);

SELECT uniq(CounterID), uniqUpTo(5)(toInt8(CounterID)), count() FROM test.hits WHERE toInt8(CounterID + 1 - 1) = toInt8(1731);
SELECT uniq(CounterID), uniqUpTo(5)(toInt8(CounterID)), count() FROM test.hits WHERE toInt8(CounterID) = toInt8(1731);

SELECT uniq(CounterID), uniqUpTo(5)(toInt16(CounterID)), count() FROM test.hits WHERE toInt16(CounterID + 1 - 1) = 1731;
SELECT uniq(CounterID), uniqUpTo(5)(toInt16(CounterID)), count() FROM test.hits WHERE toInt16(CounterID) = 1731;

SET max_rows_to_read = 500000;

SELECT uniq(CounterID), count() FROM test.hits WHERE toString(CounterID) = '1731';

SET max_rows_to_read = 2200000;

SELECT count() FROM test.hits WHERE CounterID < 732797;
SELECT count() FROM test.hits WHERE CounterID <= 732797;
SELECT count() FROM test.hits WHERE CounterID < 732797 AND CounterID > 107931;
SELECT count() FROM test.hits WHERE CounterID < 732797 AND CounterID >= 107931;
SELECT count() FROM test.hits WHERE CounterID <= 732797 AND CounterID > 107931;
SELECT count() FROM test.hits WHERE CounterID <= 732797 AND CounterID >= 107931;
SELECT count() FROM test.hits WHERE -CounterID > -732797;
SELECT count() FROM test.hits WHERE -CounterID >= -732797;
SELECT count() FROM test.hits WHERE -CounterID > -732797 AND CounterID > 107931;
SELECT count() FROM test.hits WHERE -CounterID > -732797 AND CounterID >= 107931;
SELECT count() FROM test.hits WHERE -CounterID >= -732797 AND CounterID > 107931;
SELECT count() FROM test.hits WHERE -CounterID >= -732797 AND CounterID >= 107931;
SELECT count() FROM test.hits WHERE CounterID < 732797 AND -CounterID < -107931;
SELECT count() FROM test.hits WHERE CounterID < 732797 AND -CounterID <= -107931;
SELECT count() FROM test.hits WHERE CounterID <= 732797 AND -CounterID < -107931;
SELECT count() FROM test.hits WHERE CounterID <= 732797 AND -CounterID <= -107931;

SET max_rows_to_read = 0;

SELECT count() FROM test.hits WHERE EventDate = '2014-03-20';
SELECT count() FROM test.hits WHERE toDayOfMonth(EventDate) = 20;
SELECT count() FROM test.hits WHERE toDayOfWeek(EventDate) = 4;
SELECT count() FROM test.hits WHERE toUInt16(EventDate) = toUInt16(toDate('2014-03-20'));
SELECT count() FROM test.hits WHERE toInt64(EventDate) = toInt64(toDate('2014-03-20'));
SELECT count() FROM test.hits WHERE toDateTime(EventDate) = '2014-03-20 00:00:00';

SET max_rows_to_read = 50000;

SELECT count() FROM test.hits WHERE toMonth(EventDate) != 3;
SELECT count() FROM test.hits WHERE toYear(EventDate) != 2014;
SELECT count() FROM test.hits WHERE toDayOfMonth(EventDate) > 23 OR toDayOfMonth(EventDate) < 17;
