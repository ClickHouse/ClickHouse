-- The accurate casts of a `Date`, `Date32`, `DateTime` or `DateTime64` value into `Date` or `DateTime` reject a value
-- the target cannot represent, whatever `date_time_overflow_behavior` is.

-- { echoOn }
SELECT accurateCastOrNull(toDate32('2200-01-01'), 'DateTime(\'UTC\')');
SELECT accurateCastOrNull(toDate32('2000-01-01'), 'DateTime(\'UTC\')');
SELECT accurateCastOrNull(toDate32('1969-12-31'), 'DateTime(\'UTC\')');
SELECT accurateCastOrNull(toDate('2106-02-07'), 'DateTime(\'UTC\')');
SELECT accurateCastOrNull(toDate('2106-02-08'), 'DateTime(\'UTC\')');
SELECT accurateCastOrNull(toDate('2106-02-07'), 'DateTime(\'America/Hermosillo\')');
SELECT accurateCastOrNull(toDate('1970-01-01'), 'DateTime(\'Asia/Tokyo\')');
SELECT accurateCastOrNull(toDate32('1970-01-01'), 'DateTime(\'Asia/Tokyo\')');
SELECT accurateCastOrNull(toDate32('1970-01-02'), 'DateTime(\'Asia/Tokyo\')');
SELECT accurateCastOrNull(toDateTime64('2106-02-07 06:28:15', 0, 'UTC'), 'DateTime(\'UTC\')');
SELECT accurateCastOrNull(toDateTime64('2106-02-07 06:28:16', 0, 'UTC'), 'DateTime(\'UTC\')');
SELECT accurateCastOrNull(toDateTime64('1969-12-31 23:59:59.9', 1, 'UTC'), 'DateTime(\'UTC\')');
SELECT accurateCastOrNull(toDateTime64('2020-01-01 00:00:00.5', 1, 'UTC'), 'DateTime(\'UTC\')');
SELECT accurateCastOrNull(toDate32('2149-06-06'), 'Date');
SELECT accurateCastOrNull(toDate32('2149-06-07'), 'Date');
SELECT accurateCastOrNull(toDate32('1969-12-31'), 'Date');
SELECT accurateCastOrNull(toDateTime64('2200-01-01 00:00:00', 0, 'UTC'), 'Date');
SELECT accurateCastOrNull(toDateTime64('1969-12-31 00:00:00', 0, 'UTC'), 'Date');
SELECT accurateCastOrNull(toDateTime(0, 'America/New_York'), 'Date');
SELECT accurateCastOrNull(toDateTime(86400, 'America/New_York'), 'Date');

SELECT accurateCastOrNull(toDate32('1969-12-31'), 'DateTime(\'UTC\')') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT accurateCastOrNull(toDate32('2106-02-07'), 'DateTime(\'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';
SELECT accurateCastOrNull(toDate32('2106-02-07'), 'DateTime(\'UTC\')') SETTINGS date_time_overflow_behavior = 'saturate';
SELECT accurateCastOrNull(toDateTime64('2200-01-01 00:00:00', 0, 'UTC'), 'DateTime(\'UTC\')') SETTINGS date_time_overflow_behavior = 'throw';

SELECT accurateCast(toDate32('2200-01-01'), 'DateTime(\'UTC\')'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCast(toDateTime64('2200-01-01 00:00:00', 0, 'UTC'), 'Date'); -- { serverError CANNOT_CONVERT_TYPE }
SELECT accurateCastOrDefault(toDate32('2200-01-01'), 'DateTime(\'UTC\')');
SELECT accurateCastOrDefault(toDate32('2200-01-01'), 'DateTime(\'UTC\')', toDateTime('2000-01-01 00:00:00', 'UTC'));

SELECT accurateCastOrNull(d, 'DateTime(\'UTC\')') FROM values('d Nullable(Date32)', ('2000-01-01'), ('2200-01-01'), (NULL));
SELECT accurateCastOrNull(toLowCardinality(toDate32('2200-01-01')), 'DateTime(\'UTC\')') SETTINGS allow_suspicious_low_cardinality_types = 1;
SELECT accurateCastOrNull(materialize(toDate32(d)), 'Date'), accurateCastOrNull(materialize(toDateTime64(d, 0, 'UTC')), 'DateTime(\'UTC\')')
FROM values('d String', ('2020-07-27'), ('2200-01-01'));

SELECT accurateCast(toDate('2020-01-01'), 'DateTime(\'UTC\')'), accurateCast(toDateTime('2020-01-01 12:00:00', 'UTC'), 'Date'),
    accurateCast(toDateTime64('2020-01-01 12:00:00.5', 1, 'UTC'), 'Date');
-- { echoOff }

DROP DICTIONARY IF EXISTS dict_05895;
DROP TABLE IF EXISTS src_05895;
CREATE TABLE src_05895 (id UInt64, s Date, e Date, v String) ENGINE = MergeTree ORDER BY id;
INSERT INTO src_05895 VALUES (1, '2020-01-01', '2020-12-31', 'in2020'), (1, '2100-01-01', '2149-06-06', 'late');
CREATE DICTIONARY dict_05895 (id UInt64, s Date, e Date, v String)
PRIMARY KEY id SOURCE(CLICKHOUSE(TABLE 'src_05895')) LIFETIME(0) LAYOUT(RANGE_HASHED()) RANGE(MIN s MAX e);

-- { echoOn }
SELECT dictGet('dict_05895', 'v', toUInt64(1), toDate32('2020-07-27'));
SELECT dictGet('dict_05895', 'v', toUInt64(1), toDate32('2200-01-01')); -- { serverError CANNOT_CONVERT_TYPE }
-- { echoOff }

DROP DICTIONARY dict_05895;
DROP TABLE src_05895;
