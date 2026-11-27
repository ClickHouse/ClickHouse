-- The start of the Date32 day 1970-01-01 is before the epoch in a time zone ahead of UTC.
-- `saturate` must give the epoch and `throw` must throw instead of wrapping to 2106-02-07.

SELECT toDateTime(toDate32('1970-01-01'), 'Asia/Tokyo'), toDateTime(toDate32('1970-01-01'), 'Europe/Amsterdam'), toDateTime(toDate32('1970-01-01'), 'America/New_York') SETTINGS date_time_overflow_behavior = 'saturate';

SELECT toDateTime(toDate32('1970-01-01'), 'Asia/Tokyo') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }
SELECT toDateTime(toDate32('1970-01-01'), 'America/New_York') SETTINGS date_time_overflow_behavior = 'throw';

-- Days where the direct conversion differs from the conversion through DateTime64(0).
SELECT 'Asia/Tokyo', groupArrayIf(number, toDateTime(toDate32('1970-01-01') + number, 'Asia/Tokyo') != toDateTime(toDateTime64(toDate32('1970-01-01') + number, 0, 'Asia/Tokyo'), 'Asia/Tokyo')) FROM numbers(49710) SETTINGS date_time_overflow_behavior = 'saturate';
SELECT 'Asia/Kolkata', groupArrayIf(number, toDateTime(toDate32('1970-01-01') + number, 'Asia/Kolkata') != toDateTime(toDateTime64(toDate32('1970-01-01') + number, 0, 'Asia/Kolkata'), 'Asia/Kolkata')) FROM numbers(49710) SETTINGS date_time_overflow_behavior = 'saturate';
SELECT 'Europe/Amsterdam', groupArrayIf(number, toDateTime(toDate32('1970-01-01') + number, 'Europe/Amsterdam') != toDateTime(toDateTime64(toDate32('1970-01-01') + number, 0, 'Europe/Amsterdam'), 'Europe/Amsterdam')) FROM numbers(49710) SETTINGS date_time_overflow_behavior = 'saturate';
SELECT 'America/New_York', groupArrayIf(number, toDateTime(toDate32('1970-01-01') + number, 'America/New_York') != toDateTime(toDateTime64(toDate32('1970-01-01') + number, 0, 'America/New_York'), 'America/New_York')) FROM numbers(49710) SETTINGS date_time_overflow_behavior = 'saturate';

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
CREATE TABLE src (k UInt8, d Date32) ENGINE = Memory;
CREATE TABLE dst (k UInt8, dt DateTime('Asia/Tokyo')) ENGINE = Memory;
INSERT INTO src VALUES (1, '1969-12-31'), (2, '1970-01-01'), (3, '1970-01-02');
INSERT INTO dst SELECT k, d FROM src;
SELECT k, dt FROM dst ORDER BY k;
DROP TABLE src;
DROP TABLE dst;

SELECT CAST(toNullable(toDate32('1970-01-01')) AS Nullable(DateTime('Asia/Tokyo'))), CAST([toDate32('1970-01-01')] AS Array(DateTime('Asia/Tokyo'))) SETTINGS date_time_overflow_behavior = 'saturate';
