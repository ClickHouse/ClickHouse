-- The start of the Date32 day 1970-01-01 is before the epoch in a time zone ahead of UTC.
-- `saturate` and `throw` must give the epoch instead of wrapping to 2106-02-07. `throw` must not reject it:
-- day 0 is the value behind a NULL, and the value with which accurateCastOrNull probes a Variant or Dynamic member.

SELECT toDateTime(toDate32('1970-01-01'), 'Asia/Tokyo'), toDateTime(toDate32('1970-01-01'), 'Europe/Amsterdam'), toDateTime(toDate32('1970-01-01'), 'America/New_York') SETTINGS date_time_overflow_behavior = 'saturate';

SELECT toDateTime(toDate32('1970-01-01'), 'Asia/Tokyo'), toDateTime(toDate32('1970-01-01'), 'America/New_York') SETTINGS date_time_overflow_behavior = 'throw';
SELECT toDateTime(toDate32('1969-12-31'), 'Asia/Tokyo') SETTINGS date_time_overflow_behavior = 'throw'; -- { serverError VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE }

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

-- NULL rows and Variant / Dynamic members under `throw`.
DROP TABLE IF EXISTS nullable_dates;
CREATE TABLE nullable_dates (k UInt8, d Nullable(Date32)) ENGINE = Memory;
INSERT INTO nullable_dates VALUES (1, NULL), (2, '2000-01-01');
SELECT k, toDateTime(d, 'Asia/Tokyo'), CAST(d AS Nullable(DateTime('Asia/Tokyo'))), CAST([d] AS Array(Nullable(DateTime('Asia/Tokyo')))) FROM nullable_dates ORDER BY k SETTINGS date_time_overflow_behavior = 'throw';
SELECT k, toDateTime(d) FROM nullable_dates ORDER BY k SETTINGS date_time_overflow_behavior = 'throw', session_timezone = 'Asia/Tokyo';
DROP TABLE nullable_dates;
SELECT accurateCastOrNull(toDate32('2000-01-01')::Variant(Date32, String), 'DateTime(\'Asia/Tokyo\')'), accurateCastOrNull(toDate32('2000-01-01')::Dynamic, 'DateTime(\'Asia/Tokyo\')') SETTINGS date_time_overflow_behavior = 'throw';
