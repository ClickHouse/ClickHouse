-- Tags: no-fasttest

SET engine_file_truncate_on_insert = 1;
SET session_timezone = 'Europe/Amsterdam';

-- Default: unchanged, DateTime is written as UInt32.
INSERT INTO FUNCTION file(currentDatabase() || '_datetime_off.arrow', 'Arrow')
SELECT toDateTime('2026-01-02 03:04:05', 'UTC') AS dt;
DESCRIBE file(currentDatabase() || '_datetime_off.arrow', 'Arrow');

SET output_format_arrow_datetime_as_timestamp = 1;
SET output_format_arrow_low_cardinality_as_dictionary = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_datetime_on.arrow', 'Arrow')
SELECT
    toDateTime('2026-01-02 03:04:05', 'UTC') + number AS dt,
    toDateTime('2100-01-01 00:00:00', 'UTC') AS dt_after_2038,
    toDateTime('2026-01-02 03:04:05') AS dt_default_tz,
    toDateTime64('2026-01-02 03:04:05', 0) AS dt64_default_tz,
    if(number = 0, NULL, toDateTime('2026-01-02 03:04:05', 'UTC')) AS dt_nullable,
    [toDateTime('2026-01-02 03:04:05', 'Asia/Tokyo')] AS dt_array,
    tuple(toDateTime('2026-01-02 03:04:05', 'UTC')) AS dt_tuple,
    map(toDateTime('2026-01-02 03:04:05', 'UTC'), number) AS dt_map_key,
    toLowCardinality(toDateTime('2026-01-02 03:04:05', 'UTC')) AS dt_lc
FROM numbers(2);
DESCRIBE file(currentDatabase() || '_datetime_on.arrow', 'Arrow');
SELECT * FROM file(currentDatabase() || '_datetime_on.arrow', 'Arrow') ORDER BY dt;
SELECT formatDateTime(dt, '%F %T') FROM file(currentDatabase() || '_datetime_on.arrow', 'Arrow') ORDER BY dt;

INSERT INTO FUNCTION file(currentDatabase() || '_datetime_on.arrowstream', 'ArrowStream')
SELECT toDateTime('2026-01-02 03:04:05', 'UTC') AS dt, toDateTime('2100-01-01 00:00:00', 'UTC') AS dt_after_2038;
DESCRIBE file(currentDatabase() || '_datetime_on.arrowstream', 'ArrowStream');
SELECT * FROM file(currentDatabase() || '_datetime_on.arrowstream', 'ArrowStream');
