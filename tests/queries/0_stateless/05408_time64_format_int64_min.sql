-- Formatting a Time64 that holds the minimal Int64 must not overflow.

SET enable_time_time64_type = 1;

SELECT toTime64(-9223372036854775808, 0);
SELECT toString(addSeconds(toTime64('00:00:00', 0), -9223372036854775808));
SELECT CAST(addSeconds(toTime64('00:00:00', toLowCardinality(0)), -9223372036854775808), 'Nullable(String)');
SELECT toTime64(-9223372036854775807, 0), toTime64(-3600000, 0), toTime64(-3599999, 0), toTime64(-1, 0), toTime64(9223372036854775807, 0);
