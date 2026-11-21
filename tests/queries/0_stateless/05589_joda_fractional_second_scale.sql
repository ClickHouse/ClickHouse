-- Tags: no-fasttest
-- 'S' in Joda syntax is the fractional second with exactly `repetitions` digits: reading the
-- two-digit minimum other fields use swallowed the next character and overflowed into the seconds.

-- Exactly one digit is read now; the surplus digit makes the parse fail instead of leaking into
-- the seconds.
SELECT parseDateTime64InJodaSyntax('2021-01-04 23:12:00.99', 'yyyy-MM-dd HH:mm:ss.S'); -- { serverError CANNOT_PARSE_DATETIME }
SELECT parseDateTime64InJodaSyntax('2021-01-04 23:12:34.12', 'yyyy-MM-dd HH:mm:ss.S'); -- { serverError CANNOT_PARSE_DATETIME }

-- One digit parses, and the value is the fraction of a second: '.9' with scale 1 is 0.9 s.
SELECT parseDateTime64InJodaSyntax('2021-01-04 23:12:00.9', 'yyyy-MM-dd HH:mm:ss.S');

-- 'SS' is unchanged: two digits, scale 2.
SELECT parseDateTime64InJodaSyntax('2021-01-04 23:12:00.99', 'yyyy-MM-dd HH:mm:ss.SS');

-- The other fields keep their own behaviour.
SELECT parseDateTime64InJodaSyntax('2021-01-04 23:1:00', 'yyyy-MM-dd HH:mm:ss');
