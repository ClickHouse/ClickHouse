-- A String argument of addDays is parsed per row, so the rows rejected by a short-circuit condition must not reach it.

DROP TABLE IF EXISTS t_add_interval_log;
DROP TABLE IF EXISTS t_add_interval_mt;

CREATE TABLE t_add_interval_log (k UInt64, ds String, nds Nullable(String), lcds LowCardinality(String)) ENGINE = Log;
INSERT INTO t_add_interval_log SELECT number, if(number % 10 = 0, 'x', '2020-01-01') AS s, s, s FROM numbers(100000);

SELECT sum(ds != 'x' AND addDays(ds, 1) > toDateTime('2000-01-01')) FROM t_add_interval_log;
SELECT sum(if(ds != 'x', addDays(ds, 1) > toDateTime('2000-01-01'), 0)) FROM t_add_interval_log;
-- Nullable and LowCardinality arguments.
SELECT sum(nds != 'x' AND addDays(nds, 1) > toDateTime('2000-01-01')) FROM t_add_interval_log;
SELECT sum(if(lcds != 'x', subtractDays(lcds, 1) > toDateTime('2000-01-01'), 0)) FROM t_add_interval_log;
-- The value behind a NULL is not parsed.
SELECT count(addDays(nullIf(ds, 'x'), 1)) FROM t_add_interval_log;

-- A delta that does not fit Int64, and ticks scaled with an overflow check, throw for the rejected rows too.
SELECT sum(number % 10 != 0 AND addDays(toDate('2020-01-01'), if(number % 10 = 0, 1e30, 1)) > toDate('2000-01-01')) FROM numbers(100000);
SELECT sum(number % 10 != 0 AND addDays(toDate('2020-01-01'), if(number % 10 = 0, toUInt64(18446744073709551615), toUInt64(1))) > toDate('2000-01-01')) FROM numbers(100000);
SELECT sum(number % 10 != 0 AND addMilliseconds(toDateTime('2020-01-01'), if(number % 10 = 0, toInt64(9223372036854775807), toInt64(1))) > toDateTime('2000-01-01')) FROM numbers(100000);
SELECT sum(number % 10 != 0 AND addMilliseconds(if(number % 10 = 0, toDateTime64('2262-04-11 23:47:16', 9, 'UTC'), toDateTime64('2020-01-01', 9, 'UTC')), toInt32(1000)) > toDateTime('2000-01-01')) FROM numbers(100000);
SELECT sum(number % 10 != 0 AND addSeconds(toDateTime64('2020-01-01', 9), if(number % 10 = 0, toInt64(10000000000), toInt64(1))) > toDateTime('2000-01-01')) FROM numbers(100000);
-- A LowCardinality delta: the dictionary value of the rejected rows is not evaluated.
SELECT sum(if(delta < 10, addDays(toDate('2020-01-01'), delta) > toDate('2000-01-01'), 0))
FROM (SELECT toLowCardinality(if(number % 10 = 0, toUInt64(18446744073709551615), toUInt64(1))) AS delta FROM numbers(100000));

-- A NULL argument returns NULL without the arity check, so a call with one argument is valid.
SELECT sum(number > 0 AND (number IS DISTINCT FROM subtractHours(materialize(NULL)))) FROM numbers(3);

CREATE TABLE t_add_interval_mt (k UInt64, ds String) ENGINE = MergeTree ORDER BY k;
INSERT INTO t_add_interval_mt SELECT number, if(number % 10 = 0, 'x', '2020-01-01') FROM numbers(100000);

SELECT count() FROM t_add_interval_mt PREWHERE ds != 'x' AND addDays(ds, 1) > toDateTime('2000-01-01')
SETTINGS enable_multiple_prewhere_read_steps = 1;
SELECT count() FROM t_add_interval_mt PREWHERE ds != 'x' AND addDays(ds, 1) > toDateTime('2000-01-01')
SETTINGS enable_multiple_prewhere_read_steps = 0;

DROP TABLE t_add_interval_log;
DROP TABLE t_add_interval_mt;
