-- A PREWHERE split into several read steps must not let a value of a row rejected by an earlier condition reach
-- anything that throws on it: a column filled from its DEFAULT expression in a part that lacks it, a column converted
-- after a pending ALTER, or a later condition that can throw. The first condition keeps 90% of the rows, and every
-- rejected row holds a value that throws.

SET enable_multiple_prewhere_read_steps = 1, query_plan_merge_filters = 1;

DROP TABLE IF EXISTS t_prewhere_default;
CREATE TABLE t_prewhere_default (k UInt64, g UInt8, s String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO t_prewhere_default SELECT number, number % 10 != 0, if(number % 10 = 0, 'x', toString(number)) FROM numbers(100000);
-- The part has no `n`, so it is computed from `s` when the part is read.
ALTER TABLE t_prewhere_default ADD COLUMN n UInt64 DEFAULT toUInt64(s);

SELECT 'default, column of a later condition';
SELECT count() FROM t_prewhere_default PREWHERE g = 1 AND n > 5;
SELECT 'default, column read after PREWHERE';
SELECT sum(n) FROM t_prewhere_default PREWHERE g = 1 AND k > 5;

DROP TABLE t_prewhere_default;

DROP TABLE IF EXISTS t_prewhere_conversion;
CREATE TABLE t_prewhere_conversion (k UInt64, g UInt8, v String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
SYSTEM STOP MERGES t_prewhere_conversion;
INSERT INTO t_prewhere_conversion SELECT number, number % 10 != 0, if(number % 10 = 0, 'x', toString(number)) FROM numbers(100000);
-- The mutation stays pending, so `v` is converted to UInt64 when the part is read.
ALTER TABLE t_prewhere_conversion MODIFY COLUMN v UInt64 SETTINGS alter_sync = 0;

SELECT 'conversion, column of a later condition';
SELECT count() FROM t_prewhere_conversion PREWHERE g = 1 AND v > 5;

DROP TABLE t_prewhere_conversion;

DROP TABLE IF EXISTS t_prewhere_guard;
CREATE TABLE t_prewhere_guard (k UInt64, g UInt8, ds String) ENGINE = MergeTree ORDER BY k SETTINGS index_granularity = 8192;
INSERT INTO t_prewhere_guard SELECT number, number % 10 != 0, if(number % 10 = 0, 'x', '2020-01-01') FROM numbers(100000);

-- `addDays` parses `ds` and throws on 'x'.
SELECT 'later condition that can throw';
SELECT count() FROM t_prewhere_guard PREWHERE g = 1 AND addDays(ds, 1) > toDateTime('2000-01-01');

DROP TABLE t_prewhere_guard;
