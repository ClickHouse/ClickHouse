-- A `Date32` member of a composite that is converted to an out-of-range `DateTime` is not representable:
-- it must not wrap modulo 2^32 into an unrelated timestamp, as the scalar case does not.
SELECT toDateTime(4294880896, 'UTC') IN (toDate32('1969-12-31'));
SELECT [toDateTime(4294880896, 'UTC')] IN ([toDate32('1969-12-31')]);
SELECT tuple(toDateTime(4294880896, 'UTC')) IN (tuple(toDate32('1969-12-31')));
SELECT (1, [toDateTime(4294880896, 'UTC')]) IN ((1, [toDate32('1969-12-31')]));
SELECT [toDateTime(4294880896, 'UTC')] = [toDate32('1969-12-31')];

DROP TABLE IF EXISTS t_05448;
CREATE TABLE t_05448 (a Array(DateTime('UTC'))) ENGINE = Memory;
INSERT INTO t_05448 VALUES ([toDateTime(4294880896, 'UTC')]), ([toDateTime(0, 'UTC')]);
SELECT a FROM t_05448 WHERE a IN ([toDate32('1969-12-31')]) ORDER BY a;
SELECT a FROM t_05448 WHERE a NOT IN ([toDate32('1969-12-31')]) ORDER BY a;
DROP TABLE t_05448;

-- In range, the nested conversion still works.
SELECT [toDateTime('2020-01-01 00:00:00', 'UTC')] IN ([toDate32('2020-01-01')]);

SELECT * FROM values('x Map(UInt8, DateTime(\'UTC\'))', map(1, toDate32('1969-12-31'))); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT * FROM values('x Map(UInt8, DateTime(\'UTC\'))', map(1, toDate32('2020-01-01')));
