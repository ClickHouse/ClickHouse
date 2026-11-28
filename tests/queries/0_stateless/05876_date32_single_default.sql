-- `Date32` has one default value, 1900-01-01: the value `defaultValueOfTypeName` returns is also the one inserted
-- for a default computed during a query.

SET join_use_nulls = 0;
SET group_by_use_nulls = 0;
SET input_format_null_as_default = 1;
SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_05876;
CREATE TABLE t_05876 (x UInt8, d Date32) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_05876 (x) VALUES (1);
INSERT INTO t_05876 VALUES (2, DEFAULT);
INSERT INTO t_05876 SELECT * FROM format(JSONEachRow, 'x UInt8, d Date32', '{"x":3}');
INSERT INTO t_05876 SELECT * FROM format(JSONEachRow, 'x UInt8, d Date32', '{"x":4,"d":null}');
INSERT INTO t_05876 SELECT * FROM format(CSV, 'x UInt8, d Date32', '5,\\N');
SELECT multiIf(x = 1, 'omitted column', x = 2, 'DEFAULT in VALUES', x = 3, 'JSONEachRow missing field',
    x = 4, 'JSONEachRow null', 'CSV \\N'), d FROM t_05876 ORDER BY x;
DROP TABLE t_05876;

SELECT 'defaultValueOfTypeName', defaultValueOfTypeName('Date32');
SELECT 'assumeNotNull of NULL', assumeNotNull(NULL::Nullable(Date32));
SELECT 'toDate32 of a NULL Variant', toDate32(NULL::Variant(String, UInt64));
SELECT 'toDate32 of a Variant, one type and NULL', groupArray(toDate32(v)) FROM (SELECT arrayJoin([NULL, '2000-01-01'::Variant(String, UInt64)]) AS v);
SELECT 'toDate32 of a Variant, two types and NULL', groupArray(toDate32(v)) FROM (SELECT arrayJoin([NULL, '2000-01-01'::Variant(String, UInt64), 10958::UInt64::Variant(String, UInt64)]) AS v);
SELECT 'toDate32 of a NULL Dynamic', toDate32(NULL::Dynamic);
SELECT 'CAST of a NULL Variant', CAST(NULL::Variant(String, UInt64) AS Date32);
SELECT 'CAST of a NULL Dynamic', CAST(NULL::Dynamic AS Date32);

DROP TABLE IF EXISTS l_05876;
DROP TABLE IF EXISTS r_05876;
CREATE TABLE l_05876 (k UInt32, a UInt8) ENGINE = MergeTree ORDER BY k;
CREATE TABLE r_05876 (k UInt32, a UInt8, d Date32) ENGINE = MergeTree ORDER BY k;
INSERT INTO l_05876 VALUES (1, 1);
INSERT INTO r_05876 VALUES (2, 2, '2000-01-01');
SELECT 'LEFT JOIN, hash', r.d FROM l_05876 AS l LEFT JOIN r_05876 AS r ON l.k = r.k SETTINGS join_algorithm = 'hash';
SELECT 'LEFT JOIN, full_sorting_merge', r.d FROM l_05876 AS l LEFT JOIN r_05876 AS r ON l.k = r.k
SETTINGS join_algorithm = 'full_sorting_merge';
SELECT 'LEFT JOIN, inequality', r.d FROM l_05876 AS l LEFT JOIN r_05876 AS r ON l.a > r.a AND l.k > r.k
SETTINGS join_algorithm = 'inequality';
DROP TABLE l_05876;
DROP TABLE r_05876;

SELECT 'ROLLUP', groupArray(d) FROM (SELECT d FROM (SELECT toDate32('2000-01-01') AS d) GROUP BY ROLLUP(d) ORDER BY d);
SELECT 'WITH FILL', groupArray(d) FROM (
    SELECT n, d FROM (SELECT 1 AS n, toDate32('2000-01-01') AS d UNION ALL SELECT 3, toDate32('2000-01-03')) ORDER BY n WITH FILL);
SELECT 'lagInFrame', lagInFrame(d) OVER (ORDER BY n ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
FROM (SELECT 1 AS n, toDate32('2000-01-01') AS d);
SELECT 'anyOrDefault of no rows', anyOrDefault(d) FROM (SELECT toDate32('2000-01-01') AS d) WHERE 0;
SELECT 'arrayResize', arrayResize([toDate32('2000-01-01')], 2);
SELECT 'JSONExtract of a missing key', JSONExtract('{}', 'a', 'Date32');
SELECT 'JSON typed path, missing', CAST('{"x":1}', 'JSON(a Date32)').a;
SELECT 'JSON typed path, null', CAST('{"a":null}', 'JSON(a Date32)').a;
SELECT 'JSON typed path, LowCardinality, null', CAST('{"a":null}', 'JSON(a LowCardinality(Date32))').a;

DROP TABLE IF EXISTS t_lc_05876;
CREATE TABLE t_lc_05876 (x UInt8, d LowCardinality(Date32), t Tuple(Date32)) ENGINE = MergeTree ORDER BY x;
INSERT INTO t_lc_05876 VALUES (1, DEFAULT, DEFAULT);
SELECT 'LowCardinality and Tuple, DEFAULT in VALUES', d, t FROM t_lc_05876;
DROP TABLE t_lc_05876;
SELECT 'LowCardinality, arrayResize', arrayResize([toLowCardinality(toDate32('2000-01-01'))], 2);
SELECT 'Tuple, arrayResize', arrayResize([tuple(toDate32('2000-01-01'))], 2);

SELECT 'firstNonDefault, 1970-01-01 is not the default', firstNonDefault(toDate32('1970-01-01'), toDate32('2000-01-01'));
SELECT 'firstNonDefault, 1900-01-01 is the default', firstNonDefault(toDate32('1900-01-01'), toDate32('2000-01-01'));
SELECT 'firstNonDefault, one argument', firstNonDefault(toDate32('1900-01-01'));
SELECT 'firstNonDefault, only defaults', firstNonDefault(toDate32('1900-01-01'), toDate32('1900-01-01'));
SELECT 'firstNonDefault, LowCardinality', firstNonDefault(toLowCardinality(toDate32('1900-01-01')), toLowCardinality(toDate32('2000-01-01')));
