-- A set-operation branch whose column type differs from the common type only by a Decimal precision or a time zone
-- must be converted to the common type.

SET use_variant_as_common_type = 1, output_format_decimal_trailing_zeros = 0;

DROP TABLE IF EXISTS t_int;
DROP TABLE IF EXISTS t_float;
DROP TABLE IF EXISTS t_utc;
DROP TABLE IF EXISTS t_tokyo;

CREATE TABLE t_int (a Int64, b Decimal(35, 7)) ENGINE = Memory;
INSERT INTO t_int VALUES (1, 1);
CREATE TABLE t_float (a Float64, b UInt32) ENGINE = Memory;
INSERT INTO t_float VALUES (1.5, 2);

SELECT 'UNION ALL, Variant result';
SELECT a + b AS r, variantType(r) FROM (SELECT * FROM t_int UNION ALL SELECT * FROM t_float) ORDER BY toString(r);

SELECT 'EXCEPT ALL, Variant result';
SELECT a + b AS r, variantType(r) FROM (SELECT * FROM t_int EXCEPT ALL SELECT * FROM t_float) ORDER BY toString(r);

SELECT 'time zone';
SELECT toHour(x) FROM (SELECT materialize(toDateTime(1767225600, 'UTC')) AS x UNION ALL SELECT materialize(toDateTime(1767225600, 'Asia/Tokyo'))) ORDER BY 1;

SELECT 'time zone, DateTime64';
SELECT toHour(x) FROM (SELECT materialize(toDateTime64(1767225600, 3, 'UTC')) AS x UNION ALL SELECT materialize(toDateTime64(1767225600, 3, 'Asia/Tokyo'))) ORDER BY 1;

SELECT 'result type';
SELECT x FROM (SELECT materialize(1.5::Decimal(5, 2)) AS x UNION ALL SELECT materialize(7::UInt8)) ORDER BY x FORMAT TSVWithNamesAndTypes;

SELECT 'nested in a tuple with a Variant';
SELECT t FROM (SELECT materialize(tuple(1::UInt8::Variant(UInt8, String), toDecimal128(1, 7)::Decimal(35, 7))) AS t UNION ALL SELECT materialize(tuple('a'::Variant(UInt8, String), 2::UInt32))) ORDER BY toString(t) FORMAT TSVWithNamesAndTypes;

SELECT 'Geometry in a tuple';
SELECT t FROM (SELECT materialize(tuple((0., 0.)::Point::Geometry, toDecimal128(1, 7)::Decimal(35, 7))) AS t UNION ALL SELECT materialize(tuple((1., 1.)::Point::Geometry, 2::UInt32))) ORDER BY toString(t) FORMAT TSVWithNamesAndTypes;

SELECT 'JSON typed path';
SELECT x, JSONAllPathsWithTypes(x) AS paths FROM (SELECT materialize('{"p":1.5}'::JSON(p Decimal(5, 2))) AS x UNION ALL SELECT materialize('{"p":7}'::JSON(p UInt8))) ORDER BY toString(x) FORMAT TSVWithNamesAndTypes;

SELECT 'JSON typed path, time zone';
SELECT toString(x) FROM (SELECT materialize('{"t":1767225600}'::JSON(t DateTime('UTC'))) AS x UNION ALL SELECT materialize('{"t":1767225600}'::JSON(t DateTime('Asia/Tokyo')))) ORDER BY 1;

SELECT 'same type name, different captured time zone';
SET session_timezone = 'UTC';
CREATE TABLE t_utc (ts DateTime) ENGINE = Memory;
INSERT INTO t_utc VALUES (1767225600);
SET session_timezone = 'Asia/Tokyo';
CREATE TABLE t_tokyo (ts DateTime) ENGINE = Memory;
INSERT INTO t_tokyo VALUES (1767225600);
SET session_timezone = 'UTC';
SELECT toHour(ts) FROM (SELECT ts FROM t_utc UNION ALL SELECT ts FROM t_tokyo) ORDER BY 1;

DROP TABLE t_int;
DROP TABLE t_float;
DROP TABLE t_utc;
DROP TABLE t_tokyo;
