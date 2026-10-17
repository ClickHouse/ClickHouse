-- https://github.com/ClickHouse/ClickHouse/issues/125622, https://github.com/ClickHouse/ClickHouse/issues/71041
-- With insert_null_as_default, only source NULLs become defaults: a non-NULL value the column cannot hold fails the INSERT.

SET insert_null_as_default = 1;
SET enable_nullable_tuple_type = 1;

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;
DROP TABLE IF EXISTS dst_i8;
DROP TABLE IF EXISTS src_nulls;
DROP TABLE IF EXISTS dst_defaults;
DROP TABLE IF EXISTS null_payload;
DROP TABLE IF EXISTS types;
DROP TABLE IF EXISTS ip4;
DROP TABLE IF EXISTS controls;

CREATE TABLE src (id Int32, v Nullable(String)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst (id Int32, v Int32) ENGINE = MergeTree ORDER BY id;
INSERT INTO src VALUES (1, '5'), (2, '5a');
INSERT INTO dst SELECT id, v FROM src; -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO dst SELECT 3, toLowCardinality(toNullable(materialize('5a'))); -- { serverError CANNOT_PARSE_TEXT }
SELECT countIf(v = 0) FROM dst;

CREATE TABLE dst_i8 (x Int8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO dst_i8 SELECT 'meow'::Nullable(String); -- { serverError CANNOT_PARSE_TEXT }

CREATE TABLE src_nulls (id Int32, v Nullable(String)) ENGINE = MergeTree ORDER BY id;
CREATE TABLE dst_defaults (id Int32, i Int32, d Int32 DEFAULT 42) ENGINE = MergeTree ORDER BY id;
INSERT INTO src_nulls VALUES (1, '5'), (2, NULL), (3, '-7');
INSERT INTO dst_defaults SELECT id, v, v FROM src_nulls;
SELECT * FROM dst_defaults ORDER BY id;

CREATE TABLE null_payload (i Int32, fs FixedString(2), t Tuple(Int32)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO null_payload SELECT nullIf(materialize('5a'), '5a'), nullIf(materialize('abcdef'), 'abcdef'), nullIf(tuple(materialize('5a')), tuple('5a'));
SELECT * FROM null_payload;

CREATE TABLE types (f Float64, d Decimal(9, 2), fs FixedString(2), u UUID, ip IPv4, dt Date, dtt DateTime, lfs LowCardinality(FixedString(2)))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO types (f) SELECT toNullable(materialize('abc')); -- { serverError CANNOT_PARSE_NUMBER }
INSERT INTO types (d) SELECT toNullable(materialize('5a')); -- { serverError CANNOT_PARSE_TEXT }
INSERT INTO types (fs) SELECT toNullable(materialize('abcdef')); -- { serverError TOO_LARGE_STRING_SIZE }
INSERT INTO types (u) SELECT toNullable(materialize('5a')); -- { serverError CANNOT_PARSE_UUID }
INSERT INTO types (ip) SELECT toNullable(materialize('5a')); -- { serverError CANNOT_PARSE_IPV4 }
INSERT INTO types (dt) SELECT toNullable(materialize('5a')); -- { serverError CANNOT_PARSE_DATE }
INSERT INTO types (dtt) SELECT toNullable(materialize('5a')); -- { serverError CANNOT_PARSE_DATETIME }
INSERT INTO types (lfs) SELECT toNullable(materialize('abcdef')); -- { serverError TOO_LARGE_STRING_SIZE }
SELECT count() FROM types;

CREATE TABLE ip4 (ip IPv4) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO ip4 SELECT toNullable(toIPv6('2001:db8::1')); -- { serverError CANNOT_CONVERT_TYPE }
INSERT INTO ip4 SELECT nullIf(toIPv6('2001:db8::1'), toIPv6('2001:db8::1'));
SELECT * FROM ip4;

-- A Nullable column keeps the CAST rule (unparsable text is NULL); a valid value converts as with insert_null_as_default = 0.
CREATE TABLE controls (n Nullable(Int32), u UInt8) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO controls SELECT toNullable(materialize('5a')), toNullable(materialize('300'));
SELECT * FROM controls;

DROP TABLE src;
DROP TABLE dst;
DROP TABLE dst_i8;
DROP TABLE src_nulls;
DROP TABLE dst_defaults;
DROP TABLE null_payload;
DROP TABLE types;
DROP TABLE ip4;
DROP TABLE controls;
