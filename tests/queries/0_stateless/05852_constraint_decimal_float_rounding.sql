-- a comparison of a decimal with a float rounds the decimal to `Float64`, the constraint graph must not use it as exact

SET convert_query_to_cnf = 1;

DROP TABLE IF EXISTS t_decimal_float_constant;
DROP TABLE IF EXISTS t_decimal_float_enum;
DROP TABLE IF EXISTS t_decimal_strict_bound;
DROP TABLE IF EXISTS t_decimal_float_column;
DROP TABLE IF EXISTS t_decimal_float_projection;
DROP TABLE IF EXISTS t_decimal_exact_chain;
DROP TABLE IF EXISTS t_decimal_chain_hint;
DROP TABLE IF EXISTS t_decimal_chain_column;
DROP TABLE IF EXISTS t_datetime64_float;

-- the float bound of the constraint is not the exact value of `d`
CREATE TABLE t_decimal_float_constant (d Decimal128(18), CONSTRAINT c CHECK d = toFloat64(1)) ENGINE = Memory;
INSERT INTO t_decimal_float_constant VALUES ('1.000000000000000001');
SELECT 'float constant', count() FROM t_decimal_float_constant WHERE d > toDecimal128(1, 18) SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 0;
SELECT 'float constant', count() FROM t_decimal_float_constant WHERE d > toDecimal128(1, 18) SETTINGS optimize_using_constraints = 1, optimize_substitute_columns = 0, optimize_append_index = 0;
SELECT 'float constant', count() FROM t_decimal_float_constant WHERE d = toDecimal128(1, 18) SETTINGS optimize_using_constraints = 1, optimize_substitute_columns = 0, optimize_append_index = 0;

-- the same through an `Enum` and a float column
CREATE TABLE t_decimal_float_enum (e Enum8('a' = 1), f Float64, d Decimal128(18), CONSTRAINT c CHECK e = f AND d = f AND f = 1) ENGINE = Memory;
INSERT INTO t_decimal_float_enum VALUES ('a', 1, '1.000000000000000001');
SELECT 'enum bridge', count() FROM t_decimal_float_enum WHERE d > toDecimal128(1, 18) SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 0;
SELECT 'enum bridge', count() FROM t_decimal_float_enum WHERE d > toDecimal128(1, 18) SETTINGS optimize_using_constraints = 1, optimize_substitute_columns = 0, optimize_append_index = 0;

-- an exact strict bound does not order the rounded decimal against a float constant
CREATE TABLE t_decimal_strict_bound (d Decimal128(18), CONSTRAINT c CHECK d > toDecimal128(1, 18)) ENGINE = Memory;
INSERT INTO t_decimal_strict_bound VALUES ('1.000000000000000001');
SELECT 'strict bound', count() FROM t_decimal_strict_bound WHERE d > 1.0 SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 0;
SELECT 'strict bound', count() FROM t_decimal_strict_bound WHERE d > 1.0 SETTINGS optimize_using_constraints = 1, optimize_substitute_columns = 0, optimize_append_index = 0;

-- `CHECK d = f` gives no index hint on `d`
CREATE TABLE t_decimal_float_column (d Decimal128(18), f Float64, CONSTRAINT c CHECK d = f) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_decimal_float_column VALUES ('1.000000000000000001', 1);
SELECT 'index hint', count() FROM t_decimal_float_column WHERE f <= toDecimal128(1, 18) SETTINGS optimize_using_constraints = 0, optimize_append_index = 0, optimize_substitute_columns = 0;
SELECT 'index hint', count() FROM t_decimal_float_column WHERE f <= toDecimal128(1, 18) SETTINGS optimize_using_constraints = 1, optimize_append_index = 1, optimize_substitute_columns = 0;

-- `CHECK d = f` gives no substitution of `d`
CREATE TABLE t_decimal_float_projection (d Decimal128(18), f Float64, CONSTRAINT c CHECK d = f) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_decimal_float_projection VALUES ('1.000000000000000001', 1);
SELECT 'projection', d FROM t_decimal_float_projection WHERE f > 0 SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 0;
SELECT 'projection', d FROM t_decimal_float_projection WHERE f > 0 SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 1;

-- `d`, `n` and `f` are exactly equal, but the float bound and the decimal constant compare with rounding
CREATE TABLE t_decimal_exact_chain (d Decimal128(18), n Int64, f Float64, CONSTRAINT c CHECK d = n AND n = f AND f = toFloat64(1)) ENGINE = Memory;
INSERT INTO t_decimal_exact_chain VALUES (1, 1, 1);
SELECT 'exact chain', count() FROM t_decimal_exact_chain WHERE d > toDecimal128('0.999999999999999999', 18) SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 0;
SELECT 'exact chain', count() FROM t_decimal_exact_chain WHERE d > toDecimal128('0.999999999999999999', 18) SETTINGS optimize_using_constraints = 1, optimize_substitute_columns = 0, optimize_append_index = 0;

-- the query compares `f` with a rounded decimal constant, a hint on `d` compares exactly
CREATE TABLE t_decimal_chain_hint (d Decimal128(18), n Int64, f Float64, CONSTRAINT c CHECK d = n AND n = f) ENGINE = MergeTree ORDER BY d;
INSERT INTO t_decimal_chain_hint VALUES (1, 1, 1);
SELECT 'exact chain hint', count() FROM t_decimal_chain_hint WHERE f <= toDecimal128('0.999999999999999999', 18) SETTINGS optimize_using_constraints = 0, optimize_append_index = 0, optimize_substitute_columns = 0;
SELECT 'exact chain hint', count() FROM t_decimal_chain_hint WHERE f <= toDecimal128('0.999999999999999999', 18) SETTINGS optimize_using_constraints = 1, optimize_append_index = 1, optimize_substitute_columns = 0;

-- `d < n AND n = f` is exact, but the query `d < f` rounds `d`
CREATE TABLE t_decimal_chain_column (d Decimal128(18), n Int64, f Float64, CONSTRAINT c CHECK d < n AND n = f) ENGINE = Memory;
INSERT INTO t_decimal_chain_column VALUES ('0.999999999999999999', 1, 1);
SELECT 'chain column', count() FROM t_decimal_chain_column WHERE d < f SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 0;
SELECT 'chain column', count() FROM t_decimal_chain_column WHERE d < f SETTINGS optimize_using_constraints = 1, optimize_substitute_columns = 0, optimize_append_index = 0;

-- a `DateTime64` compares with a float the same way
CREATE TABLE t_datetime64_float (d DateTime64(9, 'UTC'), CONSTRAINT c CHECK d = toFloat64(1577836800)) ENGINE = Memory;
INSERT INTO t_datetime64_float VALUES ('2020-01-01 00:00:00.000000001');
SELECT 'datetime64', count() FROM t_datetime64_float WHERE d > toDateTime64('2020-01-01 00:00:00', 9, 'UTC') SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 0;
SELECT 'datetime64', count() FROM t_datetime64_float WHERE d > toDateTime64('2020-01-01 00:00:00', 9, 'UTC') SETTINGS optimize_using_constraints = 1, optimize_substitute_columns = 0, optimize_append_index = 0;

DROP TABLE t_decimal_float_constant;
DROP TABLE t_decimal_float_enum;
DROP TABLE t_decimal_strict_bound;
DROP TABLE t_decimal_float_column;
DROP TABLE t_decimal_float_projection;
DROP TABLE t_decimal_exact_chain;
DROP TABLE t_decimal_chain_hint;
DROP TABLE t_decimal_chain_column;
DROP TABLE t_datetime64_float;
