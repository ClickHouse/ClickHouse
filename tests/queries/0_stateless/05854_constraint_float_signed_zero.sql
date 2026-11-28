-- `-0 = 0` holds, but `1 / -0` is `-inf`: constraints must not replace a float term with an equal value

SET convert_query_to_cnf = 1, optimize_append_index = 0;

DROP TABLE IF EXISTS t_zero_const;
DROP TABLE IF EXISTS t_zero_enum;
DROP TABLE IF EXISTS t_zero_projection;
DROP TABLE IF EXISTS t_zero_columns;

CREATE TABLE t_zero_const (f Float64, CONSTRAINT c CHECK f = toFloat64(0)) ENGINE = Memory;
INSERT INTO t_zero_const SELECT toFloat64('-0');

SELECT 'constant filter', count() FROM t_zero_const WHERE 1 / f < 0 SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 0;
SELECT 'constant filter', count() FROM t_zero_const WHERE 1 / f < 0 SETTINGS optimize_using_constraints = 1, optimize_substitute_columns = 0;

CREATE TABLE t_zero_enum (e Enum8('zero' = 0, 'one' = 1), f Float64, CONSTRAINT c1 CHECK e = f, CONSTRAINT c2 CHECK e = 0) ENGINE = Memory;
INSERT INTO t_zero_enum SELECT 'zero', toFloat64('-0');

SELECT 'enum bridge filter', count() FROM t_zero_enum WHERE 1 / f < 0 SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 0;
SELECT 'enum bridge filter', count() FROM t_zero_enum WHERE 1 / f < 0 SETTINGS optimize_using_constraints = 1, optimize_substitute_columns = 0;

CREATE TABLE t_zero_projection (k UInt8, f Float64, CONSTRAINT c CHECK f = 0) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_zero_projection SELECT 1, toFloat64('-0');

SELECT 'constant projection', 1 / f FROM t_zero_projection WHERE f = 0 SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 0;
SELECT 'constant projection', 1 / f FROM t_zero_projection WHERE f = 0 SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 1;

-- `g` is cheaper to read than `f`, so the substitution picks it
CREATE TABLE t_zero_columns (k UInt8, f Float64 CODEC(NONE), g Float64, CONSTRAINT c CHECK f = g) ENGINE = MergeTree ORDER BY k SETTINGS min_bytes_for_wide_part = 0;
INSERT INTO t_zero_columns SELECT 1, toFloat64('-0'), toFloat64('0') FROM numbers(1000);

SELECT 'column projection', min(1 / f) FROM t_zero_columns WHERE f = g SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 0;
SELECT 'column projection', min(1 / f) FROM t_zero_columns WHERE f = g SETTINGS optimize_using_constraints = 0, optimize_substitute_columns = 1;

DROP TABLE t_zero_const;
DROP TABLE t_zero_enum;
DROP TABLE t_zero_projection;
DROP TABLE t_zero_columns;
