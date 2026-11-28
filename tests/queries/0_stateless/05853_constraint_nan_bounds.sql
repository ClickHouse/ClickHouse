-- every ordered comparison with a `NaN` is false, the constraint graph must not order a bound against it

SET convert_query_to_cnf = 1, optimize_substitute_columns = 0, optimize_append_index = 0;

DROP TABLE IF EXISTS t_nan_bound;

CREATE TABLE t_nan_bound (f Float64, CONSTRAINT c CHECK f = 2) ENGINE = Memory;
INSERT INTO t_nan_bound VALUES (2);

SELECT 'less', count() FROM t_nan_bound WHERE f < nan SETTINGS optimize_using_constraints = 0;
SELECT 'less', count() FROM t_nan_bound WHERE f < nan SETTINGS optimize_using_constraints = 1;
SELECT 'less or equals', count() FROM t_nan_bound WHERE f <= nan SETTINGS optimize_using_constraints = 0;
SELECT 'less or equals', count() FROM t_nan_bound WHERE f <= nan SETTINGS optimize_using_constraints = 1;
SELECT 'greater', count() FROM t_nan_bound WHERE f > nan SETTINGS optimize_using_constraints = 0;
SELECT 'greater', count() FROM t_nan_bound WHERE f > nan SETTINGS optimize_using_constraints = 1;
SELECT 'greater or equals', count() FROM t_nan_bound WHERE f >= nan SETTINGS optimize_using_constraints = 0;
SELECT 'greater or equals', count() FROM t_nan_bound WHERE f >= nan SETTINGS optimize_using_constraints = 1;
SELECT 'nan less', count() FROM t_nan_bound WHERE nan < f SETTINGS optimize_using_constraints = 0;
SELECT 'nan less', count() FROM t_nan_bound WHERE nan < f SETTINGS optimize_using_constraints = 1;
SELECT 'nan greater or equals', count() FROM t_nan_bound WHERE nan >= f SETTINGS optimize_using_constraints = 0;
SELECT 'nan greater or equals', count() FROM t_nan_bound WHERE nan >= f SETTINGS optimize_using_constraints = 1;
SELECT 'equals', count() FROM t_nan_bound WHERE f = nan SETTINGS optimize_using_constraints = 0;
SELECT 'equals', count() FROM t_nan_bound WHERE f = nan SETTINGS optimize_using_constraints = 1;
SELECT 'not equals', count() FROM t_nan_bound WHERE f != nan SETTINGS optimize_using_constraints = 0;
SELECT 'not equals', count() FROM t_nan_bound WHERE f != nan SETTINGS optimize_using_constraints = 1;

DROP TABLE t_nan_bound;
