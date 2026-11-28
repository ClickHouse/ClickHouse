-- arrays of different types compare by a common type or element by element, which treat a NaN differently, so equality is not transitive

SET convert_query_to_cnf = 1, optimize_substitute_columns = 0, optimize_append_index = 0;

DROP TABLE IF EXISTS t_array_mixed;
DROP TABLE IF EXISTS t_array_nested;
DROP TABLE IF EXISTS t_array_same;

CREATE TABLE t_array_mixed
(
    a Array(Tuple(Int64, Float64)),
    b Array(Tuple(Int128, Float64)),
    c Array(Tuple(UInt64, Float64)),
    CONSTRAINT ck CHECK a = b AND b = c
) ENGINE = Memory;
INSERT INTO t_array_mixed VALUES ([(0, nan)], [(0, nan)], [(0, nan)]);
SELECT 'array', a = b, b = c, a = c FROM t_array_mixed;
SELECT 'array a = c', count() FROM t_array_mixed WHERE a = c SETTINGS optimize_using_constraints = 0;
SELECT 'array a = c', count() FROM t_array_mixed WHERE a = c SETTINGS optimize_using_constraints = 1;

CREATE TABLE t_array_nested
(
    a Tuple(Array(Tuple(Int64, Float64))),
    b Tuple(Array(Tuple(Int128, Float64))),
    c Tuple(Array(Tuple(UInt64, Float64))),
    CONSTRAINT ck CHECK a = b AND b = c
) ENGINE = Memory;
INSERT INTO t_array_nested VALUES (([(0, nan)]), ([(0, nan)]), ([(0, nan)]));
SELECT 'nested', a = b, b = c, a = c FROM t_array_nested;
SELECT 'nested a = c', count() FROM t_array_nested WHERE a = c SETTINGS optimize_using_constraints = 0;
SELECT 'nested a = c', count() FROM t_array_nested WHERE a = c SETTINGS optimize_using_constraints = 1;

-- arrays of one type compare the same way, so the graph still joins them
CREATE TABLE t_array_same
(
    a Array(Tuple(Int64, Float64)),
    b Array(Tuple(Int64, Float64)),
    c Array(Tuple(Int64, Float64)),
    CONSTRAINT ck CHECK a = b AND b = c
) ENGINE = Memory;
INSERT INTO t_array_same VALUES ([(0, nan)], [(0, nan)], [(0, nan)]);
SELECT 'same a = c', count() FROM t_array_same WHERE a = c SETTINGS optimize_using_constraints = 0;
SELECT 'same a = c', count() FROM t_array_same WHERE a = c SETTINGS optimize_using_constraints = 1;
SELECT 'same folded', countIf(explain LIKE '%WHERE%') FROM (EXPLAIN QUERY TREE SELECT count() FROM t_array_same WHERE a = c SETTINGS optimize_using_constraints = 1);

DROP TABLE t_array_mixed;
DROP TABLE t_array_nested;
DROP TABLE t_array_same;
