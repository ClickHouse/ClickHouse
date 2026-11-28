-- Rewriting references to subcolumns of missing columns in default expressions
-- must not capture identifiers bound to lambda parameters or expression aliases.

DROP TABLE IF EXISTS t_lambda;
CREATE TABLE t_lambda
(
    id UInt8,
    `x.n` Tuple(v UInt8) DEFAULT tuple(99),
    a Array(Tuple(n Tuple(v UInt8))) DEFAULT [tuple(tuple(7))],
    d Array(UInt8) DEFAULT arrayMap(x -> x.n.v, a)
) ENGINE = Memory;
INSERT INTO t_lambda (id) VALUES (1);
INSERT INTO t_lambda (id, `x.n`) VALUES (2, tuple(99));
SELECT id, d FROM t_lambda ORDER BY id;
DROP TABLE t_lambda;

-- A dotted lambda parameter.
DROP TABLE IF EXISTS t_lambda_dotted;
CREATE TABLE t_lambda_dotted
(
    id UInt8,
    x Tuple(n UInt8) DEFAULT tuple(99),
    d Array(UInt8) DEFAULT arrayMap(`x.n` -> `x.n` + 1, [1, 2])
) ENGINE = Memory;
INSERT INTO t_lambda_dotted (id) VALUES (1);
INSERT INTO t_lambda_dotted (id, x) VALUES (2, tuple(99));
SELECT id, d FROM t_lambda_dotted ORDER BY id;
DROP TABLE t_lambda_dotted;

-- An expression alias.
DROP TABLE IF EXISTS t_alias;
CREATE TABLE t_alias
(
    id UInt8,
    x Tuple(a UInt8) DEFAULT tuple(99),
    d UInt8 DEFAULT (1 AS `x.a`) + `x.a`
) ENGINE = Memory;
INSERT INTO t_alias (id) VALUES (1);
INSERT INTO t_alias (id, x) VALUES (2, tuple(99));
SELECT id, d FROM t_alias ORDER BY id;
DROP TABLE t_alias;

-- A reference to a subcolumn of a missing column outside the lambda is still rewritten.
DROP TABLE IF EXISTS t_mixed;
CREATE TABLE t_mixed
(
    id UInt8,
    `x.n` Tuple(v UInt8) DEFAULT tuple(5),
    a Array(Tuple(n Tuple(v UInt8))) DEFAULT [tuple(tuple(7))],
    d Array(UInt8) DEFAULT arrayMap(x -> x.n.v + x.n.v, a),
    e UInt8 DEFAULT x.n.v
) ENGINE = Memory;
INSERT INTO t_mixed (id) VALUES (1);
INSERT INTO t_mixed (id, `x.n`) VALUES (2, tuple(6));
SELECT id, d, e FROM t_mixed ORDER BY id;
DROP TABLE t_mixed;
