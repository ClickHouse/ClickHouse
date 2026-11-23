-- A correlated subquery on the right side of IN / NOT IN is rewritten into EXISTS and returns what the regular IN
-- returns for the same values.

SET allow_correlated_subqueries = 1;

DROP TABLE IF EXISTS o;
DROP TABLE IF EXISTS i;

CREATE TABLE o (k UInt8, x Float64) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Float64) ENGINE = Memory;
INSERT INTO o VALUES (1, nan), (1, -0.), (1, 0.), (1, 0.1), (2, 0.), (2, -0.), (3, 0.1);
INSERT INTO i VALUES (1, nan), (1, 0.1), (1, 0.), (2, -0.);
SELECT 'Float64 / Float64', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, toString(x);
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x Float32) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Float64) ENGINE = Memory;
INSERT INTO o VALUES (1, nan), (1, -0.), (1, 0.), (1, 0.1), (1, 0.5), (2, 0.), (3, 0.5);
INSERT INTO i VALUES (1, nan), (1, 0.1), (1, 0.5), (2, -0.);
SELECT 'Float32 / Float64', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, toString(x);
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x Float64) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Int32) ENGINE = Memory;
INSERT INTO o VALUES (1, nan), (1, -0.), (1, 1.5), (1, 1.), (2, 0.), (3, 1.);
INSERT INTO i VALUES (1, 0), (1, 1), (2, 0);
SELECT 'Float64 / Int32', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, toString(x);
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, a Nullable(Int32), b String) ENGINE = Memory;
CREATE TABLE i (k UInt8, c Int64, d String) ENGINE = Memory;
INSERT INTO o VALUES (1, 1, 'a'), (1, 1, 'b'), (1, NULL, 'a'), (2, 2, 'b'), (3, 1, 'a');
INSERT INTO i VALUES (1, 1, 'a'), (2, 2, 'b');
SELECT 'Tuple(Nullable(Int32), String) / Int64, String', k, a, b,
    (a, b) IN (SELECT c, d FROM i WHERE i.k = o.k) AS got,
    (a, b) NOT IN (SELECT c, d FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, (a, b) IN (SELECT c, d FROM i WHERE k = 1), k = 2, (a, b) IN (SELECT c, d FROM i WHERE k = 2), (a, b) IN (SELECT c, d FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, (a, b) NOT IN (SELECT c, d FROM i WHERE k = 1), k = 2, (a, b) NOT IN (SELECT c, d FROM i WHERE k = 2), (a, b) NOT IN (SELECT c, d FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName((a, b) IN (SELECT c, d FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName((a, b) NOT IN (SELECT c, d FROM i WHERE k = 1))
FROM o ORDER BY k, a, b;
DROP TABLE o;
DROP TABLE i;

-- An array subquery on the right of IN is the set of its elements.
CREATE TABLE o (k UInt8, x Int32) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Int32) ENGINE = Memory;
INSERT INTO o VALUES (1, 1), (1, 2), (2, 3), (3, 1);
INSERT INTO i VALUES (1, 1), (1, 5), (2, 3);
SELECT 'Int32 / Array(Int32)', k, x,
    x IN (SELECT groupArray(e) FROM i WHERE i.k = o.k GROUP BY i.k) AS got,
    x NOT IN (SELECT groupArray(e) FROM i WHERE i.k = o.k GROUP BY i.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT groupArray(e) FROM i WHERE k = 1 GROUP BY k), k = 2, x IN (SELECT groupArray(e) FROM i WHERE k = 2 GROUP BY k), x IN (SELECT groupArray(e) FROM i WHERE k = 3 GROUP BY k))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT groupArray(e) FROM i WHERE k = 1 GROUP BY k), k = 2, x NOT IN (SELECT groupArray(e) FROM i WHERE k = 2 GROUP BY k), x NOT IN (SELECT groupArray(e) FROM i WHERE k = 3 GROUP BY k)))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;
