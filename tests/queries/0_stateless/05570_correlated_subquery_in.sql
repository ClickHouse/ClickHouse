-- A correlated subquery on the right side of IN / NOT IN is rewritten into EXISTS and returns what the regular IN
-- returns for the same values.

SET allow_correlated_subqueries = 1;

SELECT 'issue';
SELECT r.id
FROM values('id Int32, owner Int32', (1, 10), (2, 20)) r
WHERE r.id IN (SELECT p.id FROM values('id Int32, owner Int32', (1, 10), (2, 20)) p WHERE p.owner = r.owner)
ORDER BY 1;
SELECT r.id
FROM values('id Int32, owner Int32', (1, 10), (2, 20)) r
WHERE r.id IN (SELECT p.id FROM values('id Int32, owner Int32', (1, 10), (2, 20)) p WHERE p.owner = r.owner)
ORDER BY 1
SETTINGS join_use_nulls = 1, group_by_use_nulls = 1, prefer_column_name_to_alias = 1, union_default_mode = 'DISTINCT', cast_keep_nullable = 1;
SELECT r.id
FROM values('id Int32, owner Int32', (1, 10), (2, 20)) r
WHERE r.id IN (SELECT p.id FROM values('id Int32, owner Int32', (1, 10), (2, 20)) p WHERE p.owner = r.owner)
ORDER BY 1
SETTINGS rewrite_in_to_join = 1;

DROP TABLE IF EXISTS r_mt;
DROP TABLE IF EXISTS p_mt;
CREATE TABLE r_mt (id Int32, owner Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE p_mt (id Int32, owner Int32) ENGINE = MergeTree ORDER BY owner;
INSERT INTO r_mt VALUES (1, 10), (2, 20), (3, 30), (4, 10);
INSERT INTO p_mt VALUES (1, 10), (2, 20), (3, 31), (4, 11);
SELECT r.id FROM r_mt r WHERE r.id IN (SELECT p.id FROM p_mt p WHERE p.owner = r.owner) ORDER BY 1;
SELECT r.id FROM r_mt r WHERE r.id NOT IN (SELECT p.id FROM p_mt p WHERE p.owner = r.owner) ORDER BY 1;
DROP TABLE r_mt;
DROP TABLE p_mt;

-- The result and its type must be the ones of the regular IN, which builds a set for each key separately.
SELECT 'parity';
DROP TABLE IF EXISTS o;
DROP TABLE IF EXISTS i;

CREATE TABLE o (k UInt8, x Int32) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Int32) ENGINE = Memory;
INSERT INTO o VALUES (1, 1), (1, 2), (2, 2), (2, 3), (3, 1);
INSERT INTO i VALUES (1, 1), (2, 2), (2, 3);
SELECT 'Int32 / Int32', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x Nullable(Int32)) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Int64) ENGINE = Memory;
INSERT INTO o VALUES (1, 1), (1, NULL), (2, 2), (3, NULL);
INSERT INTO i VALUES (1, 1), (2, 3);
SELECT 'Nullable(Int32) / Int64', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x Int16) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Nullable(Int32)) ENGINE = Memory;
INSERT INTO o VALUES (1, 1), (1, 3), (2, 3), (3, 1);
INSERT INTO i VALUES (1, 1), (1, NULL), (2, NULL);
SELECT 'Int16 / Nullable(Int32)', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x String) ENGINE = Memory;
CREATE TABLE i (k UInt8, e UInt8) ENGINE = Memory;
INSERT INTO o VALUES (1, '1'), (1, 'x'), (1, '256'), (2, '2'), (3, '1');
INSERT INTO i VALUES (1, 1), (1, 0), (2, 2);
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x UInt8) ENGINE = Memory;
CREATE TABLE i (k UInt8, e String) ENGINE = Memory;
INSERT INTO o VALUES (1, 1), (1, 2), (2, 7), (3, 1);
INSERT INTO i VALUES (1, '1'), (1, 'x'), (2, '07');
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x Decimal32(2)) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Int32) ENGINE = Memory;
INSERT INTO o VALUES (1, 1.50), (1, 2), (2, 1), (3, 1);
INSERT INTO i VALUES (1, 1), (2, 1);
SELECT 'Decimal32(2) / Int32', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x UInt8) ENGINE = Memory;
CREATE TABLE i (k UInt8, e Bool) ENGINE = Memory;
INSERT INTO o VALUES (1, 2), (1, 1), (1, 0), (2, 0), (3, 1);
INSERT INTO i VALUES (1, true), (2, false);
SELECT 'UInt8 / Bool', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x LowCardinality(Nullable(String))) ENGINE = Memory;
CREATE TABLE i (k UInt8, e String) ENGINE = Memory;
INSERT INTO o VALUES (1, 'a'), (1, NULL), (1, 'b'), (2, 'b'), (3, 'a');
INSERT INTO i VALUES (1, 'a'), (2, 'b');
SELECT 'LowCardinality(Nullable(String)) / String', k, x,
    x IN (SELECT e FROM i WHERE i.k = o.k) AS got,
    x NOT IN (SELECT e FROM i WHERE i.k = o.k) AS got_not,
    isNotDistinctFrom(got, multiIf(k = 1, x IN (SELECT e FROM i WHERE k = 1), k = 2, x IN (SELECT e FROM i WHERE k = 2), x IN (SELECT e FROM i WHERE k = 3))),
    isNotDistinctFrom(got_not, multiIf(k = 1, x NOT IN (SELECT e FROM i WHERE k = 1), k = 2, x NOT IN (SELECT e FROM i WHERE k = 2), x NOT IN (SELECT e FROM i WHERE k = 3))),
    toTypeName(got) = toTypeName(x IN (SELECT e FROM i WHERE k = 1)) AND toTypeName(got_not) = toTypeName(x NOT IN (SELECT e FROM i WHERE k = 1))
FROM o ORDER BY k, x;
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x DateTime64(3, 'UTC')) ENGINE = Memory;
CREATE TABLE i (k UInt8, e DateTime('UTC')) ENGINE = Memory;
INSERT INTO o VALUES (1, '2020-01-01 10:00:00.000'), (1, '2020-01-01 10:00:00.500'), (2, '2020-01-01 10:00:01.000'), (3, '2020-01-01 10:00:00.000');
INSERT INTO i VALUES (1, '2020-01-01 10:00:00'), (2, '2020-01-01 10:00:01');
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
DROP TABLE o;
DROP TABLE i;

CREATE TABLE o (k UInt8, x Date) ENGINE = Memory;
CREATE TABLE i (k UInt8, e String) ENGINE = Memory;
INSERT INTO o VALUES (1, '2020-01-01'), (1, '2020-01-02'), (2, '2020-01-02'), (3, '2020-01-01');
INSERT INTO i VALUES (1, '2020-01-01'), (1, 'not a date'), (2, '2020-01-02');
SELECT k FROM o WHERE x IN (SELECT e FROM i WHERE i.k = o.k); -- { serverError NOT_IMPLEMENTED }
DROP TABLE o;
DROP TABLE i;
