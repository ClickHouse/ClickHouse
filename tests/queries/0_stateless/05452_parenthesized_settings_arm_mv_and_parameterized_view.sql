-- A parenthesized operand of a set operation with a `SETTINGS` clause of its own keeps its wrapper in the AST.

-- The structure of a parameterized view in such an operand is inferred with the operand's settings.
DROP TABLE IF EXISTS pv;
CREATE VIEW pv AS SELECT sum(number) AS x FROM numbers({n:UInt64});

SELECT CAST(99, 'Nullable(UInt64)') AS x WHERE 0
UNION ALL
(
    SELECT x FROM pv(n = 0)
    SETTINGS aggregate_functions_null_for_empty = 1
);

DROP TABLE pv;

-- Such operands are still simple `SELECT` queries in a materialized view.
DROP TABLE IF EXISTS mv1;
DROP TABLE IF EXISTS mv2;
DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS src2;
CREATE TABLE src (id UInt64) ENGINE = Memory;
CREATE TABLE src2 (id UInt64) ENGINE = Memory;

CREATE MATERIALIZED VIEW mv1 ENGINE = Memory AS
SELECT id FROM src
UNION ALL
(SELECT id FROM src2 SETTINGS max_threads = 1);

CREATE MATERIALIZED VIEW mv2 ENGINE = Memory AS
(SELECT id FROM src SETTINGS max_threads = 1)
UNION ALL
SELECT id FROM src2;

INSERT INTO src VALUES (1);
SELECT id FROM mv1 ORDER BY id;
SELECT id FROM mv2 ORDER BY id;

ALTER TABLE mv1 MODIFY QUERY
(SELECT id + 100 AS id FROM src SETTINGS max_threads = 1)
UNION ALL
(SELECT id FROM src2 SETTINGS max_threads = 2);

INSERT INTO src VALUES (2);
SELECT id FROM mv1 ORDER BY id;

DROP TABLE mv1;
DROP TABLE mv2;
DROP TABLE src;
DROP TABLE src2;
