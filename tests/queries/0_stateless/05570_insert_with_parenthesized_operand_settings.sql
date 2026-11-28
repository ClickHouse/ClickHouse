-- A `WITH` clause written before `INSERT` must reach a parenthesized operand with its own `SETTINGS` clause.

DROP TABLE IF EXISTS src;
DROP TABLE IF EXISTS dst;

CREATE TABLE src (x UInt8) ENGINE = Memory;
CREATE TABLE dst (arm UInt8, x UInt8) ENGINE = Memory;
INSERT INTO src VALUES (99);

-- A CTE shadowing a table.
WITH src AS (SELECT toUInt8(1) AS x)
INSERT INTO dst
(SELECT 1 AS arm, x FROM src SETTINGS max_threads = 1)
UNION ALL
SELECT 2 AS arm, x FROM src;

SELECT * FROM dst ORDER BY arm;
TRUNCATE TABLE dst;

-- A scalar alias.
WITH toUInt8(7) AS y
INSERT INTO dst
(SELECT 1 AS arm, y AS x SETTINGS max_threads = 1)
UNION ALL
SELECT 2 AS arm, y AS x;

SELECT * FROM dst ORDER BY arm;
TRUNCATE TABLE dst;

-- A single parenthesized operand.
WITH toUInt8(5) AS y
INSERT INTO dst
(SELECT 1 AS arm, y AS x SETTINGS max_threads = 1);

SELECT * FROM dst ORDER BY arm;

DROP TABLE src;
DROP TABLE dst;
