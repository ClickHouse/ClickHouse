-- In a materialized view, the query of `body(...)` of the `url` table function reads the inserted block
-- instead of the source table, as the other subqueries of the view do.
-- The request is sent to the HTTP interface of the server itself, which executes the body as a query.

CREATE TABLE src_null (x UInt64) ENGINE = Null;
CREATE TABLE src_memory (x UInt64) ENGINE = Memory;
CREATE TABLE dst (engine String, analyzer UInt8, x UInt64) ENGINE = Memory;

-- `Memory` already contains a row, which must not be seen by the body.
INSERT INTO src_memory VALUES (100);

CREATE MATERIALIZED VIEW mv_null_analyzer TO dst AS
SELECT 'Null' AS engine, 1 AS analyzer, u.x AS x
FROM src_null
CROSS JOIN url('http://127.0.0.1:8123/', TSV, 'x UInt64',
    body(SELECT concat('SELECT ', toString(sum(x)), ' FORMAT TSV') FROM src_null FORMAT TSVRaw)) AS u
SETTINGS enable_analyzer = 1;

CREATE MATERIALIZED VIEW mv_null_old TO dst AS
SELECT 'Null' AS engine, 0 AS analyzer, u.x AS x
FROM src_null
CROSS JOIN url('http://127.0.0.1:8123/', TSV, 'x UInt64',
    body(SELECT concat('SELECT ', toString(sum(x)), ' FORMAT TSV') FROM src_null FORMAT TSVRaw)) AS u
SETTINGS enable_analyzer = 0;

CREATE MATERIALIZED VIEW mv_memory_analyzer TO dst AS
SELECT 'Memory' AS engine, 1 AS analyzer, u.x AS x
FROM src_memory
CROSS JOIN url('http://127.0.0.1:8123/', TSV, 'x UInt64',
    body(SELECT concat('SELECT ', toString(sum(x)), ' FORMAT TSV') FROM src_memory FORMAT TSVRaw)) AS u
SETTINGS enable_analyzer = 1;

CREATE MATERIALIZED VIEW mv_memory_old TO dst AS
SELECT 'Memory' AS engine, 0 AS analyzer, u.x AS x
FROM src_memory
CROSS JOIN url('http://127.0.0.1:8123/', TSV, 'x UInt64',
    body(SELECT concat('SELECT ', toString(sum(x)), ' FORMAT TSV') FROM src_memory FORMAT TSVRaw)) AS u
SETTINGS enable_analyzer = 0;

INSERT INTO src_null VALUES (2), (3);
INSERT INTO src_memory VALUES (2), (3);

SELECT engine, analyzer, x, count() FROM dst GROUP BY ALL ORDER BY ALL;

DROP TABLE mv_null_analyzer;
DROP TABLE mv_null_old;
DROP TABLE mv_memory_analyzer;
DROP TABLE mv_memory_old;
DROP TABLE src_null;
DROP TABLE src_memory;
DROP TABLE dst;
