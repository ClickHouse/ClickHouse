-- In a materialized view, the `body(...)` query of a `url` table function can read another `url` whose body
-- reads the inserted block. Every block of a single insert must get its own body on both levels: the storages
-- of the table functions are reused between the blocks, but neither of them may keep the view source of the
-- first block. The requests are sent to the HTTP interface of the server itself, which executes the body as a query.

CREATE TABLE src (x UInt64) ENGINE = Null;
CREATE TABLE dst (analyzer UInt8, x UInt64) ENGINE = Memory;

CREATE MATERIALIZED VIEW mv_analyzer TO dst AS
SELECT 1 AS analyzer, u.x AS x
FROM src
CROSS JOIN url('http://127.0.0.1:8123/', TSV, 'x UInt64',
    body(SELECT concat('SELECT ', toString(n), ' FORMAT TSV')
         FROM url('http://127.0.0.1:8123/', TSV, 'n UInt64',
             body(SELECT concat('SELECT ', toString(sum(x)), ' FORMAT TSV') FROM src FORMAT TSVRaw))
         FORMAT TSVRaw)) AS u
SETTINGS enable_analyzer = 1;

CREATE MATERIALIZED VIEW mv_old TO dst AS
SELECT 0 AS analyzer, u.x AS x
FROM src
CROSS JOIN url('http://127.0.0.1:8123/', TSV, 'x UInt64',
    body(SELECT concat('SELECT ', toString(n), ' FORMAT TSV')
         FROM url('http://127.0.0.1:8123/', TSV, 'n UInt64',
             body(SELECT concat('SELECT ', toString(sum(x)), ' FORMAT TSV') FROM src FORMAT TSVRaw))
         FORMAT TSVRaw)) AS u
SETTINGS enable_analyzer = 0;

-- Three blocks of two rows each: (1, 2), (3, 4), (5, 6).
INSERT INTO src SELECT number + 1 FROM numbers(6)
SETTINGS max_threads = 1, max_block_size = 2, max_insert_block_size = 2,
    min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0,
    min_insert_block_size_rows_for_materialized_views = 0,
    min_insert_block_size_bytes_for_materialized_views = 0;

SELECT analyzer, x, count() FROM dst GROUP BY ALL ORDER BY ALL;

DROP TABLE mv_analyzer;
DROP TABLE mv_old;
DROP TABLE src;
DROP TABLE dst;
