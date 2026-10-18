-- INSERT ... RETURNING runs a SELECT on top of the inserted blocks.

DROP TABLE IF EXISTS t_returning;
DROP TABLE IF EXISTS t_returning_mv_target;
DROP TABLE IF EXISTS t_returning_mv;

CREATE TABLE t_returning
(
    id UInt64,
    s String,
    d UInt64 DEFAULT id * 10,
    m UInt64 MATERIALIZED id + 1,
    a UInt64 ALIAS id * 2
)
ENGINE = MergeTree ORDER BY id;

SELECT 'VALUES, with defaults, materialized and alias columns';
INSERT INTO t_returning (id, s) RETURNING (SELECT id, s, d, m, a FROM t_returning ORDER BY id) VALUES (1, 'a'), (2, 'b');
SELECT *, m FROM t_returning ORDER BY id;

SELECT 'Only the inserted data is visible';
INSERT INTO t_returning (id, s) RETURNING (SELECT count(), sum(id) FROM t_returning) VALUES (3, 'c'), (4, 'd'), (5, 'e');
SELECT count() FROM t_returning;

SELECT 'Without the list of columns';
INSERT INTO t_returning RETURNING (SELECT * FROM t_returning) VALUES (6, 'f', 0);

SELECT 'INSERT SELECT with many blocks';
INSERT INTO t_returning (id, s) RETURNING (SELECT count(), min(id), max(id), sum(m) FROM t_returning)
SELECT number + 100, toString(number) FROM numbers(10000) SETTINGS max_block_size = 1000, min_insert_block_size_rows = 1000;
SELECT count() FROM t_returning;

SELECT 'Parallel insert';
INSERT INTO t_returning (id, s) RETURNING (SELECT count(), sum(id) FROM t_returning)
SELECT number + 1000000, '' FROM numbers(10000) SETTINGS max_insert_threads = 4, max_threads = 4, max_block_size = 1000, min_insert_block_size_rows = 1000;
SELECT count() FROM t_returning WHERE id >= 1000000 AND id < 2000000;

SELECT 'LIMIT does not stop the insert';
INSERT INTO t_returning (id, s) RETURNING (SELECT id FROM t_returning ORDER BY id LIMIT 2)
SELECT number + 2000000, '' FROM numbers(1000) SETTINGS max_block_size = 100, min_insert_block_size_rows = 100;
INSERT INTO t_returning (id, s) RETURNING (SELECT id FROM t_returning LIMIT 0)
SELECT number + 2100000, '' FROM numbers(1000) SETTINGS max_block_size = 100, min_insert_block_size_rows = 100;
SELECT count() FROM t_returning WHERE id >= 2000000 AND id < 3000000;

SELECT 'Without FROM, the RETURNING query reads the inserted data';
INSERT INTO t_returning (id, s) RETURNING (SELECT count(), sum(id)) VALUES (7, 'g'), (8, 'h');
INSERT INTO t_returning (id, s) RETURNING (SELECT 'Hello') VALUES (9, 'i'), (10, 'j');

SELECT 'INSERT INTO FUNCTION';
INSERT INTO FUNCTION null('x UInt64, y String') RETURNING (SELECT count(), sum(x), max(y)) SELECT number, toString(number) FROM numbers(1000);

SELECT 'The RETURNING query does not read the table';
INSERT INTO t_returning (id, s) RETURNING (SELECT 'Hello' FROM system.one) VALUES (3000000, 'x');
INSERT INTO t_returning (id, s) RETURNING (SELECT count() FROM t_returning WHERE 0) VALUES (3000001, 'x');
SELECT count() FROM t_returning WHERE id >= 3000000 AND id < 4000000;

SELECT 'The table can be read only once';
INSERT INTO t_returning (id, s) RETURNING (SELECT count() FROM t_returning UNION ALL SELECT count() FROM t_returning) VALUES (4000000, 'x'); -- { serverError NOT_IMPLEMENTED }

SELECT 'Materialized views';
CREATE TABLE t_returning_mv_target (id UInt64) ENGINE = MergeTree ORDER BY id;
CREATE MATERIALIZED VIEW t_returning_mv TO t_returning_mv_target AS SELECT id * 100 AS id FROM t_returning;
INSERT INTO t_returning (id, s) RETURNING (SELECT groupArray(id) FROM t_returning) VALUES (5000000, 'x'), (5000001, 'y');
SELECT * FROM t_returning_mv_target ORDER BY id;

SELECT 'async_insert is ignored';
INSERT INTO t_returning (id, s) RETURNING (SELECT id, s FROM t_returning) SETTINGS async_insert = 1 VALUES (6000000, 'x');
SELECT count() FROM t_returning WHERE id = 6000000;

SELECT 'Formatting';
SELECT formatQuerySingleLine('INSERT INTO t (x) RETURNING (SELECT x + 1 FROM t WHERE x > 0) SETTINGS max_threads = 1 VALUES (1)');
SELECT formatQuerySingleLine('INSERT INTO t RETURNING (SELECT count() FROM t) SELECT number FROM numbers(10)');

DROP TABLE t_returning_mv;
DROP TABLE t_returning_mv_target;
DROP TABLE t_returning;
