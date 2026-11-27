-- Comparing an `Enum` key with an unsigned numeric constant must keep the strict bound, so the part that only has 'zero' is skipped.

DROP TABLE IF EXISTS t_enum8;
DROP TABLE IF EXISTS t_enum16;
DROP TABLE IF EXISTS t_enum_nullable;

SET enable_parallel_replicas = 0;

CREATE TABLE t_enum8 (e Enum8('neg' = -1, 'zero' = 0, 'one' = 1), v UInt64) ENGINE = MergeTree PARTITION BY e ORDER BY tuple();
INSERT INTO t_enum8 SELECT 'zero', number FROM numbers(1000);
INSERT INTO t_enum8 SELECT 'one', number FROM numbers(10);

CREATE TABLE t_enum16 (e Enum16('neg' = -1000, 'zero' = 0, 'one' = 1000), v UInt64) ENGINE = MergeTree PARTITION BY e ORDER BY tuple();
INSERT INTO t_enum16 SELECT 'zero', number FROM numbers(1000);
INSERT INTO t_enum16 SELECT 'one', number FROM numbers(10);

CREATE TABLE t_enum_nullable (e Nullable(Enum8('neg' = -1, 'zero' = 0, 'one' = 1)), v UInt64) ENGINE = MergeTree PARTITION BY e ORDER BY tuple() SETTINGS allow_nullable_key = 1;
INSERT INTO t_enum_nullable SELECT 'zero', number FROM numbers(1000);
INSERT INTO t_enum_nullable SELECT 'one', number FROM numbers(10);

SELECT 'Enum8';
SELECT sum(v), count() FROM t_enum8 WHERE e > 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_enum8 WHERE e > 0) WHERE explain LIKE '%Parts: %/%';
SELECT sum(v), count() FROM t_enum8 WHERE 0 < e;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_enum8 WHERE 0 < e) WHERE explain LIKE '%Parts: %/%';
SELECT sum(v), count() FROM t_enum8 WHERE e < 1;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_enum8 WHERE e < 1) WHERE explain LIKE '%Parts: %/%';
SELECT sum(v), count() FROM t_enum8 WHERE e != 0;
SELECT sum(v), count() FROM t_enum8 WHERE e >= 1;

SELECT 'Enum16';
SELECT sum(v), count() FROM t_enum16 WHERE e > 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_enum16 WHERE e > 0) WHERE explain LIKE '%Parts: %/%';
SELECT sum(v), count() FROM t_enum16 WHERE e < 1000;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_enum16 WHERE e < 1000) WHERE explain LIKE '%Parts: %/%';

SELECT 'Nullable(Enum8)';
SELECT sum(v), count() FROM t_enum_nullable WHERE e > 0;
SELECT trimLeft(explain) FROM (EXPLAIN indexes = 1 SELECT sum(v) FROM t_enum_nullable WHERE e > 0) WHERE explain LIKE '%Parts: %/%';

DROP TABLE t_enum8;
DROP TABLE t_enum16;
DROP TABLE t_enum_nullable;
