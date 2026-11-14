-- The left row matches one right row through the first disjunct and another right row through the second.
-- `ANY` and `SEMI` joins return the row of the first disjunct in ON order, so swapping the disjuncts swaps the row.

SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET collect_hash_table_stats_during_joins = 0;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (a UInt64, b UInt64) ENGINE = Memory;
CREATE TABLE r (a UInt64, b UInt64, v String) ENGINE = Memory;
INSERT INTO l VALUES (1, 10);
INSERT INTO r VALUES (1, 99, 'by_a'), (7, 10, 'by_b');

SELECT 'hash', 'left any a|b', r.v FROM l LEFT ANY JOIN r ON l.a = r.a OR l.b = r.b SETTINGS join_algorithm = 'hash';
SELECT 'hash', 'left any b|a', r.v FROM l LEFT ANY JOIN r ON l.b = r.b OR l.a = r.a SETTINGS join_algorithm = 'hash';
SELECT 'hash', 'inner any a|b', r.v FROM l INNER ANY JOIN r ON l.a = r.a OR l.b = r.b SETTINGS join_algorithm = 'hash';
SELECT 'hash', 'inner any b|a', r.v FROM l INNER ANY JOIN r ON l.b = r.b OR l.a = r.a SETTINGS join_algorithm = 'hash';
SELECT 'hash', 'left semi a|b', r.v FROM l LEFT SEMI JOIN r ON l.a = r.a OR l.b = r.b SETTINGS join_algorithm = 'hash';
SELECT 'hash', 'left semi b|a', r.v FROM l LEFT SEMI JOIN r ON l.b = r.b OR l.a = r.a SETTINGS join_algorithm = 'hash';

SELECT 'parallel', 'left any a|b', r.v FROM l LEFT ANY JOIN r ON l.a = r.a OR l.b = r.b
    SETTINGS join_algorithm = 'parallel_hash', hash_join_layout = 'parallel', max_threads = 4;
SELECT 'parallel', 'left any b|a', r.v FROM l LEFT ANY JOIN r ON l.b = r.b OR l.a = r.a
    SETTINGS join_algorithm = 'parallel_hash', hash_join_layout = 'parallel', max_threads = 4;
SELECT 'serial', 'left any a|b', r.v FROM l LEFT ANY JOIN r ON l.a = r.a OR l.b = r.b
    SETTINGS join_algorithm = 'parallel_hash', hash_join_layout = 'serial', max_threads = 4;
SELECT 'serial', 'left any b|a', r.v FROM l LEFT ANY JOIN r ON l.b = r.b OR l.a = r.a
    SETTINGS join_algorithm = 'parallel_hash', hash_join_layout = 'serial', max_threads = 4;

DROP TABLE l;
DROP TABLE r;
