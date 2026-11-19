-- A hash join with several disjuncts reports the keys and bytes of all its tables. The first disjunct takes only the
-- 90 right rows with `flag = 1`, which hold all 37 values of `k`, as 10 and 37 are coprime. The second takes all 900
-- rows, whose `s` are distinct. So the join holds 937 keys, in either disjunct order; a join that counted one table
-- would report 37 or 900. Both orders hold the same tables, so they also report about the same bytes. `EXPLAIN ANALYZE`
-- reports both counts as the unique keys and the memory of the hash table.

SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET collect_hash_table_stats_during_joins = 0;
SET enable_join_runtime_filters = 0;
SET max_threads = 4;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;
DROP TABLE IF EXISTS explained;

CREATE TABLE l (k UInt64, s String) ENGINE = MergeTree ORDER BY ();
CREATE TABLE r (k UInt64, s String, flag UInt8) ENGINE = MergeTree ORDER BY ();
INSERT INTO l SELECT number, toString(number) FROM numbers(10);
INSERT INTO r SELECT number % 37, toString(number), number % 10 = 0 FROM numbers(900);

CREATE TABLE explained (layout String, disjuncts String, explain String) ENGINE = Memory;
INSERT INTO explained SELECT 'serial', 'ks', explain
FROM (EXPLAIN ANALYZE SELECT count() FROM l INNER JOIN r ON (l.k = r.k AND r.flag = 1) OR l.s = r.s SETTINGS hash_join_layout = 'serial');
INSERT INTO explained SELECT 'serial', 'sk', explain
FROM (EXPLAIN ANALYZE SELECT count() FROM l INNER JOIN r ON l.s = r.s OR (l.k = r.k AND r.flag = 1) SETTINGS hash_join_layout = 'serial');
INSERT INTO explained SELECT 'parallel', 'ks', explain
FROM (EXPLAIN ANALYZE SELECT count() FROM l INNER JOIN r ON (l.k = r.k AND r.flag = 1) OR l.s = r.s SETTINGS hash_join_layout = 'parallel');
INSERT INTO explained SELECT 'parallel', 'sk', explain
FROM (EXPLAIN ANALYZE SELECT count() FROM l INNER JOIN r ON l.s = r.s OR (l.k = r.k AND r.flag = 1) SETTINGS hash_join_layout = 'parallel');

SELECT 'keys', layout, disjuncts, maxIf(extract(explain, 'unique keys ([0-9.]+)'), explain LIKE '%Hash table:%')
FROM explained GROUP BY layout, disjuncts ORDER BY layout, disjuncts;

SELECT 'bytes', layout, min(bytes) > 0 AND max(bytes) <= min(bytes) * 1.1
FROM
(
    SELECT layout, maxIf(parseReadableSizeOrZero(extract(explain, 'memory ([0-9.]+ [A-Za-z]+)')), explain LIKE '%Hash table:%') AS bytes
    FROM explained GROUP BY layout, disjuncts
)
GROUP BY layout ORDER BY layout;

DROP TABLE explained;
DROP TABLE l;
DROP TABLE r;
