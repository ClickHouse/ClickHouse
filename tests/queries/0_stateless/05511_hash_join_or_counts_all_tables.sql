-- A hash join with several disjuncts reports the keys and bytes of all its tables. The first disjunct takes only the
-- 90 right rows with `flag = 1`, which hold all 37 values of `k`, as 10 and 37 are coprime. The second takes all 900
-- rows, whose `s` are distinct. So the join holds 937 keys, in either disjunct order; a join that counted one table
-- would report 37 or 900. Both orders hold the same tables, so they also report about the same bytes. The counts come
-- from the trace line `Join data is built, <bytes> and <keys> rows in hash table`, and from `EXPLAIN ANALYZE`.

SET join_algorithm = 'hash';
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET collect_hash_table_stats_during_joins = 0;
SET enable_join_runtime_filters = 0;
SET max_threads = 4;

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (k UInt64, s String) ENGINE = MergeTree ORDER BY ();
CREATE TABLE r (k UInt64, s String, flag UInt8) ENGINE = MergeTree ORDER BY ();
INSERT INTO l SELECT number, toString(number) FROM numbers(10);
INSERT INTO r SELECT number % 37, toString(number), number % 10 = 0 FROM numbers(900);

SELECT count() FROM l INNER JOIN r ON (l.k = r.k AND r.flag = 1) OR l.s = r.s
SETTINGS hash_join_layout = 'serial', log_comment = '05511_ks_serial' FORMAT Null;
SELECT count() FROM l INNER JOIN r ON l.s = r.s OR (l.k = r.k AND r.flag = 1)
SETTINGS hash_join_layout = 'serial', log_comment = '05511_sk_serial' FORMAT Null;
SELECT count() FROM l INNER JOIN r ON (l.k = r.k AND r.flag = 1) OR l.s = r.s
SETTINGS hash_join_layout = 'parallel', log_comment = '05511_ks_parallel' FORMAT Null;
SELECT count() FROM l INNER JOIN r ON l.s = r.s OR (l.k = r.k AND r.flag = 1)
SETTINGS hash_join_layout = 'parallel', log_comment = '05511_sk_parallel' FORMAT Null;

SYSTEM FLUSH LOGS query_log, text_log;

DROP TABLE IF EXISTS built;
CREATE TABLE built ENGINE = Memory AS
SELECT
    q.log_comment AS log_comment,
    maxIf(toUInt64OrZero(extract(t.message, 'and (\\d+) rows in hash table')), t.message LIKE 'Join data is built%') AS keys,
    maxIf(parseReadableSizeOrZero(extract(t.message, 'Join data is built, ([0-9.]+ [A-Za-z]+) and')), t.message LIKE 'Join data is built%') AS bytes
FROM system.text_log AS t
INNER JOIN
(
    SELECT query_id, log_comment FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish' AND match(log_comment, '^05511_(ks|sk)_')
) AS q ON t.query_id = q.query_id
WHERE t.event_date >= yesterday() AND t.event_time >= now() - 600
GROUP BY q.log_comment;

SELECT 'keys', log_comment, keys FROM built ORDER BY log_comment;
SELECT 'bytes', replaceRegexpOne(log_comment, '_(ks|sk)_', '_') AS setup, max(bytes) <= min(bytes) * 1.1 FROM built GROUP BY setup ORDER BY setup;

SELECT 'explain', extract(arrayStringConcat(groupArray(explain), ' '), 'unique keys ([0-9.]+)')
FROM (EXPLAIN ANALYZE SELECT count() FROM l INNER JOIN r ON (l.k = r.k AND r.flag = 1) OR l.s = r.s SETTINGS hash_join_layout = 'parallel');
SELECT 'explain swapped', extract(arrayStringConcat(groupArray(explain), ' '), 'unique keys ([0-9.]+)')
FROM (EXPLAIN ANALYZE SELECT count() FROM l INNER JOIN r ON l.s = r.s OR (l.k = r.k AND r.flag = 1) SETTINGS hash_join_layout = 'parallel');

DROP TABLE built;
DROP TABLE l;
DROP TABLE r;
