-- Tags: shard
-- The limit on groups for `GROUP BY ... LIMIT` without aggregate functions must not apply to other aggregations: in the views that the
-- query reads, and in its subqueries that shards execute.

SET optimize_trivial_group_by_limit_query = 1, max_block_size = 10, max_threads = 1;

DROP VIEW IF EXISTS v;
DROP VIEW IF EXISTS pv;
DROP TABLE IF EXISTS t;
DROP TABLE IF EXISTS r;

-- The first 50 keys have 1 row, the others have 4 rows.
CREATE TABLE t (k UInt64) ENGINE = Memory;
INSERT INTO t SELECT if(number < 100, number, 50 + number % 50) FROM numbers(250);
CREATE VIEW v AS SELECT k, count() AS c FROM t GROUP BY k;
CREATE VIEW pv AS SELECT k, count() AS c FROM t WHERE k >= {first:UInt64} GROUP BY k;

CREATE TABLE r (q String, x UInt64) ENGINE = Memory;
INSERT INTO r SELECT 'view', k FROM v WHERE c = 4 GROUP BY k LIMIT 5;
INSERT INTO r SELECT 'values', c FROM v GROUP BY c LIMIT 5;
INSERT INTO r SELECT 'parameterized view', k FROM pv(first = 0) WHERE c = 4 GROUP BY k LIMIT 5;
INSERT INTO r SELECT 'view in JOIN', t.k FROM t INNER JOIN v ON t.k = v.k WHERE v.c = 4 GROUP BY t.k LIMIT 5;
INSERT INTO r SELECT 'function view', k FROM view(SELECT k, count() AS c FROM t GROUP BY k) WHERE c = 4 GROUP BY k LIMIT 5;
INSERT INTO r SELECT 'function merge', k FROM merge(currentDatabase(), '^v$') WHERE c = 4 GROUP BY k LIMIT 5;
INSERT INTO r SELECT 'function remote', k FROM remote('127.0.0.{2,3}', currentDatabase(), v) WHERE c = 4 GROUP BY k LIMIT 5;
INSERT INTO r SELECT 'subquery', k FROM (SELECT k, count() AS c FROM t GROUP BY k) WHERE c = 4 GROUP BY k LIMIT 5;
INSERT INTO r SELECT 'IN on shards', k FROM remote('127.0.0.{2,3}', currentDatabase(), t) WHERE k IN (SELECT k FROM t GROUP BY k HAVING count() = 4) GROUP BY k LIMIT 5;
INSERT INTO r SELECT 'JOIN on shards', a.k FROM remote('127.0.0.{2,3}', currentDatabase(), t) AS a INNER JOIN (SELECT k FROM t GROUP BY k HAVING count() = 4) AS b ON a.k = b.k GROUP BY a.k LIMIT 5;
SELECT q, count(), uniqExact(x), min(x) >= 50 FROM r WHERE q != 'values' GROUP BY q ORDER BY q;
SELECT x FROM r WHERE q = 'values' ORDER BY x;

-- The limit applies to the query itself, also on shards.
SELECT k FROM t GROUP BY k LIMIT 5 FORMAT Null SETTINGS enable_parallel_replicas = 0, log_comment = '05558_local';
SELECT k FROM remote('127.0.0.2', currentDatabase(), t) GROUP BY k LIMIT 5 FORMAT Null SETTINGS enable_parallel_replicas = 0, log_comment = '05558_remote';
SYSTEM FLUSH LOGS query_log;
SELECT log_comment, is_initial_query, sum(ProfileEvents['OverflowAny']) > 0
FROM system.query_log
WHERE event_date >= yesterday() AND type = 'QueryFinish' AND log_comment IN ('05558_local', '05558_remote')
    AND initial_query_id IN (SELECT query_id FROM system.query_log WHERE event_date >= yesterday() AND current_database = currentDatabase())
GROUP BY log_comment, is_initial_query
ORDER BY log_comment, is_initial_query DESC;

DROP VIEW v;
DROP VIEW pv;
DROP TABLE t;
DROP TABLE r;
