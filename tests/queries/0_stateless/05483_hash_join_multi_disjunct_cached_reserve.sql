-- The cached hash table size of a join counts the keys of all its disjuncts, while each disjunct has its own map.
-- A second run must not reserve that size in every map, so only a join with one disjunct preallocates.

DROP TABLE IF EXISTS l;
DROP TABLE IF EXISTS r;

CREATE TABLE l (a UInt64, b UInt64, c UInt64, d UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE r (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO l SELECT number, number + 1, number + 2, number + 3 FROM numbers(1000);
INSERT INTO r SELECT number, number FROM numbers(100000);

SET join_algorithm = 'hash', hash_join_layout = 'parallel', max_threads = 4;
SET collect_hash_table_stats_during_joins = 1, enable_join_runtime_filters = 0;
SET query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 10, query_plan_optimize_join_order_randomize = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

SELECT count() FROM l ALL LEFT JOIN r ON l.a = r.k OR l.b = r.k OR l.c = r.k OR l.d = r.k FORMAT Null;
SELECT count() FROM l ALL LEFT JOIN r ON l.a = r.k OR l.b = r.k OR l.c = r.k OR l.d = r.k FORMAT Null SETTINGS log_comment = '05483_or_second';

SELECT count() FROM l ALL LEFT JOIN r ON l.a = r.k FORMAT Null;
SELECT count() FROM l ALL LEFT JOIN r ON l.a = r.k FORMAT Null SETTINGS log_comment = '05483_one_second';

SYSTEM FLUSH LOGS query_log;

-- The second run of the join with one disjunct reserves up to its 100000 keys, so the cache is read. The `OR` join reserves nothing.
SELECT log_comment, if(log_comment = '05483_or_second', reserved = 0, reserved BETWEEN 1 AND 100000)
FROM
(
    SELECT log_comment, ProfileEvents['HashJoinPreallocatedElementsInHashTables'] AS reserved
    FROM system.query_log
    WHERE event_date >= yesterday() AND event_time >= now() - 600 AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND log_comment IN ('05483_or_second', '05483_one_second')
)
ORDER BY log_comment;

DROP TABLE l;
DROP TABLE r;
