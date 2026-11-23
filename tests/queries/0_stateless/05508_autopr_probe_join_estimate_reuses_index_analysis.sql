-- The plan with parallel replicas that the automatic mode builds to decide is optimized without primary-key analysis:
-- its reads take the analysis of the single-node plan. Join optimization estimates every input of a join, and for a
-- read without an analysis that estimate used to analyze it, a second time and unpruned, so the probe checked every
-- mark of the tables against the primary key once more.

SET enable_analyzer=1;

-- Mode 2 builds the plan with parallel replicas on every run, while mode 1 would decide each query after the first
-- from the statistics the first one collected, without building it.
SET enable_parallel_replicas=1, automatic_parallel_replicas_mode=2, parallel_replicas_local_plan=1,
    parallel_replicas_for_non_replicated_merge_tree=1, max_parallel_replicas=3, cluster_for_parallel_replicas='parallel_replicas';
SET query_plan_optimize_join_order_randomize=0;
-- Don't let the min-bytes gate skip the small reads before the plan with parallel replicas is built.
SET automatic_parallel_replicas_min_bytes_per_replica=0;
SET use_query_condition_cache=0;
-- With a throwing read limit, which the test profile sets, a join estimate analyzes without keeping the result,
-- so the single-node plan analyzes every read again later, and there is no analysis for the probe to take.
SET max_rows_to_read=0;

CREATE TABLE fact (k UInt64, d UInt32, v Float64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE dim (d UInt32, name String) ENGINE = MergeTree ORDER BY d;
CREATE TABLE dim2 (d UInt32, name String) ENGINE = MergeTree ORDER BY d;
CREATE TABLE dim3 (d UInt32, m UInt8) ENGINE = MergeTree ORDER BY d;
INSERT INTO fact SELECT number, number % 1000, number / 7 FROM numbers(1000000);
INSERT INTO dim SELECT number, 'n' || toString(number) FROM numbers(1000);
INSERT INTO dim2 SELECT number, 'm' || toString(number) FROM numbers(1000);
INSERT INTO dim3 SELECT number, number % 12 FROM numbers(1000);

-- Each query runs with automatic parallel replicas and without.
SELECT dim.name, sum(fact.v) FROM fact INNER JOIN dim ON fact.d = dim.d
WHERE fact.k BETWEEN 100000 AND 110000 AND dim.d < 100
GROUP BY dim.name FORMAT Null SETTINGS log_comment = '05508_join_auto';

SELECT dim.name, sum(fact.v) FROM fact INNER JOIN dim ON fact.d = dim.d
WHERE fact.k BETWEEN 100000 AND 110000 AND dim.d < 100
GROUP BY dim.name FORMAT Null SETTINGS log_comment = '05508_join_off', enable_parallel_replicas = 0;

-- The plan with parallel replicas numbers its table expressions differently from the single-node plan when the big
-- table is written last, so only explicit aliases tell which read is which.
SELECT sum(b.v), sum(m.name = ''), sum(s.name = '') FROM dim AS m, dim2 AS s, fact AS b
WHERE m.d = b.d AND s.d = b.d AND b.k BETWEEN 100000 AND 110000 AND m.d < 100
FORMAT Null SETTINGS log_comment = '05508_big_table_last_auto';

SELECT sum(b.v), sum(m.name = ''), sum(s.name = '') FROM dim AS m, dim2 AS s, fact AS b
WHERE m.d = b.d AND s.d = b.d AND b.k BETWEEN 100000 AND 110000 AND m.d < 100
FORMAT Null SETTINGS log_comment = '05508_big_table_last_off', enable_parallel_replicas = 0;

-- A table read twice, filtered differently.
SELECT sum(b.v) FROM dim AS d1, dim AS d2, fact AS b
WHERE d1.d = b.d AND d2.d = b.d + 1 AND b.k BETWEEN 100000 AND 110000 AND d1.d < 100 AND d2.d > 500
FORMAT Null SETTINGS log_comment = '05508_table_twice_auto';

SELECT sum(b.v) FROM dim AS d1, dim AS d2, fact AS b
WHERE d1.d = b.d AND d2.d = b.d + 1 AND b.k BETWEEN 100000 AND 110000 AND d1.d < 100 AND d2.d > 500
FORMAT Null SETTINGS log_comment = '05508_table_twice_off', enable_parallel_replicas = 0;

-- A CTE used twice.
WITH c AS (SELECT k, d, v FROM fact WHERE k BETWEEN 100000 AND 110000)
SELECT count(), sum(c1.v) FROM dim AS m, c AS c1, c AS c2 WHERE c1.d = c2.d AND m.d = c1.d AND m.d < 100
FORMAT Null SETTINGS log_comment = '05508_cte_twice_auto';

WITH c AS (SELECT k, d, v FROM fact WHERE k BETWEEN 100000 AND 110000)
SELECT count(), sum(c1.v) FROM dim AS m, c AS c1, c AS c2 WHERE c1.d = c2.d AND m.d = c1.d AND m.d < 100
FORMAT Null SETTINGS log_comment = '05508_cte_twice_off', enable_parallel_replicas = 0;

-- A CTE over a join, used twice. The plan with parallel replicas numbers the table expressions of each copy from 1,
-- so both copies of a table get the same name there and different names in the single-node plan. Its reads cannot be
-- paired, and they are left without an estimate rather than analyzed.
WITH inv AS (SELECT f.d AS d, c.m AS m, sum(f.v) AS s FROM fact AS f, dim AS a, dim2 AS b, dim3 AS c
             WHERE f.d = a.d AND f.d = b.d AND f.d = c.d AND f.k BETWEEN 100000 AND 110000 GROUP BY f.d, c.m)
SELECT count(), sum(i1.s + i2.s) FROM inv AS i1, inv AS i2 WHERE i1.d = i2.d AND i1.m = 1 AND i2.m = 2
FORMAT Null SETTINGS log_comment = '05508_cte_over_join_twice_auto';

WITH inv AS (SELECT f.d AS d, c.m AS m, sum(f.v) AS s FROM fact AS f, dim AS a, dim2 AS b, dim3 AS c
             WHERE f.d = a.d AND f.d = b.d AND f.d = c.d AND f.k BETWEEN 100000 AND 110000 GROUP BY f.d, c.m)
SELECT count(), sum(i1.s + i2.s) FROM inv AS i1, inv AS i2 WHERE i1.d = i2.d AND i1.m = 1 AND i2.m = 2
FORMAT Null SETTINGS log_comment = '05508_cte_over_join_twice_off', enable_parallel_replicas = 0;

-- Under a throwing read limit the single-node join estimates do not keep their analysis, so the probe finds none to
-- take, and its reads are left without an estimate rather than analyzed.
SELECT dim.name, sum(fact.v) FROM fact INNER JOIN dim ON fact.d = dim.d
WHERE fact.k BETWEEN 100000 AND 110000 AND dim.d < 100
GROUP BY dim.name FORMAT Null SETTINGS log_comment = '05508_throwing_limit_auto', max_rows_to_read = 1000000000;

SELECT dim.name, sum(fact.v) FROM fact INNER JOIN dim ON fact.d = dim.d
WHERE fact.k BETWEEN 100000 AND 110000 AND dim.d < 100
GROUP BY dim.name FORMAT Null SETTINGS log_comment = '05508_throwing_limit_off', max_rows_to_read = 1000000000, enable_parallel_replicas = 0;

-- The same limit with the small table written first: the single-node plan swaps the sides of the join by its
-- estimates, and the probe has to make the same choice, or the two plans cannot be matched. Without column
-- statistics the index analysis is the only estimate there is.
SELECT dim.name, sum(fact.v) FROM dim INNER JOIN fact ON fact.d = dim.d
GROUP BY dim.name FORMAT Null SETTINGS log_comment = '05508_throwing_limit_swap_auto', max_rows_to_read = 1000000000, use_statistics = 0;

SELECT dim.name, sum(fact.v) FROM dim INNER JOIN fact ON fact.d = dim.d
GROUP BY dim.name FORMAT Null SETTINGS log_comment = '05508_throwing_limit_swap_off', max_rows_to_read = 1000000000, use_statistics = 0, enable_parallel_replicas = 0;

SYSTEM FLUSH LOGS query_log;

SELECT
    q.log_comment,
    q.ProfileEvents['AutomaticParallelReplicasProbePlansBuilt'] AS plan_built,
    q.ProfileEvents['IndexAnalysisRounds'] = base.ProfileEvents['IndexAnalysisRounds'] AS same_index_analysis_rounds,
    q.ProfileEvents['FilteringMarksWithPrimaryKeyProcessedMarks'] = base.ProfileEvents['FilteringMarksWithPrimaryKeyProcessedMarks']
        AS same_marks_checked
FROM system.query_log AS q, system.query_log AS base
WHERE q.current_database = currentDatabase() AND base.current_database = currentDatabase()
    AND q.type = 'QueryFinish' AND base.type = 'QueryFinish' AND q.event_date >= yesterday() AND base.event_date >= yesterday()
    AND q.query_id = q.initial_query_id AND base.query_id = base.initial_query_id
    AND endsWith(q.log_comment, '_auto') AND base.log_comment = replaceOne(q.log_comment, '_auto', '_off')
ORDER BY q.event_time_microseconds;

SELECT log_comment, ProfileEvents['AutoParallelReplicasPlansNotComparable'] AS not_comparable
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND event_date >= yesterday()
    AND query_id = initial_query_id AND log_comment = '05508_throwing_limit_swap_auto';
