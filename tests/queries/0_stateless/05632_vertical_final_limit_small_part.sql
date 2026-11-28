DROP TABLE IF EXISTS t_vf_limit;
CREATE TABLE t_vf_limit (k String, v String, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k SETTINGS index_granularity = 1024;
SYSTEM STOP MERGES t_vf_limit;
INSERT INTO t_vf_limit SELECT toString(cityHash64(number)), 'x', 1 FROM numbers(25000) SETTINGS min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_block_size = 1000000;
INSERT INTO t_vf_limit SELECT toString(cityHash64(number * 10)), 'y', 2 FROM numbers(2500) SETTINGS min_insert_block_size_rows = 1000000, min_insert_block_size_bytes = 0, max_insert_block_size = 1000000;
-- A small part spanning the whole key range holds the newest version of a key and the only row that passes the filter.
INSERT INTO t_vf_limit SELECT if(number = 0, '0', toString(cityHash64(number * 199))), if(number = 0, 'target', 'z'), 3 FROM numbers(100);

SELECT k FROM t_vf_limit FINAL WHERE v = 'target' LIMIT 1
SETTINGS enable_vertical_final = 1, max_threads = 1, max_block_size = 1024, query_plan_optimize_lazy_final = 0, enable_parallel_replicas = 0, log_comment = 'vf_limit_small_part';
SYSTEM FLUSH LOGS query_log;
SELECT read_rows < 15000 FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = 'vf_limit_small_part';
-- The plain FINAL merge stops early too, so check that the query runs vertical FINAL.
SELECT count() > 0 FROM (EXPLAIN PIPELINE SELECT k FROM t_vf_limit FINAL WHERE v = 'target' LIMIT 1
SETTINGS enable_vertical_final = 1, max_threads = 1, max_block_size = 1024, query_plan_optimize_lazy_final = 0, enable_parallel_replicas = 0)
WHERE explain ILIKE '%SelectByIndicesTransform%';

-- The rows selected before a chunk is merged to its end are emitted only once.
SELECT count(), sum(cityHash64(k, v, ver)) FROM t_vf_limit FINAL SETTINGS enable_vertical_final = 0, max_threads = 1;
SELECT count(), sum(cityHash64(k, v, ver)) FROM t_vf_limit FINAL SETTINGS enable_vertical_final = 1, max_threads = 1, max_block_size = 64;
SELECT count(), sum(cityHash64(k, v, ver)) FROM t_vf_limit FINAL SETTINGS enable_vertical_final = 1, max_threads = 4, max_block_size = 64;
SELECT v, count() FROM t_vf_limit FINAL GROUP BY v ORDER BY v SETTINGS enable_vertical_final = 1, max_threads = 1, max_block_size = 64;
DROP TABLE t_vf_limit;

-- A one-row part: its only row is selected and its input ends at once, while the other inputs are still merged.
DROP TABLE IF EXISTS t_vf_last;
CREATE TABLE t_vf_last (k UInt64, ver UInt64) ENGINE = ReplacingMergeTree(ver) ORDER BY k SETTINGS index_granularity = 64;
SYSTEM STOP MERGES t_vf_last;
INSERT INTO t_vf_last SELECT number * 2, 1 FROM numbers(2500);
INSERT INTO t_vf_last SELECT number * 2 + 1, 1 FROM numbers(2500);
INSERT INTO t_vf_last VALUES (0, 2);
SELECT count(), sum(k), sum(ver) FROM t_vf_last FINAL SETTINGS enable_vertical_final = 1, max_threads = 1, max_block_size = 64, preferred_block_size_bytes = 1000000;
DROP TABLE t_vf_last;
