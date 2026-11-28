SET enable_analyzer = 1, enable_parallel_replicas = 0;
SET join_algorithm = 'hash', hash_join_layout = 'serial', max_threads = 1, max_block_size = 65536;
SET query_plan_join_shard_by_pk_ranges = 0, query_plan_read_in_order_through_join = 0;
SET query_plan_join_swap_table = 0, query_plan_optimize_join_order_limit = 1, query_plan_convert_join_to_in = 0;
SET enable_join_key_only_hash_tables = 1, enable_join_runtime_filters = 0, enable_join_fixed_hash_table_conversion = 0;
SET enable_hash_join_row_store = 0, collect_hash_table_stats_during_joins = 0, any_join_distinct_right_table_keys = 0;
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;
SET join_use_nulls = 0, aggregate_functions_null_for_empty = 0, use_query_cache = 0;
-- Keep the initial miss-only block separate from the phase's successful suffix.
SET min_insert_block_size_rows = 0, min_insert_block_size_bytes = 0, max_insert_block_size = 65536;

DROP VIEW IF EXISTS auto_fp_05812_miss;
DROP VIEW IF EXISTS auto_fp_05812_one;
DROP VIEW IF EXISTS auto_fp_05812_all;
DROP VIEW IF EXISTS auto_fp_05812_special;
DROP VIEW IF EXISTS auto_fp_05812_phase;
DROP TABLE IF EXISTS auto_fp_05812_probe;
DROP TABLE IF EXISTS auto_fp_05812_build;
DROP TABLE IF EXISTS auto_fp_05812_duplicates;
DROP TABLE IF EXISTS auto_fp_05812_fixed;

CREATE TABLE auto_fp_05812_probe (id UInt64) ENGINE = Memory;
CREATE TABLE auto_fp_05812_build (k UInt64, v UInt64) ENGINE = Memory;
CREATE TABLE auto_fp_05812_duplicates AS auto_fp_05812_build ENGINE = Memory;
CREATE TABLE auto_fp_05812_fixed AS auto_fp_05812_build ENGINE = Memory;
INSERT INTO auto_fp_05812_probe SELECT number FROM numbers(4194304);
-- Bijective multiplication spreads keys. Exceptional keys carry zero and maximum `UInt64` payloads; sums wrap.
INSERT INTO auto_fp_05812_build
SELECT if(number = 131071, toUInt64(18446744073709551615), number * toUInt64(11400714819323198485)),
       multiIf(number = 0, toUInt64(0), number = 131071, toUInt64(18446744073709551615), toUInt64(7))
FROM numbers(131072);
INSERT INTO auto_fp_05812_duplicates SELECT k, v FROM auto_fp_05812_build ARRAY JOIN range(3) AS duplicate;
INSERT INTO auto_fp_05812_fixed SELECT number, toUInt64(7) FROM numbers(256);

CREATE VIEW auto_fp_05812_miss AS
SELECT id, (toUInt64(131072) + id) * toUInt64(11400714819323198485) AS k FROM auto_fp_05812_probe;
CREATE VIEW auto_fp_05812_all AS
SELECT id, if(id % 131072 = 131071, toUInt64(18446744073709551615), id % 131072 * toUInt64(11400714819323198485)) AS k FROM auto_fp_05812_probe;
CREATE VIEW auto_fp_05812_one AS
WITH id % 131072 AS i
SELECT id, if(id % 100 = 0, if(i = 131071, toUInt64(18446744073709551615), i * toUInt64(11400714819323198485)), (toUInt64(131072) + id) * toUInt64(11400714819323198485)) AS k FROM auto_fp_05812_probe;
CREATE VIEW auto_fp_05812_special AS
SELECT id, multiIf(id = 4194302, toUInt64(0), id = 4194303, toUInt64(18446744073709551615), (toUInt64(131072) + id) * toUInt64(11400714819323198485)) AS k FROM auto_fp_05812_probe;
CREATE VIEW auto_fp_05812_phase AS
WITH id % 131072 AS i
SELECT id, if(id < 65536, (toUInt64(131072) + id) * toUInt64(11400714819323198485), if(i = 131071, toUInt64(18446744073709551615), i * toUInt64(11400714819323198485))) AS k FROM auto_fp_05812_probe ORDER BY id;

-- Exercise the default with a real mapped join and concurrent probe readers, before setting either mode.
SELECT 'default', count(), sum(p.id), sum(b.v)
FROM auto_fp_05812_one AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k) SETTINGS max_threads = 4;

SET experimental_join_fingerprint_mode = 0;
SELECT 'miss', count(), sum(p.id), sum(b.v)
FROM auto_fp_05812_miss AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k);
SELECT 'one', count(), sum(p.id), sum(b.v)
FROM auto_fp_05812_one AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k);
SELECT 'all', count(), sum(p.id), sum(b.v)
FROM auto_fp_05812_all AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k);
SELECT 'semi', count(), sum(p.id)
FROM auto_fp_05812_one AS p LEFT SEMI JOIN (SELECT k FROM auto_fp_05812_build) AS b USING (k);
SELECT 'zero-max', count(), sum(p.id), sum(b.v)
FROM auto_fp_05812_special AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k);
SELECT 'duplicates', count(), sum(p.id), sum(b.v)
FROM auto_fp_05812_one AS p ALL INNER JOIN auto_fp_05812_duplicates AS b USING (k);
SELECT 'phase', count(), sum(p.id), sum(b.v)
FROM auto_fp_05812_phase AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k);
SELECT 'filtered', count(), sum(p.id), sum(b.v)
FROM (SELECT * FROM auto_fp_05812_one WHERE id < 8192) AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k);
SELECT 'short', count(), sum(p.id), sum(b.v)
FROM (SELECT * FROM auto_fp_05812_one ORDER BY id LIMIT 64) AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k);
SELECT 'nullable', count(), sum(p.id), sum(b.v)
FROM (SELECT id, if(id % 97 = 0, NULL, k) AS k FROM auto_fp_05812_one) AS p
ALL INNER JOIN (SELECT toNullable(k) AS k, v FROM auto_fp_05812_build) AS b USING (k);
SELECT 'fixed', count(), sum(p.id), sum(b.v)
FROM (SELECT number AS id, number % 256 AS k FROM numbers(4096)) AS p
ALL INNER JOIN auto_fp_05812_fixed AS b USING (k) SETTINGS enable_join_fixed_hash_table_conversion = 1;

SET experimental_join_fingerprint_mode = 4;
SELECT 'miss', count(), sum(p.id), sum(b.v)
FROM auto_fp_05812_miss AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k);
SELECT 'one', count(), sum(p.id), sum(b.v)
FROM auto_fp_05812_one AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k);
SELECT 'all', count(), sum(p.id), sum(b.v)
FROM auto_fp_05812_all AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k);
SELECT 'semi', count(), sum(p.id)
FROM auto_fp_05812_one AS p LEFT SEMI JOIN (SELECT k FROM auto_fp_05812_build) AS b USING (k);
SELECT 'zero-max', count(), sum(p.id), sum(b.v)
FROM auto_fp_05812_special AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k);
SELECT 'duplicates', count(), sum(p.id), sum(b.v)
FROM auto_fp_05812_one AS p ALL INNER JOIN auto_fp_05812_duplicates AS b USING (k);
SELECT 'phase', count(), sum(p.id), sum(b.v)
FROM auto_fp_05812_phase AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k);
SELECT 'filtered', count(), sum(p.id), sum(b.v)
FROM (SELECT * FROM auto_fp_05812_one WHERE id < 8192) AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k);
SELECT 'short', count(), sum(p.id), sum(b.v)
FROM (SELECT * FROM auto_fp_05812_one ORDER BY id LIMIT 64) AS p ALL INNER JOIN auto_fp_05812_build AS b USING (k);
SELECT 'nullable', count(), sum(p.id), sum(b.v)
FROM (SELECT id, if(id % 97 = 0, NULL, k) AS k FROM auto_fp_05812_one) AS p
ALL INNER JOIN (SELECT toNullable(k) AS k, v FROM auto_fp_05812_build) AS b USING (k);
SELECT 'fixed', count(), sum(p.id), sum(b.v)
FROM (SELECT number AS id, number % 256 AS k FROM numbers(4096)) AS p
ALL INNER JOIN auto_fp_05812_fixed AS b USING (k) SETTINGS enable_join_fixed_hash_table_conversion = 1;

DROP VIEW auto_fp_05812_miss;
DROP VIEW auto_fp_05812_one;
DROP VIEW auto_fp_05812_all;
DROP VIEW auto_fp_05812_special;
DROP VIEW auto_fp_05812_phase;
DROP TABLE auto_fp_05812_probe;
DROP TABLE auto_fp_05812_build;
DROP TABLE auto_fp_05812_duplicates;
DROP TABLE auto_fp_05812_fixed;
