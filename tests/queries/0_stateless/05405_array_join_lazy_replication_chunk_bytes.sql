-- Lazy replication of an ARRAY JOIN passenger must not change how its output is sized.

DROP TABLE IF EXISTS aj_mv_eager;
DROP TABLE IF EXISTS aj_mv_lazy;
DROP TABLE IF EXISTS aj_dst_eager;
DROP TABLE IF EXISTS aj_dst_lazy;
DROP TABLE IF EXISTS aj_src_eager;
DROP TABLE IF EXISTS aj_src_lazy;

CREATE TABLE aj_src_eager (payload String, arr Array(UInt8)) ENGINE = Null;
CREATE TABLE aj_src_lazy (payload String, arr Array(UInt8)) ENGINE = Null;
CREATE TABLE aj_dst_eager (payload String, x UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE aj_dst_lazy (payload String, x UInt8) ENGINE = MergeTree ORDER BY tuple();
CREATE MATERIALIZED VIEW aj_mv_eager TO aj_dst_eager AS SELECT payload, x FROM aj_src_eager ARRAY JOIN arr AS x;
CREATE MATERIALIZED VIEW aj_mv_lazy TO aj_dst_lazy AS SELECT payload, x FROM aj_src_lazy ARRAY JOIN arr AS x;
SYSTEM STOP MERGES aj_dst_eager;
SYSTEM STOP MERGES aj_dst_lazy;

SET max_block_size = 1000, max_threads = 1, max_insert_threads = 1, min_insert_block_size_rows = 0, min_insert_block_size_bytes = 32000000;

INSERT INTO aj_src_eager SELECT concat(toString(number), repeat('a', 100)), range(10) FROM numbers(20000) SETTINGS enable_lazy_columns_replication = 0;
INSERT INTO aj_src_lazy SELECT concat(toString(number), repeat('a', 100)), range(10) FROM numbers(20000) SETTINGS enable_lazy_columns_replication = 1;
SELECT 'eager', count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'aj_dst_eager' AND active;
SELECT 'lazy', count(), sum(rows) FROM system.parts WHERE database = currentDatabase() AND table = 'aj_dst_lazy' AND active;

SELECT payload, x FROM (SELECT concat(toString(number), repeat('a', 100)) AS payload, range(10) AS arr FROM numbers(20000)) ARRAY JOIN arr AS x WHERE x = 0
SETTINGS enable_lazy_columns_replication = 0, max_result_bytes = 10000000 FORMAT Null;
SELECT payload, x FROM (SELECT concat(toString(number), repeat('a', 100)) AS payload, range(10) AS arr FROM numbers(20000)) ARRAY JOIN arr AS x WHERE x = 0
SETTINGS enable_lazy_columns_replication = 1, query_plan_fuse_filter_into_array_join = 1, max_result_bytes = 10000000 FORMAT Null;
SELECT payload, x FROM (SELECT concat(toString(number), repeat('a', 100)) AS payload, range(10) AS arr FROM numbers(20000)) ARRAY JOIN arr AS x WHERE x = 0
SETTINGS enable_lazy_columns_replication = 1, query_plan_fuse_filter_into_array_join = 0, max_result_bytes = 10000000 FORMAT Null;
SELECT 'limits ok';

DROP TABLE aj_mv_eager;
DROP TABLE aj_mv_lazy;
DROP TABLE aj_dst_eager;
DROP TABLE aj_dst_lazy;
DROP TABLE aj_src_eager;
DROP TABLE aj_src_lazy;
