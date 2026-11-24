-- Hash join range tables whose probe prefetches. 200000 keys pass the size rule on an L2 cache of up to 3 MiB, and about 96-98%
-- of the probe keys are in the range, so most blocks prefetch. Half of the `kh` keys are outside the range, so a block stops
-- prefetching after its first rows, and blocks of 1001 rows end inside a batch of prefetched rows. Each query runs on the
-- range table and on the hash table.

SET join_algorithm = 'hash', hash_join_layout = 'serial', max_bytes_ratio_before_external_join = 0, max_bytes_before_external_join = 0;
SET query_plan_join_swap_table = false, query_plan_optimize_join_order_randomize = 0;
SET enable_join_runtime_filters = 0, enable_software_prefetch_in_join = 1, join_use_nulls = 0;

DROP TABLE IF EXISTS rt_build;
DROP TABLE IF EXISTS rt_build_dup;
DROP TABLE IF EXISTS rt_build64;
DROP TABLE IF EXISTS rt_probe;

CREATE TABLE rt_build (k UInt32, v UInt32) ENGINE = Memory;
CREATE TABLE rt_build_dup (k UInt32, v UInt32) ENGINE = Memory;
CREATE TABLE rt_build64 (k Int64, v UInt32) ENGINE = Memory;
CREATE TABLE rt_probe (k UInt32, kn Nullable(UInt32), k64 Int64, kh UInt32, x UInt8) ENGINE = Memory;

INSERT INTO rt_build SELECT number + 1, number FROM numbers(200000);
INSERT INTO rt_build_dup SELECT intDiv(number, 2) + 1, number FROM numbers(400000);
INSERT INTO rt_build64 SELECT toInt64(number) - 100000, number FROM numbers(200000);
-- 2% of the keys are outside the range, 2% of `kn` are NULL, and `x < 98` masks 2% of the rows.
INSERT INTO rt_probe
    SELECT k, if(number % 50 = 13, NULL, k), toInt64(k) - 100001, if(number % 2 = 0, k, 300000 + number % 1000), number % 100
    FROM (SELECT number, if(number % 50 = 7, 300000 + number % 1000, intHash32(number) % 200000 + 1) AS k FROM numbers(300000));

SET enable_join_fixed_hash_table_conversion = 1;
SELECT 'range';
SELECT 'inner', count(), sum(b.v) FROM rt_probe AS p INNER JOIN rt_build AS b ON p.k = b.k;
SELECT 'left', count(), sum(b.v) FROM rt_probe AS p LEFT JOIN rt_build AS b ON p.k = b.k;
SELECT 'right', count(), sum(b.v), sum(p.k) FROM rt_probe AS p RIGHT JOIN rt_build AS b ON p.k = b.k;
SELECT 'full', count(), sum(b.v), sum(p.k) FROM rt_probe AS p FULL JOIN rt_build AS b ON p.k = b.k;
SELECT 'left semi', count(), sum(p.k) FROM rt_probe AS p LEFT SEMI JOIN rt_build AS b ON p.k = b.k;
SELECT 'left anti', count(), sum(p.k) FROM rt_probe AS p LEFT ANTI JOIN rt_build AS b ON p.k = b.k;
SELECT 'right semi', count(), sum(b.v) FROM rt_probe AS p RIGHT SEMI JOIN rt_build AS b ON p.k = b.k;
SELECT 'right anti', count(), sum(b.v) FROM rt_probe AS p RIGHT ANTI JOIN rt_build AS b ON p.k = b.k;
SELECT 'left any', count(), sum(b.v) FROM rt_probe AS p ANY LEFT JOIN rt_build AS b ON p.k = b.k;
SELECT 'inner any', count(), sum(b.v) FROM rt_probe AS p ANY INNER JOIN rt_build AS b ON p.k = b.k;
SELECT 'right any', count(), sum(b.v), sum(p.k) FROM rt_probe AS p ANY RIGHT JOIN rt_build AS b ON p.k = b.k;
SELECT 'inner dup', count(), sum(b.v) FROM rt_probe AS p INNER JOIN rt_build_dup AS b ON p.k = b.k;
SELECT 'left dup', count(), sum(b.v) FROM rt_probe AS p LEFT JOIN rt_build_dup AS b ON p.k = b.k;
SELECT 'nullable', count(), sum(b.v) FROM rt_probe AS p LEFT JOIN rt_build AS b ON p.kn = b.k;
SELECT 'mask', count(), sum(b.v) FROM rt_probe AS p LEFT JOIN rt_build AS b ON p.k = b.k AND p.x < 98;
SELECT 'key64', count(), sum(b.v) FROM rt_probe AS p INNER JOIN rt_build64 AS b ON p.k64 = b.k;
SELECT 'left key64', count(), sum(b.v) FROM rt_probe AS p LEFT JOIN rt_build64 AS b ON p.k64 = b.k;
SELECT 'left half', count(), sum(b.v) FROM rt_probe AS p LEFT JOIN rt_build AS b ON p.kh = b.k;
SELECT 'inner blocks', count(), sum(b.v) FROM (SELECT toUInt32(intHash32(number) % 200000 + 1) AS k FROM numbers(300000)) AS p INNER JOIN rt_build AS b ON p.k = b.k SETTINGS max_block_size = 1001;

SET enable_join_fixed_hash_table_conversion = 0;
SELECT 'hash';
SELECT 'inner', count(), sum(b.v) FROM rt_probe AS p INNER JOIN rt_build AS b ON p.k = b.k;
SELECT 'left', count(), sum(b.v) FROM rt_probe AS p LEFT JOIN rt_build AS b ON p.k = b.k;
SELECT 'right', count(), sum(b.v), sum(p.k) FROM rt_probe AS p RIGHT JOIN rt_build AS b ON p.k = b.k;
SELECT 'full', count(), sum(b.v), sum(p.k) FROM rt_probe AS p FULL JOIN rt_build AS b ON p.k = b.k;
SELECT 'left semi', count(), sum(p.k) FROM rt_probe AS p LEFT SEMI JOIN rt_build AS b ON p.k = b.k;
SELECT 'left anti', count(), sum(p.k) FROM rt_probe AS p LEFT ANTI JOIN rt_build AS b ON p.k = b.k;
SELECT 'right semi', count(), sum(b.v) FROM rt_probe AS p RIGHT SEMI JOIN rt_build AS b ON p.k = b.k;
SELECT 'right anti', count(), sum(b.v) FROM rt_probe AS p RIGHT ANTI JOIN rt_build AS b ON p.k = b.k;
SELECT 'left any', count(), sum(b.v) FROM rt_probe AS p ANY LEFT JOIN rt_build AS b ON p.k = b.k;
SELECT 'inner any', count(), sum(b.v) FROM rt_probe AS p ANY INNER JOIN rt_build AS b ON p.k = b.k;
SELECT 'right any', count(), sum(b.v), sum(p.k) FROM rt_probe AS p ANY RIGHT JOIN rt_build AS b ON p.k = b.k;
SELECT 'inner dup', count(), sum(b.v) FROM rt_probe AS p INNER JOIN rt_build_dup AS b ON p.k = b.k;
SELECT 'left dup', count(), sum(b.v) FROM rt_probe AS p LEFT JOIN rt_build_dup AS b ON p.k = b.k;
SELECT 'nullable', count(), sum(b.v) FROM rt_probe AS p LEFT JOIN rt_build AS b ON p.kn = b.k;
SELECT 'mask', count(), sum(b.v) FROM rt_probe AS p LEFT JOIN rt_build AS b ON p.k = b.k AND p.x < 98;
SELECT 'key64', count(), sum(b.v) FROM rt_probe AS p INNER JOIN rt_build64 AS b ON p.k64 = b.k;
SELECT 'left key64', count(), sum(b.v) FROM rt_probe AS p LEFT JOIN rt_build64 AS b ON p.k64 = b.k;
SELECT 'left half', count(), sum(b.v) FROM rt_probe AS p LEFT JOIN rt_build AS b ON p.kh = b.k;
SELECT 'inner blocks', count(), sum(b.v) FROM (SELECT toUInt32(intHash32(number) % 200000 + 1) AS k FROM numbers(300000)) AS p INNER JOIN rt_build AS b ON p.k = b.k SETTINGS max_block_size = 1001;

DROP TABLE rt_build;
DROP TABLE rt_build_dup;
DROP TABLE rt_build64;
DROP TABLE rt_probe;
