-- The sizes and the set bits depend on the hash table and the Bloom filter layout, and the probe counts on when
-- the probe side starts to read, so they are not checked.

-- EXPLAIN ANALYZE rejects distributed plans.
SET enable_parallel_replicas = 0;
SET enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0;
SET join_algorithm = 'hash', query_plan_join_swap_table = 0, query_plan_optimize_join_order_randomize = 0;
-- Without the join order optimization, the join does not convert its hash table into a `FixedHashMap`.
SET query_plan_optimize_join_order_limit = 10;
SET join_runtime_filter_from_fixed_hash_table = 0;
SET max_threads = 1;

DROP TABLE IF EXISTS t_probe;
DROP TABLE IF EXISTS t_build;
DROP TABLE IF EXISTS t_probe_nullable;
DROP TABLE IF EXISTS t_build_nullable;
DROP TABLE IF EXISTS t_probe_uint16;
DROP TABLE IF EXISTS t_build_uint16;

CREATE TABLE t_probe (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_build (k UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe_nullable (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_build_nullable (k Nullable(UInt64)) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_probe_uint16 (k UInt16) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE t_build_uint16 (k UInt16) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_probe SELECT number FROM numbers(100000);
INSERT INTO t_build SELECT number * 7 FROM numbers(1000);
INSERT INTO t_probe_nullable SELECT number FROM numbers(100000);
INSERT INTO t_build_nullable SELECT number * 7 FROM numbers(1000);
INSERT INTO t_probe_uint16 SELECT number FROM numbers(65536);
INSERT INTO t_build_uint16 SELECT number * 7 FROM numbers(1000);

SELECT 'exact set';
SELECT replaceRegexpAll(replaceRegexpOne(explain, '^[│└├─ ]+', ''), ' · (size|set bits) [^·]*[^ ·]', '')
FROM (EXPLAIN ANALYZE SELECT count() FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k)
WHERE explain LIKE '%Runtime filter:%';

SELECT 'bloom filter';
SELECT replaceRegexpAll(replaceRegexpOne(explain, '^[│└├─ ]+', ''), ' · (size|set bits) [^·]*[^ ·]', '')
FROM (EXPLAIN ANALYZE SELECT count() FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k SETTINGS join_runtime_filter_exact_values_limit = 100)
WHERE explain LIKE '%Runtime filter:%';

-- A (U)Int8 or (U)Int16 key whose exact set overflows gets a dense bitset instead of a Bloom filter.
SELECT 'dense bitset';
SELECT replaceRegexpAll(replaceRegexpOne(explain, '^[│└├─ ]+', ''), ' · (size|set bits) [^·]*[^ ·]', '')
FROM (EXPLAIN ANALYZE SELECT count() FROM t_probe_uint16 AS p INNER JOIN t_build_uint16 AS b ON p.k = b.k SETTINGS join_runtime_filter_exact_values_limit = 100)
WHERE explain LIKE '%Runtime filter:%';

-- A Bloom filter with any bit set is too full. Whether it is dropped after it was built, or not built because the
-- hash table statistics of an earlier run predict that, depends on those statistics. Either way, no row is checked.
SELECT 'disabled bloom filter';
SELECT
    countIf(explain LIKE '%Runtime filter: type disabled · reason bloom filter %too full%'),
    countIf(explain LIKE '%Probe: checked 0.00 · passed 0.00 · skipped %')
FROM (EXPLAIN ANALYZE SELECT count() FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k
    SETTINGS join_runtime_filter_exact_values_limit = 100, join_runtime_bloom_filter_max_ratio_of_set_bits = 0);

-- A `Nullable` key has no Bloom filter, so a positive filter whose exact set overflows passes every row.
SELECT 'nullable key';
SELECT replaceRegexpAll(replaceRegexpOne(explain, '^[│└├─ ]+', ''), ' · (size|set bits) [^·]*[^ ·]', '')
FROM (EXPLAIN ANALYZE SELECT count() FROM t_probe_nullable AS p INNER JOIN t_build_nullable AS b ON p.k = b.k SETTINGS join_runtime_filter_exact_values_limit = 100)
WHERE explain LIKE '%Runtime filter:%';

-- `NOT IN` keeps the keys it has when the exact set overflows.
SELECT 'anti join';
SELECT replaceRegexpAll(replaceRegexpOne(explain, '^[│└├─ ]+', ''), ' · (size|set bits) [^·]*[^ ·]', '')
FROM (EXPLAIN ANALYZE SELECT count() FROM t_probe AS p LEFT ANTI JOIN t_build AS b ON p.k = b.k)
WHERE explain LIKE '%Runtime filter:%';

SELECT replaceRegexpAll(replaceRegexpOne(explain, '^[│└├─ ]+', ''), ' · (size|set bits) [^·]*[^ ·]', '')
FROM (EXPLAIN ANALYZE SELECT count() FROM t_probe AS p LEFT ANTI JOIN t_build AS b ON p.k = b.k SETTINGS join_runtime_filter_exact_values_limit = 100)
WHERE explain LIKE '%Runtime filter:%';

-- The keys span less than 2^16, so the join converts its hash table into a `FixedHashMap` and uses it in place of
-- the filter it built.
SELECT 'fixed hash map';
SELECT replaceRegexpAll(replaceRegexpOne(explain, '^[│└├─ ]+', ''), ' · (size|set bits) [^·]*[^ ·]', '')
FROM (EXPLAIN ANALYZE SELECT count() FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k SETTINGS join_runtime_filter_from_fixed_hash_table = 1)
WHERE explain LIKE '%Runtime filter:%';

SELECT replaceRegexpAll(replaceRegexpOne(explain, '^[│└├─ ]+', ''), ' · (size|set bits) [^·]*[^ ·]', '')
FROM (EXPLAIN ANALYZE SELECT count() FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k
    SETTINGS join_runtime_filter_from_fixed_hash_table = 1, join_runtime_filter_exact_values_limit = 100)
WHERE explain LIKE '%Runtime filter:%';

SELECT 'probe';
SELECT countIf(match(explain, '^[│└├─ ]*Probe: checked [0-9.]+( thousand)? · passed [0-9.]+( thousand)?( · pass rate [0-9.]+%)? · skipped [0-9.]+( thousand)? · before ready [0-9.]+( thousand)?$'))
FROM (EXPLAIN ANALYZE SELECT count() FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k);

SELECT 'no runtime filter';
SELECT countIf(explain LIKE '%Runtime filter:%')
FROM (EXPLAIN ANALYZE SELECT count() FROM t_probe AS p INNER JOIN t_build AS b ON p.k = b.k SETTINGS enable_join_runtime_filters = 0);

DROP TABLE t_probe;
DROP TABLE t_build;
DROP TABLE t_probe_nullable;
DROP TABLE t_build_nullable;
DROP TABLE t_probe_uint16;
DROP TABLE t_build_uint16;
