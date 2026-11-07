-- DPsub with `query_plan_join_order_use_interesting_orders` picks `full_sorting_merge` for joins of inputs sorted by the join key.
-- The tables are small; the statistics hint below gives the sizes the plans are chosen for.

-- Disable force_primary_key_reverse_order: Query plan output depends on sort direction
SET force_primary_key_reverse_order = 0;
SET enable_analyzer = 1;
SET query_plan_optimize_join_order_algorithm = 'dpsub';
SET query_plan_join_order_use_interesting_orders = 1;
SET enable_join_transitive_predicates = 1;
SET optimize_read_in_order = 1;
SET max_rows_in_set_to_optimize_join = 0;
SET query_plan_join_swap_table = 'auto';
SET enable_parallel_replicas = 0;
SET query_plan_optimize_join_order_randomize = 0;
SET query_plan_optimize_join_order_limit = 10;
SET optimize_sorting_by_input_stream_properties = 1;
SET enable_join_runtime_filters = 1;
SET enable_join_runtime_filters_index_analysis = 1;
-- The runtime filter skips probe granules at read time only with this on.
SET use_skip_indexes_on_data_read = 1;
SET use_statistics = 0;
SET use_hash_table_stats_for_join_reordering = 0;
-- A merge join of sorted inputs is costed as sharded by primary key ranges only with this on.
SET query_plan_join_shard_by_pk_ranges = 1;

DROP TABLE IF EXISTS sorted1;
DROP TABLE IF EXISTS sorted2;
DROP TABLE IF EXISTS sorted3;
DROP TABLE IF EXISTS sorted4;
DROP TABLE IF EXISTS unsorted1;
DROP TABLE IF EXISTS unsorted2;
DROP TABLE IF EXISTS tiny;
DROP TABLE IF EXISTS probe;
DROP TABLE IF EXISTS build_narrow;
DROP TABLE IF EXISTS build_wide;
DROP TABLE IF EXISTS build_small;
DROP TABLE IF EXISTS float_a;
DROP TABLE IF EXISTS float_b;
DROP TABLE IF EXISTS nullable_a;
DROP TABLE IF EXISTS nullable_b;
DROP TABLE IF EXISTS no_pk_a;
DROP TABLE IF EXISTS no_pk_b;
DROP TABLE IF EXISTS short_pk_a;
DROP TABLE IF EXISTS short_pk_b;
DROP TABLE IF EXISTS full_pk_a;
DROP TABLE IF EXISTS full_pk_b;
DROP TABLE IF EXISTS two_keys_a;
DROP TABLE IF EXISTS two_keys_b;
DROP TABLE IF EXISTS two_sorted_a;
DROP TABLE IF EXISTS two_sorted_b;
DROP TABLE IF EXISTS tenant_a;
DROP TABLE IF EXISTS tenant_b;

CREATE TABLE sorted1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE sorted2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE sorted3 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE sorted4 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE unsorted1 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE unsorted2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE tiny (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE probe (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE build_narrow (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE build_wide (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE build_small (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE float_a (k Float64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE float_b (k Float64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE nullable_a (k Nullable(UInt64), v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
CREATE TABLE nullable_b (k Nullable(UInt64), v UInt64) ENGINE = MergeTree ORDER BY k SETTINGS allow_nullable_key = 1;
CREATE TABLE no_pk_a (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k PRIMARY KEY tuple();
CREATE TABLE no_pk_b (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k PRIMARY KEY tuple();
CREATE TABLE short_pk_a (k UInt64, s UInt64, v UInt64) ENGINE = MergeTree ORDER BY (k, s) PRIMARY KEY k;
CREATE TABLE short_pk_b (k UInt64, s UInt64, v UInt64) ENGINE = MergeTree ORDER BY (k, s) PRIMARY KEY k;
CREATE TABLE full_pk_a (k UInt64, s UInt64, v UInt64) ENGINE = MergeTree ORDER BY (k, s);
CREATE TABLE full_pk_b (k UInt64, s UInt64, v UInt64) ENGINE = MergeTree ORDER BY (k, s);
CREATE TABLE two_keys_a (k UInt64, s UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE two_keys_b (k UInt64, s UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE two_sorted_a (k UInt64, s UInt64, v UInt64) ENGINE = MergeTree ORDER BY (k, s);
CREATE TABLE two_sorted_b (k UInt64, s UInt64, v UInt64) ENGINE = MergeTree ORDER BY (k, s);
CREATE TABLE tenant_a (tenant UInt64, k UInt64, s UInt64, v UInt64) ENGINE = MergeTree ORDER BY (tenant, k);
CREATE TABLE tenant_b (tenant UInt64, k UInt64, s UInt64, v UInt64) ENGINE = MergeTree ORDER BY (tenant, k);

INSERT INTO sorted1 SELECT number, number FROM numbers(1000);
INSERT INTO sorted2 SELECT number, number FROM numbers(1000);
INSERT INTO sorted3 SELECT number, number FROM numbers(1000);
INSERT INTO sorted4 SELECT number, number FROM numbers(1000);
INSERT INTO unsorted1 SELECT number, number FROM numbers(1000);
INSERT INTO unsorted2 SELECT number, number FROM numbers(1000);
INSERT INTO tiny SELECT number * 1000, number FROM numbers(10);
INSERT INTO probe SELECT number, number FROM numbers(1000);
INSERT INTO build_narrow SELECT number, number FROM numbers(100);
INSERT INTO build_wide SELECT number * 10, number FROM numbers(100);
INSERT INTO build_small SELECT number * 50, number FROM numbers(20);
INSERT INTO float_a SELECT toFloat64(number), number FROM numbers(1000);
INSERT INTO float_b SELECT toFloat64(number), number FROM numbers(1000);
INSERT INTO nullable_a SELECT number, number FROM numbers(1000);
INSERT INTO nullable_b SELECT number, number FROM numbers(1000);
INSERT INTO no_pk_a SELECT number, number FROM numbers(1000);
INSERT INTO no_pk_b SELECT number, number FROM numbers(1000);
INSERT INTO short_pk_a SELECT 0, number, number FROM numbers(1000);
INSERT INTO short_pk_b SELECT 0, number, number FROM numbers(1000);
INSERT INTO full_pk_a SELECT 0, number, number FROM numbers(1000);
INSERT INTO full_pk_b SELECT 0, number, number FROM numbers(1000);
INSERT INTO two_keys_a SELECT number % 2, intHash64(number), number FROM numbers(1000);
INSERT INTO two_keys_b SELECT number % 2, intHash64(number), number FROM numbers(1000);
INSERT INTO two_sorted_a SELECT number % 2, intHash64(number), number FROM numbers(1000);
INSERT INTO two_sorted_b SELECT number % 2, intHash64(number), number FROM numbers(1000);
INSERT INTO tenant_a SELECT 1, number % 2, intHash64(number), number FROM numbers(1000);
INSERT INTO tenant_b SELECT 1, number % 2, intHash64(number), number FROM numbers(1000);

-- A merge join pays a fixed cost, so it wins only for inputs of millions of rows. `build_narrow` covers a hundredth of the
-- probe key range, `build_wide` and `build_small` all of it. `two_*` and `tenant_*` have two values of `k`.
SET param__internal_join_table_stat_hints = '{
    "sorted1": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "sorted2": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "sorted3": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "sorted4": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "unsorted1": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "unsorted2": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "tiny": { "cardinality": 10, "distinct_keys": { "k": 10 } },
    "probe": { "cardinality": 200000000, "distinct_keys": { "k": 200000000 }, "key_ranges": { "k": [0, 200000000] } },
    "build_narrow": { "cardinality": 2000000, "distinct_keys": { "k": 2000000 }, "key_ranges": { "k": [0, 2000000] } },
    "build_wide": { "cardinality": 20000000, "distinct_keys": { "k": 20000000 }, "key_ranges": { "k": [0, 200000000] } },
    "build_small": { "cardinality": 4000000, "distinct_keys": { "k": 4000000 }, "key_ranges": { "k": [0, 200000000] } },
    "float_a": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "float_b": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "nullable_a": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "nullable_b": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "no_pk_a": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "no_pk_b": { "cardinality": 50000000, "distinct_keys": { "k": 50000000 } },
    "short_pk_a": { "cardinality": 50000000, "distinct_keys": { "k": 1, "s": 50000000 } },
    "short_pk_b": { "cardinality": 50000000, "distinct_keys": { "k": 1, "s": 50000000 } },
    "full_pk_a": { "cardinality": 50000000, "distinct_keys": { "k": 1, "s": 50000000 } },
    "full_pk_b": { "cardinality": 50000000, "distinct_keys": { "k": 1, "s": 50000000 } },
    "two_keys_a": { "cardinality": 50000000, "distinct_keys": { "k": 2, "s": 50000000 } },
    "two_keys_b": { "cardinality": 50000000, "distinct_keys": { "k": 2, "s": 50000000 } },
    "two_sorted_a": { "cardinality": 50000000, "distinct_keys": { "k": 2, "s": 50000000 } },
    "two_sorted_b": { "cardinality": 50000000, "distinct_keys": { "k": 2, "s": 50000000 } },
    "tenant_a": { "cardinality": 50000000, "distinct_keys": { "tenant": 1, "k": 2, "s": 50000000 } },
    "tenant_b": { "cardinality": 50000000, "distinct_keys": { "tenant": 1, "k": 2, "s": 50000000 } }
}';

SELECT '-- both inputs sorted by the key: merge join';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1 SELECT sum(sorted1.v + sorted2.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k)
WHERE explain LIKE '%Algorithm:%' OR explain LIKE '%Read type:%'
ORDER BY 1;

SELECT '-- neither input sorted: hash join';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1 SELECT sum(unsorted1.v + unsorted2.v) FROM unsorted1 JOIN unsorted2 ON unsorted1.k = unsorted2.k)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- sorted inputs, but a tiny build side: hash join';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1 SELECT sum(sorted1.v + tiny.v) FROM sorted1 JOIN tiny ON sorted1.k = tiny.k)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- a chain on one key: both joins merge, the result of the first one is not sorted again';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1
    SELECT sum(sorted1.v + sorted2.v + sorted3.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k JOIN sorted3 ON sorted2.k = sorted3.k)
WHERE explain LIKE '%Algorithm:%' OR explain LIKE '%Read type:%' OR explain LIKE '%Prefix sort description:%'
ORDER BY 1;

SELECT '-- a chain of four, in whatever tree shape: every input of every merge join is already sorted';
SELECT countIf(explain LIKE '%FullSortingMergeJoin%'), countIf(explain LIKE '%Sort % before JOIN%'), countIf(explain LIKE '%Prefix sort description:%')
FROM (EXPLAIN PLAN sorting = 1
    SELECT sum(sorted1.k + sorted2.k + sorted3.k + sorted4.k) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k
        JOIN sorted3 ON sorted2.k = sorted3.k JOIN sorted4 ON sorted3.k = sorted4.k);

SELECT '-- two large sorted tables and a tiny one: joining the tiny one first beats merging the large ones, no merge join';
SELECT countIf(explain LIKE '%Algorithm:%'), countIf(explain LIKE '%FullSortingMergeJoin%')
FROM (EXPLAIN PLAN
    SELECT sum(sorted1.v + sorted2.v + tiny.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k JOIN tiny ON sorted2.k = tiny.k)
SETTINGS enable_join_runtime_filters = 0;

SELECT '-- build keys in a narrow range: a hash join, whose runtime filter skips most of the sorted probe side';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN SELECT sum(probe.v + build_narrow.v) FROM probe JOIN build_narrow ON probe.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- build keys in a narrow range, runtime filters disabled: the hash join reads the whole probe, the sharded merge only its overlap';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN SELECT sum(probe.v + build_narrow.v) FROM probe JOIN build_narrow ON probe.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS enable_join_runtime_filters = 0;

SELECT '-- build keys in a narrow range, skip indexes off at read time: the runtime filter skips nothing, a merge join';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN SELECT sum(probe.v + build_narrow.v) FROM probe JOIN build_narrow ON probe.k = build_narrow.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS use_skip_indexes_on_data_read = 0;

SELECT '-- build keys spread over the probe range: the runtime filter skips nothing, a merge join';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN SELECT sum(probe.v + build_wide.v) FROM probe JOIN build_wide ON probe.k = build_wide.k)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- a probe side 50 times the build side: streaming it through the merge costs more than probing, a hash join';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN SELECT sum(probe.v + build_small.v) FROM probe JOIN build_small ON probe.k = build_small.k)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- floating-point keys: the merge cannot be sharded by key ranges, one stream loses to a hash join';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN SELECT count() FROM float_a JOIN float_b ON float_a.k = float_b.k)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- Nullable keys: the merge cannot be sharded by key ranges, one stream loses to a hash join';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN SELECT count() FROM nullable_a JOIN nullable_b ON nullable_a.k = nullable_b.k)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- sorted by the key, but an empty primary key: the merge cannot be sharded, one stream loses to a hash join';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN SELECT sum(no_pk_a.v + no_pk_b.v) FROM no_pk_a JOIN no_pk_b ON no_pk_a.k = no_pk_b.k)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- read in order by the key after a filter fixes the first primary key column, which the range split needs: a hash join';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT count() FROM (SELECT * FROM tenant_a WHERE tenant = 1) AS a JOIN (SELECT * FROM tenant_b WHERE tenant = 1) AS b ON a.k = b.k)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- joined on a monotonic function of the sorting key: read in order, but not split by key ranges, a hash join';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT count() FROM (SELECT intDiv(k, 2) AS q FROM sorted1) AS x JOIN (SELECT intDiv(k, 2) AS q FROM sorted2) AS y ON x.q = y.q)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- primary key on a constant first key only: one key range, one stream loses to a hash join';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT count() FROM short_pk_a JOIN short_pk_b ON short_pk_a.k = short_pk_b.k AND short_pk_a.s = short_pk_b.s)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- the same with the second key in the primary key: split by it, a merge join';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT count() FROM full_pk_a JOIN full_pk_b ON full_pk_a.k = full_pk_b.k AND full_pk_a.s = full_pk_b.s)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- sharding by primary key ranges disabled: one stream loses to a hash join';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN SELECT sum(sorted1.v + sorted2.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS query_plan_join_shard_by_pk_ranges = 0;

SELECT '-- two keys, sorted by a low-cardinality first key only: a hash join';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT count() FROM two_keys_a JOIN two_keys_b ON two_keys_a.k = two_keys_b.k AND two_keys_a.s = two_keys_b.s)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- two keys, sorted by both, written in another order: a merge join on the keys in sorting key order';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN actions = 1
    SELECT count() FROM two_sorted_a JOIN two_sorted_b ON two_sorted_a.s = two_sorted_b.s AND two_sorted_a.k = two_sorted_b.k)
WHERE explain LIKE '%Algorithm:%' OR explain LIKE '%Join conditions:%';

SELECT '-- two keys after a filter that fixes the first storage key column: sorted by the first join key only, a hash join';
SELECT if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join') FROM (EXPLAIN PLAN
    SELECT count() FROM (SELECT * FROM tenant_a WHERE tenant = 1) AS a JOIN (SELECT * FROM tenant_b WHERE tenant = 1) AS b ON a.k = b.k AND a.s = b.s)
WHERE explain LIKE '%Algorithm:%';

SELECT '-- without optimize_sorting_by_input_stream_properties a join result is sorted again, so the second join hashes';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1
    SELECT sum(sorted1.v + sorted2.v + sorted3.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k JOIN sorted3 ON sorted2.k = sorted3.k)
WHERE explain LIKE '%Algorithm:%'
ORDER BY 1
SETTINGS optimize_sorting_by_input_stream_properties = 0;

SELECT '-- join_algorithm set explicitly without full_sorting_merge: hash join';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1 SELECT sum(sorted1.v + sorted2.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS join_algorithm = 'parallel_hash';

SELECT '-- join_algorithm set explicitly in a subquery only: its join stays a hash join';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN
    SELECT * FROM (SELECT sum(sorted1.v + sorted2.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k SETTINGS join_algorithm = 'parallel_hash'))
WHERE explain LIKE '%Algorithm:%';

SELECT '-- join_algorithm set explicitly with full_sorting_merge as a fallback: the priority list is kept, a hash join';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1 SELECT sum(sorted1.v + sorted2.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS join_algorithm = 'parallel_hash,full_sorting_merge';

SELECT '-- join_algorithm set explicitly to its default: the optimizer still chooses, a merge join';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1 SELECT sum(sorted1.v + sorted2.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS join_algorithm = 'direct,parallel_hash,hash,ie_join';

SELECT '-- the setting is off: hash join';
SELECT if(explain LIKE '%Algorithm:%', if(explain LIKE '%FullSortingMergeJoin%', 'merge join', 'hash join'), replaceRegexpAll(explain, '^[│└├─ ]+', '')) FROM (EXPLAIN PLAN sorting = 1 SELECT sum(sorted1.v + sorted2.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k)
WHERE explain LIKE '%Algorithm:%'
SETTINGS query_plan_join_order_use_interesting_orders = 0;

SELECT '-- results';
SELECT count(), sum(sorted1.v + sorted2.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k;
SELECT count(), sum(sorted1.v + sorted2.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k
SETTINGS query_plan_join_order_use_interesting_orders = 0;
SELECT count(), sum(sorted1.v + sorted2.v + sorted3.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k JOIN sorted3 ON sorted2.k = sorted3.k;
SELECT count(), sum(sorted1.v + sorted2.v + sorted3.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k JOIN sorted3 ON sorted2.k = sorted3.k
SETTINGS query_plan_join_order_use_interesting_orders = 0;
SELECT count(), sum(sorted1.v + tiny.v) FROM sorted1 JOIN tiny ON sorted1.k = tiny.k;
SELECT count(), sum(probe.v + build_narrow.v) FROM probe JOIN build_narrow ON probe.k = build_narrow.k;
SELECT count(), sum(probe.v + build_wide.v) FROM probe JOIN build_wide ON probe.k = build_wide.k;
SELECT count(), sum(probe.v + build_small.v) FROM probe JOIN build_small ON probe.k = build_small.k;
SELECT count() FROM float_a JOIN float_b ON float_a.k = float_b.k;
SELECT count(), sum(nullable_a.v + nullable_b.v) FROM nullable_a JOIN nullable_b ON nullable_a.k = nullable_b.k;
SELECT count(), sum(no_pk_a.v + no_pk_b.v) FROM no_pk_a JOIN no_pk_b ON no_pk_a.k = no_pk_b.k;
SELECT count(), sum(short_pk_a.v + short_pk_b.v) FROM short_pk_a JOIN short_pk_b ON short_pk_a.k = short_pk_b.k AND short_pk_a.s = short_pk_b.s;
SELECT count(), sum(full_pk_a.v + full_pk_b.v) FROM full_pk_a JOIN full_pk_b ON full_pk_a.k = full_pk_b.k AND full_pk_a.s = full_pk_b.s;
SELECT count(), sum(two_keys_a.v + two_keys_b.v) FROM two_keys_a JOIN two_keys_b ON two_keys_a.k = two_keys_b.k AND two_keys_a.s = two_keys_b.s;
SELECT count(), sum(two_sorted_a.v + two_sorted_b.v) FROM two_sorted_a JOIN two_sorted_b ON two_sorted_a.s = two_sorted_b.s AND two_sorted_a.k = two_sorted_b.k;
SELECT count(), sum(sorted1.v + sorted2.v + sorted3.v + sorted4.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k
    JOIN sorted3 ON sorted2.k = sorted3.k JOIN sorted4 ON sorted3.k = sorted4.k;
SELECT count(), sum(sorted1.v + sorted2.v + sorted3.v + sorted4.v) FROM sorted1 JOIN sorted2 ON sorted1.k = sorted2.k
    JOIN sorted3 ON sorted2.k = sorted3.k JOIN sorted4 ON sorted3.k = sorted4.k
SETTINGS query_plan_join_order_use_interesting_orders = 0;

DROP TABLE sorted1;
DROP TABLE sorted2;
DROP TABLE sorted3;
DROP TABLE sorted4;
DROP TABLE unsorted1;
DROP TABLE unsorted2;
DROP TABLE tiny;
DROP TABLE probe;
DROP TABLE build_narrow;
DROP TABLE build_wide;
DROP TABLE build_small;
DROP TABLE float_a;
DROP TABLE float_b;
DROP TABLE nullable_a;
DROP TABLE nullable_b;
DROP TABLE no_pk_a;
DROP TABLE no_pk_b;
DROP TABLE short_pk_a;
DROP TABLE short_pk_b;
DROP TABLE full_pk_a;
DROP TABLE full_pk_b;
DROP TABLE two_keys_a;
DROP TABLE two_keys_b;
DROP TABLE two_sorted_a;
DROP TABLE two_sorted_b;
DROP TABLE tenant_a;
DROP TABLE tenant_b;
