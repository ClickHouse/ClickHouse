-- Tags: no-asan, no-tsan
-- `03277_join_adaptive_spill_oom` with packed `String` keys. They make the hash tables smaller, so this test sets a lower
-- memory limit: the first query must still exceed it, and the second must still fit by spilling.
create table adaptive_spill_05458_1 (`k` String, `x` String ) Engine=Memory;
create table adaptive_spill_05458_2 (`k` String, `x` String ) Engine=Memory;
create table adaptive_spill_05458_3 (`k` String, `x` String ) Engine=Memory;

insert into adaptive_spill_05458_1 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);
insert into adaptive_spill_05458_2 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);
insert into adaptive_spill_05458_3 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);

set max_threads=1;
set join_algorithm='grace_hash';
set max_memory_usage=241172480;
set enable_parallel_replicas=0; -- parallel replicas distribute data across nodes, reducing per-node memory and preventing the expected OOM
set grace_hash_join_initial_buckets=1; -- more initial buckets split the right side, reducing per-bucket memory and preventing the expected OOM
set collect_hash_table_stats_during_joins=0;
-- don't limit the memory usage for join
set max_bytes_in_join=0;
-- keep the join's own spill threshold out of reach, so that only the scheduler can trigger spilling
set max_bytes_before_external_join='100Gi';
set max_bytes_ratio_before_external_join=0;
set enable_packed_string_keys_in_hash_join = 1;
-- Random row estimates from `query_plan_optimize_join_order_randomize` can be below the default `min_rows_for_packed_string_keys_in_hash_join`.
set min_rows_for_packed_string_keys_in_hash_join = 0;

set enable_adaptive_memory_spill_scheduler=false;
select t1.k, t2.x, t3.x from adaptive_spill_05458_1 as t1 left join adaptive_spill_05458_2 as t2 on t1.k = t2.k left join adaptive_spill_05458_3 as t3 on t1.k = t3.k Format Null; --{serverError MEMORY_LIMIT_EXCEEDED}

set enable_adaptive_memory_spill_scheduler=true;
select t1.k, t2.x, t3.x from adaptive_spill_05458_1 as t1 left join adaptive_spill_05458_2 as t2 on t1.k = t2.k left join adaptive_spill_05458_3 as t3 on t1.k = t3.k Format Null;

drop table if exists adaptive_spill_05458_1;
drop table if exists adaptive_spill_05458_2;
drop table if exists adaptive_spill_05458_3;
