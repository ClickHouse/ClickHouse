-- Tags: no-asan, no-tsan
-- With packed `String` keys, the memory spill scheduler should be able to spill a `SpillingHashJoin`, not just a top-level `grace_hash`.
create table spill_scheduler_05459_1 (`k` String, `x` String) Engine=Memory;
create table spill_scheduler_05459_2 (`k` String, `x` String) Engine=Memory;
create table spill_scheduler_05459_3 (`k` String, `x` String) Engine=Memory;

insert into spill_scheduler_05459_1 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);
insert into spill_scheduler_05459_2 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);
insert into spill_scheduler_05459_3 select cast(rand() as String) as k, cast(rand() as String) as x from numbers(1000000);

set max_threads=1;
set join_algorithm='hash';
set max_memory_usage=314572800;
set enable_parallel_replicas=0; -- parallel replicas distribute data across nodes, reducing per-node memory
set grace_hash_join_initial_buckets=1; -- more initial buckets split the right side, reducing per-bucket memory
set collect_hash_table_stats_during_joins=0;
set max_bytes_in_join=0;
-- The layout of 04695.
set hash_join_layout='serial';
-- Put the join's own spill threshold out of reach so that only the scheduler can make it spill.
set max_bytes_before_external_join='100Gi';
set max_bytes_ratio_before_external_join=0;
-- Packed `String` keys make the hash tables small enough for the first query to fit, unlike the exact keys of 04695.
set enable_packed_string_keys_in_hash_join = 1;
-- Random row estimates from `query_plan_optimize_join_order_randomize` can be below the default `min_rows_for_packed_string_keys_in_hash_join`.
set min_rows_for_packed_string_keys_in_hash_join = 0;

set enable_adaptive_memory_spill_scheduler=false;
select t1.k, t2.x, t3.x from spill_scheduler_05459_1 as t1 left join spill_scheduler_05459_2 as t2 on t1.k = t2.k left join spill_scheduler_05459_3 as t3 on t1.k = t3.k Format Null;

set enable_adaptive_memory_spill_scheduler=true;
select t1.k, t2.x, t3.x from spill_scheduler_05459_1 as t1 left join spill_scheduler_05459_2 as t2 on t1.k = t2.k left join spill_scheduler_05459_3 as t3 on t1.k = t3.k Format Null
settings log_comment = '05459_scheduler_spill';

-- The threshold above is out of reach, so any temporary file here was written because the scheduler
-- asked for it: moving the data into an unpartitioned `GraceHashJoin` alone would free nothing.
system flush logs query_log;
select ProfileEvents['ExternalJoinWritePart'] > 0 from system.query_log
where current_database = currentDatabase() and log_comment = '05459_scheduler_spill'
    and type = 'QueryFinish' and event_date >= yesterday();

drop table if exists spill_scheduler_05459_1;
drop table if exists spill_scheduler_05459_2;
drop table if exists spill_scheduler_05459_3;
