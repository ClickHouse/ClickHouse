-- Tags: no-fasttest, no-parallel
-- no-fasttest: needs a build with libfiu to enable the failpoint.
-- no-parallel: the failpoint is server-wide and fires once.

-- When the memory spill scheduler asks the join to spill, the switch to `GraceHashJoin` frees the
-- in-memory maps before it moves the blocks. If it throws after that, the build threads waiting
-- for it must get an exception. They must not insert into the freed maps.

SET enable_adaptive_memory_spill_scheduler = 1;
SET max_memory_usage = 400000000;
-- Put the join's own spill threshold out of reach so that only the scheduler can make it spill.
SET max_bytes_before_external_join = '100Gi';
SET max_bytes_ratio_before_external_join = 0;
-- Many build threads, so that some of them wait while the switch runs.
SET max_threads = 16;
SET parallel_hash_join_threshold = 0;
SET query_plan_join_swap_table = 'false';
SET enable_parallel_replicas = 0;

SYSTEM ENABLE FAILPOINT spilling_hash_join_throw_after_maps_release;

SELECT count() FROM (SELECT number AS k FROM numbers(1000)) AS l
    INNER JOIN (SELECT number AS k FROM numbers_mt(15000000)) AS r
    ON l.k = r.k; -- { serverError FAULT_INJECTED }

SYSTEM DISABLE FAILPOINT spilling_hash_join_throw_after_maps_release;
