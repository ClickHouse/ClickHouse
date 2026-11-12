-- `EXPLAIN PLAN fragments = 1, json = 1`: the query fragments of a plan distributed to stateless workers and the
-- exchanges between them, as one JSON document. The queries are only explained, never executed.

DROP TABLE IF EXISTS small;
DROP TABLE IF EXISTS big;

CREATE TABLE small(sid UInt64, s Array(Int64)) ENGINE = MergeTree ORDER BY sid;
CREATE TABLE big(bid UInt64, b Array(Int64)) ENGINE = MergeTree ORDER BY bid;

INSERT INTO small SELECT number, [number] FROM numbers(0, 1000);
INSERT INTO big SELECT number, [number] FROM numbers(0, 100000);

-- The same settings as `05339_explain_plan_fragments`, which the test harness would otherwise randomize.
SET explain_query_plan_default = 'legacy';
SET query_plan_join_swap_table = 0;
SET max_rows_to_group_by = 0;
SET distributed_aggregation_memory_efficient = 1, enable_memory_bound_merging_of_aggregation_results = 0, optimize_aggregation_in_order = 0;
SET join_runtime_filter_min_probe_rows = 0, query_plan_optimize_join_order_randomize = 0, use_hash_table_stats_for_join_reordering = 0;
SET distributed_plan_prefer_replicas_over_workers = 1;

SET
    make_distributed_plan = 1,
    enable_parallel_replicas = 0,
    enable_join_runtime_filters = 0,
    use_statistics = 0,
    distributed_plan_optimize_exchanges = 1,
    distributed_plan_default_shuffle_join_bucket_count = 3,
    distributed_plan_default_reader_bucket_count = 3,
    distributed_plan_force_exchange_kind = 'Streaming';

SELECT '-- aggregation, the whole document';
EXPLAIN fragments = 1, json = 1
SELECT bid % 10 AS k, sum(bid)
FROM big
GROUP BY k
FORMAT TSVRaw;

SELECT '-- order by: the sort order of the gather';
SELECT JSONExtractRaw(explain, 1, 'Exchanges') FROM (
    EXPLAIN fragments = 1, json = 1
    SELECT bid
    FROM big
    ORDER BY bid DESC)
FORMAT TSVRaw;

SELECT '-- persisted';
SELECT JSONExtractString(explain, 1, 'Exchanges', 1, 'Mode') FROM (
    EXPLAIN fragments = 1, json = 1
    SELECT bid % 10 AS k, sum(bid)
    FROM big
    GROUP BY k
    SETTINGS distributed_plan_force_exchange_kind = 'Persisted');

SELECT '-- shuffle join: levels, exchanges and an exchange id on both ends of every exchange';
SELECT
    JSONExtract(explain, 1, 'Levels', 'Array(Array(UInt32))'),
    JSONExtract(explain, 1, 'Exchanges', 'Array(Tuple(Id UInt32, Name String, From UInt32, To UInt32, Kind String))'),
    length(JSONExtractArrayRaw(explain, 1, 'Fragments')),
    countSubstrings(JSONExtractRaw(explain, 1, 'Fragments'), '"Exchange Id"') = 2 * length(JSONExtractArrayRaw(explain, 1, 'Exchanges'))
FROM (
    EXPLAIN fragments = 1, json = 1
    SELECT small.sid, count()
    FROM big, small
    WHERE small.sid = (big.bid + 1) % 5000
    GROUP BY small.sid
    SETTINGS distributed_plan_max_rows_to_broadcast = 0);

SELECT '-- the result fragment: no nested plan, its root sends the result to the client';
SELECT
    JSONHas(explain, 1, 'Plan'),
    JSONExtractUInt(explain, 1, 'Result Fragment') = JSONExtractUInt(explain, 1, 'Fragments', -1, 'Id'),
    JSONExtractString(explain, 1, 'Fragments', -1, 'Steps', 1, 'Node Type'),
    JSONExtractString(explain, 1, 'Fragments', -1, 'Steps', 1, 'Description'),
    JSONHas(explain, 1, 'Fragments', -1, 'Steps', 1, 'Exchange Id'),
    JSONHas(explain, 1, 'Fragments', -1, 'Steps', 1, 'Plans')
FROM (
    EXPLAIN fragments = 1, json = 1
    SELECT bid % 10 AS k, sum(bid)
    FROM big
    GROUP BY k);

SELECT '-- flat steps: the root is the first step, and every other step is the input of exactly one step of its fragment';
SELECT
    fragment_id,
    length(step_ids),
    root = step_ids[1],
    arraySort(arrayFlatten(children)) = arraySort(arrayPopFront(step_ids))
FROM
(
    SELECT
        JSONExtractUInt(fragment, 'Id') AS fragment_id,
        JSONExtractString(fragment, 'Root') AS root,
        arrayMap(step -> JSONExtractString(step, 'Node Id'), JSONExtractArrayRaw(fragment, 'Steps')) AS step_ids,
        arrayMap(step -> JSONExtract(step, 'Children', 'Array(String)'), JSONExtractArrayRaw(fragment, 'Steps')) AS children
    FROM (
        EXPLAIN fragments = 1, json = 1
        SELECT small.sid, count()
        FROM big, small
        WHERE small.sid = (big.bid + 1) % 5000
        GROUP BY small.sid
        SETTINGS distributed_plan_max_rows_to_broadcast = 0)
    ARRAY JOIN JSONExtractArrayRaw(explain, 1, 'Fragments') AS fragment
)
ORDER BY fragment_id;

SELECT '-- flat steps: the inputs of a join in order, the left side first';
SELECT
    JSONExtractString(step, 'Node Type'),
    arrayMap(id -> (JSONExtractString(steps[indexOf(ids, id)], 'Node Type'), JSONExtractUInt(steps[indexOf(ids, id)], 'Exchange Id')),
        JSONExtract(step, 'Children', 'Array(String)'))
FROM
(
    SELECT
        arrayFlatten(arrayMap(fragment -> JSONExtractArrayRaw(fragment, 'Steps'), JSONExtractArrayRaw(explain, 1, 'Fragments'))) AS steps,
        arrayMap(step -> JSONExtractString(step, 'Node Id'), steps) AS ids
    FROM (
        EXPLAIN fragments = 1, json = 1
        SELECT small.sid, count()
        FROM big, small
        WHERE small.sid = (big.bid + 1) % 5000
        GROUP BY small.sid
        SETTINGS distributed_plan_max_rows_to_broadcast = 0)
)
ARRAY JOIN steps AS step
WHERE JSONExtractString(step, 'Node Type') LIKE '%Join%';

SELECT '-- estimated rows only with estimates = 1';
SELECT countSubstrings(explain, '"Estimated Rows"') FROM (
    EXPLAIN fragments = 1, json = 1
    SELECT bid % 10 AS k, sum(bid)
    FROM big
    GROUP BY k);

SELECT '-- make_distributed_plan = 0';
SELECT JSONHas(explain, 1, 'Plan'), JSONHas(explain, 1, 'Plan', 'Plans'), JSONHas(explain, 1, 'Fragments'), JSONHas(explain, 1, 'Fallback Reason') FROM (
    EXPLAIN fragments = 1, json = 1
    SELECT bid % 10 AS k, sum(bid)
    FROM big
    GROUP BY k
    SETTINGS make_distributed_plan = 0);

SELECT '-- fallback to local execution';
SELECT JSONHas(explain, 1, 'Fragments'), JSONExtractString(explain, 1, 'Fallback Reason') FROM (
    EXPLAIN fragments = 1, json = 1
    SELECT bid % 10 AS k, sum(bid)
    FROM big
    GROUP BY k WITH TOTALS
    SETTINGS distributed_plan_fallback_to_local_execution = 1);

SELECT '-- errors';
EXPLAIN fragments = 1, json = 1, distributed = 1 SELECT count() FROM big; -- { serverError NOT_IMPLEMENTED }
EXPLAIN fragments = 1, json = 1, optimize = 0 SELECT count() FROM big; -- { serverError BAD_ARGUMENTS }
EXPLAIN tree = 1, json = 1 SELECT count() FROM big; -- { serverError NOT_IMPLEMENTED }

DROP TABLE small;
DROP TABLE big;
