-- ARRAY JOIN in every mode returns the rows of arrayJoin over the zipped (padded or resized) arrays

DROP TABLE IF EXISTS t_aj_window;
CREATE TABLE t_aj_window (id UInt64, a Array(String), b Array(UInt64), c Array(Nullable(String)), m Map(String, UInt64), r Array(UInt8))
ENGINE = MergeTree ORDER BY id;
INSERT INTO t_aj_window SELECT
    number,
    arrayMap(i -> concat('a', toString(number), '_', toString(i)), range(number % 5)),
    arrayMap(i -> number * 100 + i, range(number % 5)),
    arrayMap(i -> if(i % 2 = 0, NULL, concat('c', toString(number + i))), range(number % 5)),
    mapFromArrays(arrayMap(i -> concat('k', toString(i)), range(number % 5)), arrayMap(i -> number + i, range(number % 5))),
    arrayMap(i -> toUInt8(i + 1), range(number % 3))
FROM numbers(300);

-- small windows, so most windows start inside a block; arrayJoin() stays in the expression; the filter is fused
SET max_block_size = 7, query_plan_lower_array_join_function = 0, query_plan_fuse_filter_into_array_join = 1, serialize_query_plan = 0, make_distributed_plan = 0, enable_parallel_replicas = 0;

SELECT (SELECT count(), sum(cityHash64(tuple(id, x, y, z, k.1, k.2))) FROM t_aj_window LEFT ARRAY JOIN a AS x, b AS y, c AS z, m AS k)
     = (SELECT count(), sum(cityHash64(tuple(id, e.1, e.2, e.3, e.4, e.5))) FROM (SELECT id, arrayJoin(arrayZip(emptyArrayToSingle(a), emptyArrayToSingle(b), emptyArrayToSingle(c), emptyArrayToSingle(mapKeys(m)), emptyArrayToSingle(mapValues(m)))) AS e FROM t_aj_window));
SELECT (SELECT count(), sum(cityHash64(tuple(id, z, x))) FROM t_aj_window LEFT ARRAY JOIN c AS z, a AS x)
     = (SELECT count(), sum(cityHash64(tuple(id, e.1, e.2))) FROM (SELECT id, arrayJoin(arrayZip(emptyArrayToSingle(c), emptyArrayToSingle(a))) AS e FROM t_aj_window));
SELECT (SELECT count(), sum(cityHash64(tuple(id, x, y, z, k.1, k.2))) FROM t_aj_window ARRAY JOIN a AS x, b AS y, c AS z, m AS k)
     = (SELECT count(), sum(cityHash64(tuple(id, e.1, e.2, e.3, e.4, e.5))) FROM (SELECT id, arrayJoin(arrayZip(a, b, c, mapKeys(m), mapValues(m))) AS e FROM t_aj_window));
SELECT (SELECT count(), sum(cityHash64(tuple(id, x, y))) FROM t_aj_window ARRAY JOIN a AS x, r AS y SETTINGS enable_unaligned_array_join = 1)
     = (SELECT count(), sum(cityHash64(tuple(id, e.1, e.2))) FROM (SELECT id, arrayJoin(arrayZip(arrayResize(a, greatest(length(a), length(r))), arrayResize(r, greatest(length(a), length(r))))) AS e FROM t_aj_window));
SELECT (SELECT count(), sum(cityHash64(tuple(id, x, y))) FROM t_aj_window LEFT ARRAY JOIN a AS x, r AS y SETTINGS enable_unaligned_array_join = 1)
     = (SELECT count(), sum(cityHash64(tuple(id, e.1, e.2))) FROM (SELECT id, arrayJoin(arrayZip(arrayResize(a, greatest(length(a), length(r), 1)), arrayResize(r, greatest(length(a), length(r), 1)))) AS e FROM t_aj_window));

-- fused element filter
SELECT count() > 0 FROM (EXPLAIN actions = 1 SELECT id, x, y FROM t_aj_window LEFT ARRAY JOIN a AS x, b AS y, c AS z WHERE y % 3 != 1) WHERE explain ILIKE '%Element filter column%';
SELECT (SELECT count(), sum(cityHash64(tuple(id, x, y, z))) FROM t_aj_window LEFT ARRAY JOIN a AS x, b AS y, c AS z WHERE y % 3 != 1)
     = (SELECT count(), sum(cityHash64(tuple(id, e.1, e.2, e.3))) FROM (SELECT id, arrayJoin(arrayZip(emptyArrayToSingle(a), emptyArrayToSingle(b), emptyArrayToSingle(c))) AS e FROM t_aj_window) WHERE e.2 % 3 != 1);
SELECT (SELECT count(), sum(cityHash64(tuple(id, x, y))) FROM t_aj_window LEFT ARRAY JOIN a AS x, r AS y WHERE y != 2 SETTINGS enable_unaligned_array_join = 1)
     = (SELECT count(), sum(cityHash64(tuple(id, e.1, e.2))) FROM (SELECT id, arrayJoin(arrayZip(arrayResize(a, greatest(length(a), length(r), 1)), arrayResize(r, greatest(length(a), length(r), 1)))) AS e FROM t_aj_window) WHERE e.2 != 2);

-- joined arrays lazily replicated by a previous ARRAY JOIN
SELECT (SELECT count(), sum(cityHash64(tuple(id, w, x, y))) FROM t_aj_window ARRAY JOIN r AS w LEFT ARRAY JOIN a AS x, b AS y SETTINGS enable_lazy_columns_replication = 1)
     = (SELECT count(), sum(cityHash64(tuple(id, w, e.1, e.2))) FROM (SELECT id, arrayJoin(r) AS w, arrayJoin(arrayZip(emptyArrayToSingle(a), emptyArrayToSingle(b))) AS e FROM t_aj_window));

SELECT x, y FROM (SELECT [1, 2] AS p, [3] AS q) LEFT ARRAY JOIN p AS x, q AS y; -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE t_aj_window;
