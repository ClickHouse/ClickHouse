-- Tests `HierarchicalKMeansData::merge`, which combines two partial `hierarchicalKMeans` reservoirs.
-- The merge has four shapes, picked by how much each side had already seen relative to `sample_cap`:
-- keep everything, replay the side that never dropped a row, adopt the other reservoir, or subsample
-- both. A small `sample_cap` is what makes the last three reachable at all; with the default
-- 1000000 nothing is ever dropped and only the first shape runs.
-- Behaviour source: the `hierarchicalKMeans` FunctionDocumentation in
-- src/AggregateFunctions/AggregateFunctionHierarchicalKMeans.cpp. "Repeated points still yield k":
-- every row below is the same vector, so the trained centroid is that vector no matter which rows
-- survived the reservoir draw, which makes the asserted result independent of the sampling RNG,
-- of the merge order and of settings.

SET max_threads = 1; -- a single merge chain, so the states arrive in the order the subqueries fix

DROP TABLE IF EXISTS kmeans_rows;
DROP TABLE IF EXISTS kmeans_states;

CREATE TABLE kmeans_rows (id UInt8, v Array(Float64)) ENGINE = Memory;
-- Rows per group, against `sample_cap` = 4: groups 1, 3, 4 and 5 overflow the reservoir and drop
-- rows, group 2 fills it exactly without dropping any, group 6 has a different dimension.
INSERT INTO kmeans_rows SELECT 1, [1.0, 2.0] FROM numbers(10);
INSERT INTO kmeans_rows SELECT 2, [1.0, 2.0] FROM numbers(4);
INSERT INTO kmeans_rows SELECT 3, [1.0, 2.0] FROM numbers(10);
INSERT INTO kmeans_rows SELECT 4, [1.0, 2.0] FROM numbers(10);
INSERT INTO kmeans_rows SELECT 5, [1.0, 2.0] FROM numbers(10);
INSERT INTO kmeans_rows SELECT 6, [1.0, 2.0, 3.0] FROM numbers(10);

CREATE TABLE kmeans_states (id UInt8, s AggregateFunction(hierarchicalKMeans(1, 16, 20, 4), Array(Float64)))
ENGINE = Memory;
INSERT INTO kmeans_states SELECT id, hierarchicalKMeansState(1, 16, 20, 4)(v) FROM kmeans_rows GROUP BY id;
-- Group 0 aggregates no rows at all, which leaves `dim` at zero - the same value that marks a state
-- that has not seen anything yet.
INSERT INTO kmeans_states SELECT 0, hierarchicalKMeansState(1, 16, 20, 4)(v) FROM kmeans_rows WHERE id = 7;

SELECT '-- merging the partial states yields the same centroid as aggregating the rows directly';
SELECT hierarchicalKMeans(1, 16, 20, 4)(v) FROM kmeans_rows WHERE id IN (1, 3, 4, 5);
SELECT hierarchicalKMeansMerge(1, 16, 20, 4)(s)
FROM (SELECT s FROM kmeans_states WHERE id IN (1, 3, 4, 5) ORDER BY id ASC LIMIT 4);

SELECT '-- an empty state contributes nothing, on either side of the merge';
SELECT hierarchicalKMeansMerge(1, 16, 20, 4)(s)
FROM (SELECT s FROM kmeans_states WHERE id IN (0, 1) ORDER BY id ASC LIMIT 2);
SELECT hierarchicalKMeansMerge(1, 16, 20, 4)(s)
FROM (SELECT s FROM kmeans_states WHERE id IN (0, 1) ORDER BY id DESC LIMIT 2);

SELECT '-- a side that never dropped a row is replayed into the other reservoir, in either direction';
SELECT hierarchicalKMeansMerge(1, 16, 20, 4)(s)
FROM (SELECT s FROM kmeans_states WHERE id IN (1, 2) ORDER BY id ASC LIMIT 2);
SELECT hierarchicalKMeansMerge(1, 16, 20, 4)(s)
FROM (SELECT s FROM kmeans_states WHERE id IN (1, 2) ORDER BY id DESC LIMIT 2);

SELECT '-- states of different dimension cannot be merged';
SELECT hierarchicalKMeansMerge(1, 16, 20, 4)(s)
FROM (SELECT s FROM kmeans_states WHERE id IN (1, 6) ORDER BY id ASC LIMIT 2); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }

DROP TABLE kmeans_states;
DROP TABLE kmeans_rows;
