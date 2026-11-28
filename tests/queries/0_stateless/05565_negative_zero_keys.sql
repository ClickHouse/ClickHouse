-- `-0.` is equal to `0.`, so the operations based on hashing have to treat them as the same key.

SELECT 'IN';
SELECT materialize(-0.) IN (0.), materialize(0.) IN (-0.), -0. IN (0.), materialize(-0.) NOT IN (0.);
SELECT materialize(-0.) IN (SELECT 0.), materialize(0.) IN (SELECT -0.);
SELECT materialize(toFloat32(-0.)) IN (toFloat32(0.)), materialize(toBFloat16(-0.)) IN (toBFloat16(0.));
SELECT materialize(toNullable(-0.)) IN (0.), materialize(toLowCardinality(-0.)) IN (0.);
SELECT materialize((-0., 1)) IN ((0., 1)), materialize([-0.]) IN ([0.]), materialize(map(-0., 1)) IN (map(0., 1));
SELECT materialize(-0.) IN (0., 1.), materialize(1.) IN (-0., 1.);
SELECT x IN (0.) FROM (SELECT arrayJoin([0., -0., 1., nan]) AS x);

SELECT 'GROUP BY';
SELECT count() FROM (SELECT x FROM (SELECT arrayJoin([0., -0.]) AS x) GROUP BY x);
SELECT count() FROM (SELECT x FROM (SELECT toFloat32(arrayJoin([0., -0.])) AS x) GROUP BY x);
SELECT count() FROM (SELECT x FROM (SELECT toNullable(arrayJoin([0., -0.])) AS x) GROUP BY x);
SELECT count() FROM (SELECT x FROM (SELECT toLowCardinality(arrayJoin([0., -0.])) AS x) GROUP BY x);
SELECT count() FROM (SELECT x, y FROM (SELECT arrayJoin([0., -0.]) AS x, 'a' AS y) GROUP BY x, y);
SELECT count() FROM (SELECT x FROM (SELECT [arrayJoin([0., -0.])] AS x) GROUP BY x);
SELECT count() FROM (SELECT x FROM (SELECT (arrayJoin([0., -0.]), 1) AS x) GROUP BY x);
SELECT count() FROM (SELECT x FROM (SELECT number % 2 ? -0. : 0. AS x FROM numbers(100000)) GROUP BY x)
    SETTINGS group_by_two_level_threshold = 1, group_by_two_level_threshold_bytes = 1, max_threads = 4, max_block_size = 1000;
-- NaN keys are grouped by their binary representation, as before.
SELECT count() FROM (SELECT x FROM (SELECT arrayJoin([nan, nan]) AS x) GROUP BY x);

SELECT 'DISTINCT';
SELECT count() FROM (SELECT DISTINCT x FROM (SELECT arrayJoin([0., -0.]) AS x));
SELECT count() FROM (SELECT DISTINCT x, y FROM (SELECT arrayJoin([0., -0.]) AS x, 'a' AS y));
SELECT count() FROM (SELECT DISTINCT x FROM (SELECT toNullable(arrayJoin([0., -0.])) AS x));

SELECT 'LIMIT BY';
SELECT count() FROM (SELECT x FROM (SELECT arrayJoin([0., -0.]) AS x) LIMIT 1 BY x);

SELECT 'JOIN';
SELECT count() FROM (SELECT 0. AS x) AS a JOIN (SELECT -0. AS x) AS b USING (x) SETTINGS join_algorithm = 'hash';
SELECT count() FROM (SELECT -0. AS x) AS a JOIN (SELECT 0. AS x) AS b USING (x) SETTINGS join_algorithm = 'parallel_hash';
SELECT count() FROM (SELECT -0. AS x) AS a JOIN (SELECT 0. AS x) AS b USING (x) SETTINGS join_algorithm = 'grace_hash';
SELECT count() FROM (SELECT -0. AS x) AS a JOIN (SELECT 0. AS x) AS b USING (x) SETTINGS join_algorithm = 'full_sorting_merge';
SELECT count() FROM (SELECT toNullable(-0.) AS x, 1 AS y) AS a JOIN (SELECT 0. AS x, 1 AS y) AS b USING (x, y) SETTINGS join_algorithm = 'hash';

-- The query found by SQLancer: the projection and the filter have to agree.
SELECT 'SQLancer';
DROP TABLE IF EXISTS t_negative_zero_keys;
CREATE TABLE t_negative_zero_keys (c0 Int32, c2 Int32) ENGINE = MergeTree ORDER BY c0;
INSERT INTO t_negative_zero_keys VALUES (1, 0), (2, -1537351038), (3, 608612285), (4, -843134361);
SELECT count() FROM t_negative_zero_keys WHERE (atan(false) / c2) NOT IN (log10(1));
SELECT sum((atan(false) / c2) NOT IN (log10(1))) FROM t_negative_zero_keys;
DROP TABLE t_negative_zero_keys;
