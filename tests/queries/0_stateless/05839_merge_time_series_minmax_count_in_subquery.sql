-- Tags: distributed
-- Tag distributed: reads a Distributed table.

-- The Distributed child makes the Merge table run GROUP BY in every child. The TimeSeries child has neither
-- column, so its rows read as (0, 0), and its read is spliced into the plan only after the projections were
-- tried, so the set of the subquery is first needed when the child plan is optimized again.

SET enable_time_series_table = 1;

DROP TABLE IF EXISTS m;
DROP TABLE IF EXISTS ts;
DROP TABLE IF EXISTS d_dist;
DROP TABLE IF EXISTS d_local;
DROP TABLE IF EXISTS keys;

CREATE TABLE ts ENGINE = TimeSeries;
INSERT INTO ts (metric_name, tags, samples) VALUES ('m', map('k', 'v'), [(toDateTime64(100, 3), 1)]);

CREATE TABLE d_local (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY a;
CREATE TABLE d_dist AS d_local ENGINE = Distributed(test_shard_localhost, currentDatabase(), d_local);

CREATE TABLE keys (a UInt32, b UInt32) ENGINE = MergeTree ORDER BY (a, b);
INSERT INTO keys VALUES (1, 1);

CREATE TABLE m (a UInt32, b UInt32) ENGINE = Merge(currentDatabase(), '^(ts|d_dist)$');

SELECT a, b FROM m WHERE (a, b) IN (SELECT a, b FROM keys) GROUP BY a, b ORDER BY a, b
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT count() FROM m WHERE (a, b) IN (SELECT a, b FROM keys)
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;

INSERT INTO keys VALUES (0, 0);

SELECT a, b FROM m WHERE (a, b) IN (SELECT a, b FROM keys) GROUP BY a, b ORDER BY a, b
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;
SELECT count() FROM m WHERE (a, b) IN (SELECT a, b FROM keys)
SETTINGS optimize_use_projections = 1, optimize_use_implicit_projections = 1;

DROP TABLE m;
DROP TABLE ts;
DROP TABLE d_dist;
DROP TABLE d_local;
DROP TABLE keys;
