-- GraphiteMergeTree FINAL rolls up rows of one path with different times, so a time condition must run after FINAL

SET optimize_on_insert = 0;
SET optimize_move_to_prewhere = 1;
SET optimize_move_to_prewhere_if_final = 1;
SET apply_prewhere_after_final = 0;
SET query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS t_graphite_final_prewhere;

-- one granule, so primary key analysis cannot drop a row of the bucket
CREATE TABLE t_graphite_final_prewhere (Path String, Time DateTime('UTC'), Value Float64, Version UInt32)
ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Path, Time) SETTINGS index_granularity = 8192;

-- both rows fall into the 6000-second bucket starting at 959996400
INSERT INTO t_graphite_final_prewhere VALUES ('sum_1', 960000100, 1, 1), ('sum_1', 960000200, 2, 1);

SELECT 'all', Path, toUnixTimestamp(Time), Value, Version FROM t_graphite_final_prewhere FINAL ORDER BY ALL;
SELECT 'ge', Path, toUnixTimestamp(Time), Value, Version FROM t_graphite_final_prewhere FINAL WHERE Time >= toDateTime(960000150, 'UTC') ORDER BY ALL;
SELECT 'le', Path, toUnixTimestamp(Time), Value, Version FROM t_graphite_final_prewhere FINAL WHERE Time <= toDateTime(960000150, 'UTC') ORDER BY ALL;
SELECT 'path', Path, toUnixTimestamp(Time), Value, Version FROM t_graphite_final_prewhere FINAL WHERE Path = 'sum_1' AND Time <= toDateTime(960000150, 'UTC') ORDER BY ALL;

-- the path condition still moves, the time condition stays in WHERE
SELECT trim(explain) FROM (
    EXPLAIN actions = 1 SELECT Value FROM t_graphite_final_prewhere FINAL WHERE Path = 'sum_1' AND Time <= toDateTime(960000150, 'UTC')
) WHERE explain ILIKE '%Prewhere filter column%';

DROP TABLE t_graphite_final_prewhere;
