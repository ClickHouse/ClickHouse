-- GraphiteMergeTree FINAL merges rows of one path with different times, so a condition on the time must run after FINAL

SET optimize_on_insert = 0;
SET optimize_move_to_prewhere = 1;
SET optimize_move_to_prewhere_if_final = 1;

DROP TABLE IF EXISTS t_graphite_final_prewhere;

-- keep one granule, so that the primary key analysis cannot drop a row of the bucket
CREATE TABLE t_graphite_final_prewhere (Path String, Time DateTime('UTC'), Value Float64, Version UInt32)
ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Path, Time) SETTINGS index_granularity = 8192;

-- the rollup puts both rows into the 6000-second bucket that starts at 959996400
INSERT INTO t_graphite_final_prewhere VALUES ('sum_1', 960000100, 1, 1), ('sum_1', 960000200, 2, 1);

SELECT 'ge', toUnixTimestamp(Time), Value, Version FROM t_graphite_final_prewhere FINAL WHERE Time >= toDateTime(960000150, 'UTC');
SELECT 'le', toUnixTimestamp(Time), Value, Version FROM t_graphite_final_prewhere FINAL WHERE Time <= toDateTime(960000150, 'UTC');

-- the path condition moves to PREWHERE and the time condition stays in WHERE
SELECT trim(explain) FROM (
    EXPLAIN actions = 1 SELECT Value FROM t_graphite_final_prewhere FINAL WHERE Path = 'sum_1' AND Time <= toDateTime(960000150, 'UTC')
) WHERE explain ILIKE '%Prewhere filter column%';

DROP TABLE t_graphite_final_prewhere;

-- the path is not the first sorting key column, so a path filter before FINAL would make the two sum_1 rows adjacent
CREATE TABLE t_graphite_final_prewhere (Path String, Time DateTime('UTC'), Value Float64, Version UInt32)
ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Time, Path) SETTINGS index_granularity = 8192;

INSERT INTO t_graphite_final_prewhere VALUES ('sum_1', 960000100, 1, 1), ('sum_2', 960000150, 5, 1), ('sum_1', 960000200, 2, 1);

SELECT 'path', toUnixTimestamp(Time), Value, Version FROM t_graphite_final_prewhere FINAL WHERE Path = 'sum_1' ORDER BY ALL;

DROP TABLE t_graphite_final_prewhere;
