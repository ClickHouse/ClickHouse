-- one rollup group must not be split across FINAL layers, partitions or unmerged parts
SET optimize_on_insert = 0;
SET max_threads = 2;

DROP TABLE IF EXISTS t_graphite_final_layers;
DROP TABLE IF EXISTS t_graphite_final_partitions;

-- all rows below fall into the 6000-second bucket that starts at 959996400

CREATE TABLE t_graphite_final_layers (Path String, Time DateTime('UTC'), Value Float64, Version UInt32)
ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Path, Time) SETTINGS index_granularity = 1;

INSERT INTO t_graphite_final_layers VALUES ('sum_1', 960000100, 1, 1), ('sum_1', 960000150, 2, 1), ('sum_1', 960000200, 4, 1);

SELECT 'layers', Path, toUnixTimestamp(Time), Value, Version FROM t_graphite_final_layers FINAL ORDER BY ALL;
SELECT 'layers', Path, toUnixTimestamp(Time), Value, Version FROM t_graphite_final_layers FINAL ORDER BY ALL SETTINGS max_threads = 1;

-- the hour boundary at 960001200 puts the two rows into different partitions
CREATE TABLE t_graphite_final_partitions (Path String, Time DateTime('UTC'), Value Float64, Version UInt32)
ENGINE = GraphiteMergeTree('graphite_rollup') PARTITION BY toStartOfHour(Time) ORDER BY (Path, Time);

INSERT INTO t_graphite_final_partitions VALUES ('sum_1', 960000100, 1, 1);
INSERT INTO t_graphite_final_partitions VALUES ('sum_1', 960001300, 2, 2);

SELECT 'partitions', Path, toUnixTimestamp(Time), Value, Version FROM t_graphite_final_partitions FINAL ORDER BY ALL;

-- each partition now holds one merged part with a point at the rounded time; FINAL keeps the point with the higher version
OPTIMIZE TABLE t_graphite_final_partitions FINAL;

SELECT 'merged parts', Path, toUnixTimestamp(Time), Value, Version FROM t_graphite_final_partitions FINAL ORDER BY ALL;

-- the rollup reads its columns that the query does not select
SELECT 'merged parts value', Value FROM t_graphite_final_partitions FINAL;
SELECT 'merged parts value', Value FROM t_graphite_final_partitions FINAL ORDER BY Value SETTINGS do_not_merge_across_partitions_select_final = 1;

DROP TABLE t_graphite_final_layers;
DROP TABLE t_graphite_final_partitions;
