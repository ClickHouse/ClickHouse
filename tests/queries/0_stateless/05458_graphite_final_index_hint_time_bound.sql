-- Graphite FINAL rolls up rows with different times, so a time bound inside indexHint must not prune either
SET optimize_on_insert = 0;

DROP TABLE IF EXISTS t_graphite_final_hint;

CREATE TABLE t_graphite_final_hint (Path String, Time DateTime('UTC'), Value Float64, Version UInt32)
ENGINE = GraphiteMergeTree('graphite_rollup') PARTITION BY toStartOfHour(Time) ORDER BY (Path, Time);

-- the first two rows fall into the 6000-second bucket that starts at 959996400, in different hour partitions
INSERT INTO t_graphite_final_hint VALUES ('sum_1', 960000100, 1, 960000150);
INSERT INTO t_graphite_final_hint VALUES ('sum_1', 960001300, 2, 960001350);
INSERT INTO t_graphite_final_hint VALUES ('sum_2', 960010000, 4, 960010050);

SELECT 'hint', Value, Version FROM t_graphite_final_hint FINAL WHERE Path = 'sum_1' AND indexHint(Time <= toDateTime(960001000, 'UTC'));

-- optimize_and_compare_chain derives toUInt32(Time) + 3600 < 960004000 as an indexHint
SELECT 'chain', Value, Version FROM t_graphite_final_hint FINAL WHERE toUInt32(Time) + 3600 < Version AND Version < 960004000 SETTINGS optimize_and_compare_chain = 1;
SELECT 'no chain', Value, Version FROM t_graphite_final_hint FINAL WHERE toUInt32(Time) + 3600 < Version AND Version < 960004000 SETTINGS optimize_and_compare_chain = 0;

-- a subquery column named like the path column is still the time column
SELECT 'chain alias', Value, Version FROM (SELECT Time AS Path, Value, Version FROM t_graphite_final_hint FINAL) WHERE toUInt32(Path) + 3600 < Version AND Version < 960004000 SETTINGS optimize_and_compare_chain = 1;

-- a hint on the path still prunes
SELECT 'path hint', Path, Value FROM t_graphite_final_hint FINAL WHERE indexHint(Path = 'sum_2') ORDER BY ALL;

DROP TABLE t_graphite_final_hint;
