-- the FINAL merge needs the rollup columns even when the query does not select them
DROP TABLE IF EXISTS t_graphite_final_columns;

CREATE TABLE t_graphite_final_columns (Path String, Time DateTime('UTC'), Value Float64, Version UInt32)
ENGINE = GraphiteMergeTree('graphite_rollup') ORDER BY (Path, Time);

INSERT INTO t_graphite_final_columns SETTINGS optimize_on_insert = 0 VALUES ('sum_1', 960000100, 1, 1), ('sum_1', 960000200, 2, 1), ('max_1', 960000100, 3, 1), ('max_1', 960000200, 4, 1);

SELECT count() FROM t_graphite_final_columns FINAL;
SELECT Path, Value FROM t_graphite_final_columns FINAL ORDER BY ALL;
SELECT Path FROM t_graphite_final_columns FINAL ORDER BY ALL;
SELECT Path FROM t_graphite_final_columns FINAL WHERE Time > 0 ORDER BY ALL;
SELECT count() FROM t_graphite_final_columns FINAL PREWHERE Time > 0;
SELECT count() FROM t_graphite_final_columns FINAL PREWHERE Value > 1;
SELECT Path, Value FROM t_graphite_final_columns FINAL ORDER BY ALL SETTINGS do_not_merge_across_partitions_select_final = 1;

DROP TABLE t_graphite_final_columns;
