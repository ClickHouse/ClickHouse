-- `-0.` and `0.` are the same key of `GROUP BY`, `DISTINCT` and `LIMIT BY`, but a partition key can put them
-- into different partitions, so the reduction over partitions must not be skipped.

DROP TABLE IF EXISTS nz;
CREATE TABLE nz (f Float64) ENGINE = MergeTree PARTITION BY bitShiftRight(reinterpretAsUInt64(f), 63) ORDER BY tuple();
INSERT INTO nz SELECT arrayJoin([0., -0.]);

SELECT 'GROUP BY';
SELECT abs(f), count() FROM nz GROUP BY f
SETTINGS allow_aggregate_partitions_independently = 1, force_aggregate_partitions_independently = 1, optimize_aggregation_in_order = 0, max_threads = 2;
SELECT abs(f), count() FROM nz GROUP BY f
SETTINGS allow_aggregate_partitions_independently = 0, max_threads = 2;

SELECT 'DISTINCT';
SELECT count() FROM (SELECT DISTINCT f FROM nz SETTINGS allow_distinct_partitions_independently = 1, force_distinct_partitions_independently = 1, max_threads = 2);
SELECT count() FROM (SELECT DISTINCT f FROM nz SETTINGS allow_distinct_partitions_independently = 0, max_threads = 2);

SELECT 'LIMIT BY';
SELECT count() FROM (SELECT f FROM nz LIMIT 1 BY f SETTINGS allow_limit_by_partitions_independently = 1, max_threads = 2);
SELECT count() FROM (SELECT f FROM nz LIMIT 1 BY f SETTINGS allow_limit_by_partitions_independently = 0, max_threads = 2);

DROP TABLE nz;
