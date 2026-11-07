-- `estimateCompressionRatio` is created from the argument type as written, `LowCardinality` included. A server that receives a
-- serialized query plan creates the function from the argument columns as well, not from the plain types the function
-- declares, so a remote shard estimates the column as it is stored, as the local server does.

SET max_threads = 1;

DROP TABLE IF EXISTS t_ecr_plan;
CREATE TABLE t_ecr_plan (k UInt8, s String, lc LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_ecr_plan SELECT number % 2, toString(number % 16), toString(number % 16) FROM numbers(30000);

SELECT
    (SELECT estimateCompressionRatio('LZ4')(lc) FROM remote('127.0.0.2', currentDatabase(), t_ecr_plan))
        = (SELECT estimateCompressionRatio('LZ4')(lc) FROM t_ecr_plan) AS low_cardinality,
    (SELECT estimateCompressionRatio('LZ4', 'String')(lc) FROM remote('127.0.0.2', currentDatabase(), t_ecr_plan))
        = (SELECT estimateCompressionRatio('LZ4', 'String')(lc) FROM t_ecr_plan) AS target_type,
    (SELECT estimateCompressionRatioIf('LZ4')(lc, k = 0) FROM remote('127.0.0.2', currentDatabase(), t_ecr_plan))
        = (SELECT estimateCompressionRatioIf('LZ4')(lc, k = 0) FROM t_ecr_plan) AS combinator,
    (SELECT estimateCompressionRatio('LZ4')(s) FROM remote('127.0.0.2', currentDatabase(), t_ecr_plan))
        = (SELECT estimateCompressionRatio('LZ4')(s) FROM t_ecr_plan) AS plain
SETTINGS serialize_query_plan = 1;

SELECT k, r_remote = r_local AS low_cardinality_by_key
FROM (SELECT k, estimateCompressionRatio('LZ4')(lc) AS r_remote FROM remote('127.0.0.2', currentDatabase(), t_ecr_plan) GROUP BY k) AS remote_estimates
INNER JOIN (SELECT k, estimateCompressionRatio('LZ4')(lc) AS r_local FROM t_ecr_plan GROUP BY k) AS local_estimates USING (k)
ORDER BY k
SETTINGS serialize_query_plan = 1;

-- The query is sent as text: the remote server analyzes it and creates the function from the column types itself.
SELECT
    (SELECT estimateCompressionRatio('LZ4')(lc) FROM remote('127.0.0.2', currentDatabase(), t_ecr_plan))
        = (SELECT estimateCompressionRatio('LZ4')(lc) FROM t_ecr_plan) AS low_cardinality
SETTINGS serialize_query_plan = 0;

DROP TABLE t_ecr_plan;
