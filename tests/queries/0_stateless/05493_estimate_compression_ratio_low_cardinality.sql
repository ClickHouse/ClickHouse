-- Tags: no-random-merge-tree-settings
-- no-random-merge-tree-settings: the estimates are compared with the sizes of parts written with a fixed granularity and block size.

-- `estimateCompressionRatio` knows that its argument is `LowCardinality` and estimates its dictionary encoding, with one
-- compressed stream per substream, as MergeTree writes a wide part.

SET max_threads = 1, max_block_size = 8192, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

DROP TABLE IF EXISTS t_ecr_lc;
DROP TABLE IF EXISTS t_ecr_lc_parts;

-- One part of three granules; the keys interleave, so GROUP BY adds the rows one at a time.
CREATE TABLE t_ecr_lc (k UInt32, s String CODEC(LZ4), lc LowCardinality(String) CODEC(LZ4))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0,
         min_compress_block_size = 65536, max_compress_block_size = 65536, ratio_of_defaults_for_sparse_serialization = 1,
         string_serialization_version = 'single_stream';

INSERT INTO t_ecr_lc SELECT number % 3, concat('value_', toString(number % 16)), concat('value_', toString(number % 16)) FROM numbers(3 * 8192);

-- The same rows, one part per key, each of one granule: what a group of GROUP BY would be on disk.
CREATE TABLE t_ecr_lc_parts (k UInt32, s String CODEC(LZ4), lc LowCardinality(String) CODEC(LZ4))
ENGINE = MergeTree PARTITION BY k ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0,
         min_compress_block_size = 65536, max_compress_block_size = 65536, ratio_of_defaults_for_sparse_serialization = 1,
         string_serialization_version = 'single_stream';

INSERT INTO t_ecr_lc_parts SELECT * FROM t_ecr_lc;

SELECT 'Estimate of a part';

SELECT
    column,
    column_data_compressed_bytes = toUInt64(round(column_data_uncompressed_bytes / ratio)) AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN
(
    SELECT 'lc' AS column, estimateCompressionRatio('LZ4', 65536)(lc) AS ratio FROM t_ecr_lc
    UNION ALL
    SELECT 's' AS column, estimateCompressionRatio('LZ4', 65536)(s) AS ratio FROM t_ecr_lc
) AS estimates USING (column)
WHERE database = currentDatabase() AND table = 't_ecr_lc' AND active
ORDER BY column;

SELECT 'A converted argument is estimated as the target type';

SELECT
    estimateCompressionRatio('LZ4', 65536)(lc) = estimateCompressionRatio('LZ4', 65536)(toLowCardinality(s)),
    estimateCompressionRatio('LZ4', 65536)(lc) = estimateCompressionRatio('LZ4', 65536)(CAST(s, 'LowCardinality(String)')),
    estimateCompressionRatio('LZ4', 65536)(lc) != estimateCompressionRatio('LZ4', 65536)(s),
    estimateCompressionRatio('LZ4', 65536)(s) = estimateCompressionRatio('LZ4', 65536)(CAST(lc, 'String'))
FROM t_ecr_lc;

SELECT 'The rows of a block are serialized one granule at a time, whatever the block size';

SELECT
    (SELECT estimateCompressionRatio('LZ4', 65536)(lc) FROM t_ecr_lc SETTINGS max_block_size = 65536)
    = (SELECT estimateCompressionRatio('LZ4', 65536)(lc) FROM t_ecr_lc SETTINGS max_block_size = 8192);

SELECT 'Dynamic and JSON columns whose structure changes between blocks are written with the structure of the first block';

SELECT estimateCompressionRatio('LZ4', 65536)(d) > 0
FROM (SELECT if(number < 1000, number::Dynamic, toString(number)::Dynamic) AS d FROM numbers(3000))
SETTINGS max_block_size = 1000;

SELECT k, estimateCompressionRatio('LZ4', 65536)(j) > 0
FROM (SELECT number % 3 AS k, if(number < 1000, '{"a":1}', concat('{"b', toString(number % 100), '":"x"}'))::JSON AS j FROM numbers(3000))
GROUP BY k ORDER BY k
SETTINGS max_block_size = 1000;

SELECT 'GROUP BY: each group is estimated as the part it would be';

SELECT
    k,
    column_data_compressed_bytes = toUInt64(round(column_data_uncompressed_bytes / ratio)) AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN
(
    SELECT k, estimateCompressionRatio('LZ4', 65536)(lc) AS ratio FROM t_ecr_lc GROUP BY k
) AS estimates ON toUInt32(partition) = k
WHERE database = currentDatabase() AND table = 't_ecr_lc_parts' AND active AND column = 'lc'
ORDER BY k;

SELECT
    k,
    column_data_compressed_bytes = toUInt64(round(column_data_uncompressed_bytes / ratio)) AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN
(
    SELECT k, estimateCompressionRatio('LZ4', 65536)(s) AS ratio FROM t_ecr_lc GROUP BY k
) AS estimates ON toUInt32(partition) = k
WHERE database = currentDatabase() AND table = 't_ecr_lc_parts' AND active AND column = 's'
ORDER BY k;

SELECT 'GROUP BY a LowCardinality key';

SELECT count(), min(ratio) > 0 FROM (SELECT lc, estimateCompressionRatio('LZ4', 65536)(lc) AS ratio FROM t_ecr_lc GROUP BY lc);

SELECT 'Combinators';

-- -If: the same rows as a WHERE, read in the same blocks.
SELECT
    estimateCompressionRatioIf('LZ4', 65536)(lc, k = 0) = (SELECT estimateCompressionRatio('LZ4', 65536)(lc) FROM t_ecr_lc WHERE k = 0),
    estimateCompressionRatioIf('LZ4', 65536)(lc, k = 3) = 0
FROM t_ecr_lc;

-- A LowCardinality condition is accepted as a plain one.
SELECT estimateCompressionRatioIf('LZ4', 65536)(lc, toLowCardinality(k = 0)) = estimateCompressionRatioIf('LZ4', 65536)(lc, k = 0) FROM t_ecr_lc;

-- -State and -Merge round-trip; the state type has the dictionary type, as for every aggregate function.
SELECT estimateCompressionRatioMerge('LZ4', 65536)(st) = (SELECT estimateCompressionRatio('LZ4', 65536)(lc) FROM t_ecr_lc)
FROM (SELECT estimateCompressionRatioState('LZ4', 65536)(lc) AS st FROM t_ecr_lc);

SELECT toTypeName(estimateCompressionRatioState('LZ4', 65536)(lc)) FROM t_ecr_lc;

-- States of a LowCardinality(String) and of a String column merge into the ratio of both columns together.
SELECT toUInt64(round(sum(column_data_uncompressed_bytes) / any(ratio))) = sum(column_data_compressed_bytes)
FROM system.parts_columns
CROSS JOIN
(
    SELECT estimateCompressionRatioMerge('LZ4', 65536)(st) AS ratio
    FROM
    (
        SELECT estimateCompressionRatioState('LZ4', 65536)(lc) AS st FROM t_ecr_lc GROUP BY k
        UNION ALL
        SELECT estimateCompressionRatioState('LZ4', 65536)(s) AS st FROM t_ecr_lc GROUP BY k
    )
) AS merged
WHERE database = currentDatabase() AND table = 't_ecr_lc_parts' AND active AND column IN ('lc', 's');

-- A grouped state keeps rows not estimated yet. Completed by a function created from the state type, which has no
-- LowCardinality, they are still estimated as the column they come from.
SELECT g, estimateCompressionRatioMerge('LZ4', 65536)(st) = any(r)
FROM
(
    SELECT
        number % 2 AS g,
        estimateCompressionRatioState('LZ4', 65536)(toLowCardinality(toString(number % 3))) AS st,
        estimateCompressionRatio('LZ4', 65536)(toLowCardinality(toString(number % 3))) AS r
    FROM numbers(10)
    GROUP BY g
)
GROUP BY g
ORDER BY g;

SELECT g, finalizeAggregation(CAST(st, 'AggregateFunction(estimateCompressionRatio(\'LZ4\', 65536), String)')) = r
FROM
(
    SELECT
        number % 2 AS g,
        estimateCompressionRatioState('LZ4', 65536)(toLowCardinality(toString(number % 3))) AS st,
        estimateCompressionRatio('LZ4', 65536)(toLowCardinality(toString(number % 3))) AS r
    FROM numbers(10)
    GROUP BY g
)
ORDER BY g;

-- -Array adds the elements one at a time: a single stream does not depend on how the rows are chunked.
SELECT estimateCompressionRatioArray('LZ4', 65536)(arr) = (SELECT estimateCompressionRatio('LZ4', 65536)(arrayJoin(arr)) FROM (SELECT [s, s] AS arr FROM t_ecr_lc))
FROM (SELECT [s, s] AS arr FROM t_ecr_lc);

SELECT estimateCompressionRatioArray('LZ4', 65536)(arr) > 0, toTypeName(arr) FROM (SELECT [lc, lc] AS arr FROM t_ecr_lc) GROUP BY toTypeName(arr);

-- arrayReduce estimates the elements as they are stored.
SELECT arrayReduce('estimateCompressionRatio', [toLowCardinality('a'), toLowCardinality('b')])
    = (SELECT estimateCompressionRatio(x) FROM (SELECT arrayJoin([toLowCardinality('a'), toLowCardinality('b')]) AS x));

SELECT 'Stacked combinators read their own arguments without LowCardinality';

-- The condition, a LowCardinality(UInt8) in an array, is false for every row: nothing is estimated.
SELECT estimateCompressionRatioIfArray('LZ4', 65536)([lc], [lc = 'none']) FROM t_ecr_lc;
SELECT max(r) FROM (SELECT estimateCompressionRatioIfArray('LZ4', 65536)([lc], [lc = 'none']) OVER () AS r FROM t_ecr_lc);

-- Every row passes: the elements are estimated as the column is stored.
SELECT estimateCompressionRatioIfArray('LZ4', 65536)([lc], [lc != 'none']) = (SELECT estimateCompressionRatio('LZ4', 65536)(lc) FROM t_ecr_lc)
FROM t_ecr_lc;

SELECT estimateCompressionRatioMapIf('LZ4', 65536)(map(k, lc), lc = 'none'), length(estimateCompressionRatioMapIf('LZ4', 65536)(map(k, lc), lc != 'none'))
FROM t_ecr_lc;

-- -Distinct compares the values of a nullable dictionary, NULL and the default value included.
SELECT estimateCompressionRatioDistinct('NONE')(x) > 0 FROM (SELECT toLowCardinality(if(number = 0, NULL, '')) AS x FROM numbers(2));

SELECT 'Window functions';

SELECT countIf(r_lc != r_s_as_lc), countIf(r_lc = r_s), min(r_lc) > 0
FROM
(
    SELECT
        estimateCompressionRatio('LZ4', 65536)(lc) OVER (ORDER BY k, s ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r_lc,
        estimateCompressionRatio('LZ4', 65536)(toLowCardinality(s)) OVER (ORDER BY k, s ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r_s_as_lc,
        estimateCompressionRatio('LZ4', 65536)(s) OVER (ORDER BY k, s ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS r_s
    FROM t_ecr_lc
    LIMIT 1000
);

-- The argument is also the partition key: the frame logic reads it without LowCardinality, the function with it.
SELECT countIf(r_lc != r_s_as_lc)
FROM
(
    SELECT
        estimateCompressionRatio('LZ4', 65536)(lc) OVER (PARTITION BY lc) AS r_lc,
        estimateCompressionRatio('LZ4', 65536)(toLowCardinality(s)) OVER (PARTITION BY s) AS r_s_as_lc
    FROM t_ecr_lc
    LIMIT 1000
);

SELECT 'Specialized codecs on the data substream';

SET allow_suspicious_low_cardinality_types = 1;

DROP TABLE IF EXISTS t_ecr_t64;
CREATE TABLE t_ecr_t64 (lc LowCardinality(UInt32) CODEC(T64), n Nullable(UInt32) CODEC(T64), arr Array(UInt32) CODEC(T64))
ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, index_granularity = 8192, index_granularity_bytes = 0,
         min_compress_block_size = 65536, max_compress_block_size = 65536, ratio_of_defaults_for_sparse_serialization = 1,
         string_serialization_version = 'single_stream';

INSERT INTO t_ecr_t64 SELECT number % 1000, if(number % 7 = 0, NULL, number), [number, number + 1] FROM numbers(3 * 8192);

SELECT
    column,
    column_data_compressed_bytes = toUInt64(round(column_data_uncompressed_bytes / ratio)) AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN
(
    SELECT 'lc' AS column, estimateCompressionRatio('T64', 65536)(lc) AS ratio FROM t_ecr_t64
    UNION ALL
    SELECT 'n' AS column, estimateCompressionRatio('T64', 65536)(n) AS ratio FROM t_ecr_t64
    UNION ALL
    SELECT 'arr' AS column, estimateCompressionRatio('T64', 65536)(arr) AS ratio FROM t_ecr_t64
) AS estimates USING (column)
WHERE database = currentDatabase() AND table = 't_ecr_t64' AND active
ORDER BY column;

-- The codec is validated against the type when the query is analyzed.
SELECT estimateCompressionRatio('T64')(s) FROM t_ecr_lc WHERE 0; -- { serverError ILLEGAL_SYNTAX_FOR_CODEC_TYPE }

SELECT 'Target type: the baseline is the column as it is, the compressed size is that of the target type';

-- Estimated as LowCardinality(String), the String column compresses to the size of the LowCardinality column, and vice versa.
SELECT
    column,
    column_data_compressed_bytes = toUInt64(round(baseline_uncompressed_bytes / ratio)) AS matches_on_disk_size
FROM system.parts_columns
INNER JOIN
(
    SELECT
        'lc' AS column,
        (SELECT column_data_uncompressed_bytes FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ecr_lc' AND active AND column = 's') AS baseline_uncompressed_bytes,
        estimateCompressionRatio('LZ4', 65536, 'LowCardinality(String)')(s) AS ratio
    FROM t_ecr_lc
    UNION ALL
    SELECT
        's' AS column,
        (SELECT column_data_uncompressed_bytes FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ecr_lc' AND active AND column = 'lc') AS baseline_uncompressed_bytes,
        estimateCompressionRatio('LZ4', 65536, 'String')(lc) AS ratio
    FROM t_ecr_lc
) AS estimates USING (column)
WHERE database = currentDatabase() AND table = 't_ecr_lc' AND active
ORDER BY column;

-- Comparable ratios: dictionary encoding stores these 16 values in fewer bytes.
SELECT
    estimateCompressionRatio('LZ4', 65536, 'LowCardinality(String)')(s) > estimateCompressionRatio('LZ4', 65536)(s),
    estimateCompressionRatio('LZ4', 65536, 'String')(lc) < estimateCompressionRatio('LZ4', 65536)(lc),
    estimateCompressionRatio('LZ4', 65536, 'String')(s) = estimateCompressionRatio('LZ4', 65536)(s),
    estimateCompressionRatio('LowCardinality(String)', 'LZ4', 65536)(s) = estimateCompressionRatio('LZ4', 65536, 'LowCardinality(String)')(s)
FROM t_ecr_lc;

-- GROUP BY casts the rows of each group when they are serialized.
SELECT
    k,
    lc_parts.column_data_compressed_bytes = toUInt64(round(s_parts.column_data_uncompressed_bytes / ratio)) AS matches_on_disk_size
FROM
(
    SELECT k, estimateCompressionRatio('LZ4', 65536, 'LowCardinality(String)')(s) AS ratio FROM t_ecr_lc GROUP BY k
) AS estimates
INNER JOIN (SELECT partition, column_data_compressed_bytes FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ecr_lc_parts' AND active AND column = 'lc') AS lc_parts ON toUInt32(lc_parts.partition) = k
INNER JOIN (SELECT partition, column_data_uncompressed_bytes FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_ecr_lc_parts' AND active AND column = 's') AS s_parts ON toUInt32(s_parts.partition) = k
ORDER BY k;

-- A wider representation of the same values: comparable with the ratio of the column as it is.
SELECT estimateCompressionRatio('LZ4', 65536, 'IPv6')(toIPv4(number)) < estimateCompressionRatio('LZ4', 65536)(toIPv4(number)) FROM numbers(3 * 8192);

-- Converting is checked as `accurateCast` does; a conversion that does not exist is rejected when the query is analyzed.
SELECT estimateCompressionRatio('LZ4', 65536, 'UInt8')(number) FROM numbers(1000); -- { serverError CANNOT_CONVERT_TYPE }
SELECT estimateCompressionRatio('LZ4', 65536, 'Array(UInt8)')(number) FROM numbers(1) WHERE 0; -- { serverError TYPE_MISMATCH }

-- A mistyped type is reported as such, not as an unknown codec.
SELECT estimateCompressionRatio('LZ4', 65536, 'LowCardinality(Strin)')(number) FROM numbers(1); -- { serverError UNKNOWN_TYPE }

-- At most one data type, at most three parameters.
SELECT estimateCompressionRatio('LZ4', 'UInt8', 'Int8')(number) FROM numbers(1); -- { serverError BAD_QUERY_PARAMETER }
SELECT estimateCompressionRatio('LZ4', 65536, 'UInt8', 'Int8')(number) FROM numbers(1); -- { serverError UNKNOWN_QUERY_PARAMETER }

DROP TABLE t_ecr_t64;
DROP TABLE t_ecr_lc_parts;
DROP TABLE t_ecr_lc;
