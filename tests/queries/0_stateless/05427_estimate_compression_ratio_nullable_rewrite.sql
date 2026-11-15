SELECT k, optimized.ratio = unoptimized.ratio
FROM
(
    SELECT number % 2 AS k, estimateCompressionRatio('NONE')(if(number % 3 = 0, number, NULL)) AS ratio
    FROM numbers(1000)
    GROUP BY k
    SETTINGS optimize_rewrite_aggregate_function_with_if = 1, max_threads = 1
) AS optimized
INNER JOIN
(
    SELECT number % 2 AS k, estimateCompressionRatio('NONE')(if(number % 3 = 0, number, NULL)) AS ratio
    FROM numbers(1000)
    GROUP BY k
    SETTINGS optimize_rewrite_aggregate_function_with_if = 0, max_threads = 1
) AS unoptimized USING (k)
ORDER BY k;

SELECT
    (SELECT estimateCompressionRatio('NONE')(if(number < 10, number, NULL)) FROM numbers(1000) SETTINGS optimize_rewrite_aggregate_function_with_if = 1, max_threads = 1)
    =
    (SELECT estimateCompressionRatio('NONE')(if(number < 10, number, NULL)) FROM numbers(1000) SETTINGS optimize_rewrite_aggregate_function_with_if = 0, max_threads = 1);

SELECT
    (SELECT estimateCompressionRatio(if(number < 10, number, NULL)) FROM numbers(1000) SETTINGS optimize_rewrite_aggregate_function_with_if = 1, max_threads = 1)
    =
    (SELECT estimateCompressionRatio(if(number < 10, number, NULL)) FROM numbers(1000) SETTINGS optimize_rewrite_aggregate_function_with_if = 0, max_threads = 1);
