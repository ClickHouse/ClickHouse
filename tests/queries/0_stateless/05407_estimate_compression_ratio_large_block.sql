-- Tags: no-sanitizers
-- no-sanitizers: too slow

SELECT round(estimateCompressionRatio('DoubleDelta', 232160396)(toUInt16(number)), 2) FROM numbers(116080198) SETTINGS max_threads = 1, max_rows_to_read = 0;
SELECT round(estimateCompressionRatio('Gorilla', 252645136)(toUInt8(0)), 2) FROM numbers(252645136) SETTINGS max_threads = 1, max_rows_to_read = 0;
