-- Aggregation without GROUP BY adds whole blocks at once: NaNs and NULLs must be skipped.
SET max_block_size = 1000;

SELECT quantilesExact(0.1, 0.5, 0.9)(number) FROM numbers(100000);
SELECT quantilesExact(0.1, 0.5, 0.9)(toDecimal64(number, 2)) FROM numbers(100000);
SELECT quantilesExact(0.1, 0.5, 0.9)(if(number % 3 = 0, nan, toFloat64(number))) FROM numbers(100000);
SELECT quantilesExact(0.1, 0.5, 0.9)(if(number % 3 = 0, NULL, number)) FROM numbers(100000);
SELECT quantilesExact(0.1, 0.5, 0.9)(multiIf(number % 3 = 0, NULL, number % 3 = 1, nan, toFloat64(number))) FROM numbers(100000);
SELECT quantilesExactIf(0.1, 0.5, 0.9)(number, number % 3 != 0) FROM numbers(100000);
SELECT quantileExactLow(0.5)(toFloat32(nan)), quantileExactHigh(0.5)(if(number < 0, number, NULL)) FROM numbers(10);
SELECT quantilesExactIf(0.1, 0.5, 0.9)(if(number % 3 = 0, nan, toFloat64(number)), number % 5 != 0) FROM numbers(100000);
SELECT quantilesExactIf(0.1, 0.5, 0.9)(if(number % 3 = 0, NULL, number), number % 5 != 0) FROM numbers(100000);
SELECT quantilesExactIf(0.1, 0.5, 0.9)(multiIf(number % 3 = 0, NULL, number % 7 = 0, nan, toFloat64(number)), number % 5 != 0) FROM numbers(100000);
SELECT quantilesExact(0.1, 0.5, 0.9)(number) FROM numbers_mt(100000) SETTINGS max_threads = 8;
SELECT quantilesExactMerge(0.1, 0.5, 0.9)(state) FROM (SELECT quantilesExactState(0.1, 0.5, 0.9)(number) AS state FROM numbers(100000) GROUP BY number % 7);
SELECT quantilesExactMerge(0.1, 0.5, 0.9)(state) FROM (SELECT quantilesExactState(0.1, 0.5, 0.9)(number) AS state FROM numbers(3) GROUP BY number);
