-- Without GROUP BY, `quantile` draws the position of the next kept value once a state has 131072 values.
SET max_threads = 1, max_bytes_before_external_group_by = 0, max_bytes_ratio_before_external_group_by = 0;

SELECT 'the same sample for any block size';
SELECT (SELECT quantiles(0.1, 0.5, 0.9)(number) FROM numbers(1000000) SETTINGS max_block_size = 65505) = (SELECT quantiles(0.1, 0.5, 0.9)(number) FROM numbers(1000000) SETTINGS max_block_size = 777);
SELECT (SELECT quantilesIf(0.1, 0.5, 0.9)(number, number % 3 != 0) FROM numbers(1000000) SETTINGS max_block_size = 65505) = (SELECT quantilesIf(0.1, 0.5, 0.9)(number, number % 3 != 0) FROM numbers(1000000) SETTINGS max_block_size = 777);
SELECT (SELECT quantiles(0.1, 0.5, 0.9)(if(number % 3 = 0, NULL, number)) FROM numbers(1000000) SETTINGS max_block_size = 65505) = (SELECT quantiles(0.1, 0.5, 0.9)(if(number % 3 = 0, NULL, number)) FROM numbers(1000000) SETTINGS max_block_size = 777);

SELECT 'uniform positions, filtered, NULL and NaN values are not counted';
SELECT arrayAll(x -> abs(x.2 / 4000000 - x.1) < 0.03, arrayZip([0.01, 0.1, 0.5, 0.9, 0.99], quantiles(0.01, 0.1, 0.5, 0.9, 0.99)(number))) FROM numbers(4000000);
SELECT abs(quantileIf(0.5)(number, number % 2 = 0) / 2000000 - 0.5) < 0.03 FROM numbers(2000000);
SELECT abs(quantile(0.5)(if(number % 3 = 0, NULL, number)) / 3000000 - 0.5) < 0.03 FROM numbers(3000000);
SELECT abs(quantile(0.5)(if(number >= 1000000, nan, toFloat64(number))) / 1000000 - 0.5) < 0.03 FROM numbers(3000000);
SELECT abs(quantileIf(0.5)(if(number % 3 = 0, nan, toFloat64(number)), number % 2 = 0) / 2000000 - 0.5) < 0.03 FROM numbers(2000000);
SELECT abs(quantile(0.5)(if(number % 5 = 0, NULL, if(number % 3 = 0, nan, toFloat64(number)))) / 2000000 - 0.5) < 0.03 FROM numbers(2000000) SETTINGS optimize_rewrite_aggregate_function_with_if = 0;
SELECT abs(toFloat64(quantile(0.25)(toDecimal64(number, 2))) / 2000000 - 0.25) < 0.03 FROM numbers(2000000);

SELECT 'sparse column';
DROP TABLE IF EXISTS t_reservoir_sparse;
CREATE TABLE t_reservoir_sparse (x UInt64) ENGINE = MergeTree ORDER BY tuple() SETTINGS ratio_of_defaults_for_sparse_serialization = 0.5;
INSERT INTO t_reservoir_sparse SELECT if(number % 10 = 0, number, 0) FROM numbers(3000000);
SELECT abs(quantile(0.95)(x) - 1500000) < 600000, quantile(0.5)(x) FROM t_reservoir_sparse;
DROP TABLE t_reservoir_sparse;

SELECT 'State and Merge, GROUP BY with one key per block';
SELECT (SELECT quantileMerge(0.9)(s) FROM (SELECT quantileState(0.9)(number) AS s FROM numbers(1000000))) = (SELECT quantile(0.9)(number) FROM numbers(1000000));
SELECT max(abs(q / 1000000 - k - 0.5)) < 0.03 FROM (SELECT intDiv(number, 1000000) AS k, quantile(0.5)(number) AS q FROM numbers(3000000) GROUP BY k);
