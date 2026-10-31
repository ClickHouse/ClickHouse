-- Keyless `uniqExact` and `count(DISTINCT)` (radix partitioned) count a negative zero and a positive zero as one value,
-- as the grouped `uniqExact` and the ordinary keyless `uniqExact` do.

SELECT uniqExact(x) FROM values('x Float64', -0., 1., 0.);
SELECT uniqExact(x) FROM values('x Float32', 0., 1., -0.);
SELECT uniqExact(x) FROM values('x BFloat16', 0., 1., -0.);
SELECT uniqExact(x) FROM values('x Nullable(Float64)', -0., NULL, 1., 0.);

SELECT count(DISTINCT x), uniqExact(x)
FROM (SELECT multiIf(number % 3 = 0, -0., number % 3 = 1, 0., toFloat64(number)) AS x FROM numbers(30000))
SETTINGS max_threads = 4, max_block_size = 1000;

SELECT count(DISTINCT x)
FROM (SELECT multiIf(number % 3 = 0, -0., number % 3 = 1, 0., toFloat64(number)) AS x FROM numbers(30000))
SETTINGS optimize_uniq_exact_radix_partitioning = 0;
