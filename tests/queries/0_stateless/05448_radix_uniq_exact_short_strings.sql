-- `uniqExact` with radix partitioning represents the strings of up to 15 bytes by their bytes and length, and the longer
-- ones by a hash. It must count as many distinct strings as without it, also when they differ only in length.

-- `count_distinct_optimization` would rewrite `uniqExact` to a `GROUP BY`.
SET max_threads = 4, count_distinct_optimization = 0, collect_hash_table_stats_during_aggregation = 0;

SELECT 'only zero bytes, by length',
    (SELECT uniqExact(x) FROM (SELECT repeat('\0', number % 21) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT repeat('\0', number % 21) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'lengths 14 to 17',
    (SELECT uniqExact(x) FROM (SELECT leftPad(toString(number % 300000), 14 + number % 4, 'x') AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT leftPad(toString(number % 300000), 14 + number % 4, 'x') AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'with and without a trailing zero byte',
    (SELECT uniqExact(x) FROM (SELECT if(number % 2 = 0, toString(number % 300001), concat(toString(number % 300001), '\0')) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT if(number % 2 = 0, toString(number % 300001), concat(toString(number % 300001), '\0')) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);

SELECT 'empty and lengths 1 to 15',
    (SELECT uniqExact(x) FROM (SELECT if(number % 3 = 0, '', leftPad(toString(number % 300000), 1 + number % 15, '0')) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 1),
    (SELECT uniqExact(x) FROM (SELECT if(number % 3 = 0, '', leftPad(toString(number % 300000), 1 + number % 15, '0')) AS x FROM numbers_mt(2000000)) SETTINGS optimize_uniq_exact_radix_partitioning = 0);
