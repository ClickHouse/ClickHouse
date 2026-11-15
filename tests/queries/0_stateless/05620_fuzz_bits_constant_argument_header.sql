-- fuzzBits with a constant argument returned a constant header (the unchanged argument) but per-row fuzzed data,
-- so consumers that trust the header read the unfuzzed value or saw an unsorted stream.
-- https://github.com/ClickHouse/ClickHouse/issues/118934

-- With probability 1 every bit is flipped: 'aaaaaaaa' (0x61) becomes 0x9E in every row.
SELECT hex(x) FROM (SELECT fuzzBits('aaaaaaaa', 1.) AS x FROM numbers(2)) ORDER BY 1;
SELECT hex(x) FROM (SELECT fuzzBits(toFixedString('aaaaaaaa', 8), 1.) AS x FROM numbers(2)) ORDER BY 1;
SELECT hex(x) FROM (SELECT fuzzBits(toNullable('aaaaaaaa'), 1.) AS x FROM numbers(2)) ORDER BY 1;
SELECT hex(x) FROM (SELECT fuzzBits(toLowCardinality('aaaaaaaa'), 1.) AS x FROM numbers(2)) ORDER BY 1;
SELECT DISTINCT hex(x) FROM (SELECT fuzzBits('aaaaaaaa', 1.) AS x FROM numbers(2));
SELECT hex(x) FROM (SELECT fuzzBits('aaaaaaaa', 1.) AS x FROM numbers(2)) LIMIT 1 BY x;
SELECT uniqExact(x) > 1 FROM (SELECT fuzzBits('aaaaaaaaaaaaaaaa', 0.4) AS x FROM numbers(100));

-- A window partitioned by a fuzzed key: every row is its own partition.
SELECT max(c), count() FROM (SELECT count() OVER (PARTITION BY fuzzBits('aaaaaaaaaaaaaaaa', 0.4), number % 1000 ORDER BY number) AS c FROM numbers(10000));
