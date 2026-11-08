-- Random settings limits: max_bytes_before_external_group_by=(0, 0); max_bytes_ratio_before_external_group_by=(0, 0)

-- `uniqExact` over several arguments, one `Tuple` or one `Decimal` argument computes the keys of a chunk of rows
-- before inserting them. The results and the serialized states must stay those of inserting the rows one by one.
-- The states are taken over `numbers` with one thread, so that the rows are inserted in a fixed order;
-- spilling (limited above) would merge partial states.

DROP VIEW IF EXISTS v;
DROP TABLE IF EXISTS t;

CREATE VIEW v AS SELECT
    number,
    toUInt16(number % 7) AS k,
    number % 1000 AS u,
    concat(toString(number % 400), repeat('a', number % 17)) AS s,
    toFixedString(toString(number % 97), 5) AS fs,
    toDecimal64((number % 250) / 4, 2) AS d,
    if(number % 5 = 0, NULL, toString(number % 300)) AS ns,
    (number % 600, toString(number % 50)) AS t2,
    [toUInt8(number % 3), toUInt8(number % 11)] AS arr,
    toLowCardinality(toString(number % 13)) AS lc
FROM numbers(3000);

CREATE TABLE t ENGINE = MergeTree ORDER BY number AS SELECT * FROM v;

SELECT 'no key';
SELECT uniqExact(u, s), uniqExact(t2), uniqExact(s, fs, d), uniqExact(arr, lc), uniqExact(u, ns), uniqExact(d),
    uniqExactIf(u, s, u % 3 = 0), count(DISTINCT u, s)
FROM t;

SELECT 'GROUP BY k';
SELECT k, uniqExact(u, s), uniqExact(t2), uniqExact(s, fs, d), uniqExact(arr, lc), uniqExact(u, ns), uniqExact(d),
    uniqExactIf(u, s, u % 3 = 0), count(DISTINCT u, s)
FROM t GROUP BY k ORDER BY k;

SELECT 'GROUP BY runs of equal keys';
SELECT intDiv(number, 500) AS r, uniqExact(u, s), uniqExact(t2), uniqExact(s, fs, d), uniqExact(arr, lc), uniqExact(u, ns),
    uniqExact(d), uniqExactIf(u, s, u % 3 = 0), count(DISTINCT u, s)
FROM t GROUP BY r ORDER BY r SETTINGS max_threads = 1;

SELECT 'states, no key';
SELECT 7, hex(MD5(hex(uniqExactState(u, s)))), hex(MD5(hex(uniqExactState(t2)))), hex(MD5(hex(uniqExactState(s, fs, d)))),
    hex(MD5(hex(uniqExactState(arr, lc)))), hex(MD5(hex(uniqExactState(u, ns)))), hex(MD5(hex(uniqExactState(d)))),
    hex(MD5(hex(uniqExactIfState(u, s, u % 3 = 0))))
FROM v SETTINGS max_threads = 1, max_block_size = 7;
SELECT 257, hex(MD5(hex(uniqExactState(u, s)))), hex(MD5(hex(uniqExactState(t2)))), hex(MD5(hex(uniqExactState(s, fs, d)))),
    hex(MD5(hex(uniqExactState(arr, lc)))), hex(MD5(hex(uniqExactState(u, ns)))), hex(MD5(hex(uniqExactState(d)))),
    hex(MD5(hex(uniqExactIfState(u, s, u % 3 = 0))))
FROM v SETTINGS max_threads = 1, max_block_size = 257;
SELECT 1000, hex(MD5(hex(uniqExactState(u, s)))), hex(MD5(hex(uniqExactState(t2)))), hex(MD5(hex(uniqExactState(s, fs, d)))),
    hex(MD5(hex(uniqExactState(arr, lc)))), hex(MD5(hex(uniqExactState(u, ns)))), hex(MD5(hex(uniqExactState(d)))),
    hex(MD5(hex(uniqExactIfState(u, s, u % 3 = 0))))
FROM v SETTINGS max_threads = 1, max_block_size = 1000;
SELECT 65409, hex(MD5(hex(uniqExactState(u, s)))), hex(MD5(hex(uniqExactState(t2)))), hex(MD5(hex(uniqExactState(s, fs, d)))),
    hex(MD5(hex(uniqExactState(arr, lc)))), hex(MD5(hex(uniqExactState(u, ns)))), hex(MD5(hex(uniqExactState(d)))),
    hex(MD5(hex(uniqExactIfState(u, s, u % 3 = 0))))
FROM v SETTINGS max_threads = 1, max_block_size = 65409;

SELECT 'states, GROUP BY k';
SELECT 7, k, hex(MD5(hex(uniqExactState(u, s)))), hex(MD5(hex(uniqExactState(t2)))), hex(MD5(hex(uniqExactState(s, fs, d)))),
    hex(MD5(hex(uniqExactState(arr, lc)))), hex(MD5(hex(uniqExactState(u, ns)))), hex(MD5(hex(uniqExactState(d)))),
    hex(MD5(hex(uniqExactIfState(u, s, u % 3 = 0))))
FROM v GROUP BY k ORDER BY k SETTINGS max_threads = 1, max_block_size = 7;
SELECT 257, k, hex(MD5(hex(uniqExactState(u, s)))), hex(MD5(hex(uniqExactState(t2)))), hex(MD5(hex(uniqExactState(s, fs, d)))),
    hex(MD5(hex(uniqExactState(arr, lc)))), hex(MD5(hex(uniqExactState(u, ns)))), hex(MD5(hex(uniqExactState(d)))),
    hex(MD5(hex(uniqExactIfState(u, s, u % 3 = 0))))
FROM v GROUP BY k ORDER BY k SETTINGS max_threads = 1, max_block_size = 257;
SELECT 1000, k, hex(MD5(hex(uniqExactState(u, s)))), hex(MD5(hex(uniqExactState(t2)))), hex(MD5(hex(uniqExactState(s, fs, d)))),
    hex(MD5(hex(uniqExactState(arr, lc)))), hex(MD5(hex(uniqExactState(u, ns)))), hex(MD5(hex(uniqExactState(d)))),
    hex(MD5(hex(uniqExactIfState(u, s, u % 3 = 0))))
FROM v GROUP BY k ORDER BY k SETTINGS max_threads = 1, max_block_size = 1000;
SELECT 65409, k, hex(MD5(hex(uniqExactState(u, s)))), hex(MD5(hex(uniqExactState(t2)))), hex(MD5(hex(uniqExactState(s, fs, d)))),
    hex(MD5(hex(uniqExactState(arr, lc)))), hex(MD5(hex(uniqExactState(u, ns)))), hex(MD5(hex(uniqExactState(d)))),
    hex(MD5(hex(uniqExactIfState(u, s, u % 3 = 0))))
FROM v GROUP BY k ORDER BY k SETTINGS max_threads = 1, max_block_size = 65409;

-- Blocks of one key each: the rows reach the function as ranges of equal keys.
SELECT 'states, GROUP BY runs of equal keys';
SELECT intDiv(number, 500) AS r, hex(MD5(hex(uniqExactState(u, s)))), hex(MD5(hex(uniqExactState(t2)))), hex(MD5(hex(uniqExactState(s, fs, d)))),
    hex(MD5(hex(uniqExactState(arr, lc)))), hex(MD5(hex(uniqExactState(u, ns)))), hex(MD5(hex(uniqExactState(d)))),
    hex(MD5(hex(uniqExactIfState(u, s, u % 3 = 0))))
FROM v GROUP BY r ORDER BY r SETTINGS max_threads = 1, max_block_size = 500;

SELECT 'states, GROUP BY with keys left out';
SELECT k, p, uniqExact(u, s), hex(MD5(hex(uniqExactState(u, s)))), hex(MD5(hex(uniqExactIfState(u, s, u % 3 = 0))))
FROM (SELECT *, number % 2 AS p FROM v) GROUP BY k, p ORDER BY k, p
SETTINGS max_rows_to_group_by = 2, group_by_overflow_mode = 'any', max_threads = 1, max_block_size = 7;

-- Two threads make the sets two-level once they hold more than 100000 keys; one stream keeps the insertion order fixed.
SELECT 'states, two-level';
SELECT 65505, uniqExact(number, s), hex(MD5(hex(uniqExactState(number, s))))
FROM (SELECT number, toString(number) AS s FROM numbers(300000)) SETTINGS max_threads = 2, max_block_size = 65505;
SELECT 1000, uniqExact(number, s), hex(MD5(hex(uniqExactState(number, s))))
FROM (SELECT number, toString(number) AS s FROM numbers(300000)) SETTINGS max_threads = 2, max_block_size = 1000;
SELECT 65505, g, uniqExact(number, s), hex(MD5(hex(uniqExactState(number, s))))
FROM (SELECT number, toString(number) AS s, number % 2 AS g FROM numbers(400000)) GROUP BY g ORDER BY g
SETTINGS max_threads = 2, max_block_size = 65505;
SELECT 1000, g, uniqExact(number, s), hex(MD5(hex(uniqExactState(number, s))))
FROM (SELECT number, toString(number) AS s, number % 2 AS g FROM numbers(400000)) GROUP BY g ORDER BY g
SETTINGS max_threads = 2, max_block_size = 1000;

DROP TABLE t;
DROP VIEW v;
