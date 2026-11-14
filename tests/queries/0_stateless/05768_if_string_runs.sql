-- if() with a String column in one or both branches copies runs of equal conditions at once.
-- Every result is compared with arrayElement([else, then], (cond != 0) + 1), for alternating, multi-valued, short,
-- long, constant and random conditions, strings of 0..96 bytes, and blocks of 1, 7 and 1000 rows.

DROP VIEW IF EXISTS v_if_string_runs;
CREATE VIEW v_if_string_runs AS
SELECT
    number AS n,
    toUInt8(multiIf(
        number < 1000, number % 2,
        number < 2000, number % 3,
        number < 3000, intDiv(number, 7) % 2 * 2,
        number < 4000, intDiv(number, 100) % 2 * 255,
        number < 4500, 1,
        number < 5000, 0,
        number < 8000, cityHash64(number) % 4 = 0,
        number < 9000, cityHash64(number) % 2,
        intDiv(number, 3) % 2)) AS c,
    repeat(char(97 + number % 26), multiIf(number < 8000, number % 97, number < 9000, 64 + number % 33, 32 + number % 17)) AS s,
    multiIf(number < 8000, if(number % 5 = 0, '', toString(number * 7919)), number < 9000, repeat('t', 64 + number % 29), repeat('t', 32 + number % 13)) AS t,
    if(number % 13 = 0, NULL, c) AS cn,
    if(number % 11 = 0, NULL, s) AS ns
FROM numbers(10000);

SELECT 'col_col', countIf(r != ref), sum(cityHash64(r)), sum(length(r))
FROM (SELECT if(c, s, t) AS r, arrayElement([t, s], (c != 0) + 1) AS ref FROM v_if_string_runs) SETTINGS max_block_size = 1;
SELECT 'col_col', countIf(r != ref), sum(cityHash64(r)), sum(length(r))
FROM (SELECT if(c, s, t) AS r, arrayElement([t, s], (c != 0) + 1) AS ref FROM v_if_string_runs) SETTINGS max_block_size = 7;
SELECT 'col_col', countIf(r != ref), sum(cityHash64(r)), sum(length(r))
FROM (SELECT if(c, s, t) AS r, arrayElement([t, s], (c != 0) + 1) AS ref FROM v_if_string_runs) SETTINGS max_block_size = 1000;

SELECT 'col_empty', countIf(r != ref), sum(cityHash64(r)), sum(length(r))
FROM (SELECT if(c, s, '') AS r, arrayElement(['', s], (c != 0) + 1) AS ref FROM v_if_string_runs) SETTINGS max_block_size = 7;
SELECT 'col_empty', countIf(r != ref), sum(cityHash64(r)), sum(length(r))
FROM (SELECT if(c, s, '') AS r, arrayElement(['', s], (c != 0) + 1) AS ref FROM v_if_string_runs) SETTINGS max_block_size = 1000;

SELECT 'col_const', countIf(r != ref), sum(cityHash64(r)), sum(length(r))
FROM (SELECT if(c, s, 'a constant longer than sixteen bytes') AS r, arrayElement(['a constant longer than sixteen bytes', s], (c != 0) + 1) AS ref FROM v_if_string_runs) SETTINGS max_block_size = 1000;
SELECT 'col_const', countIf(r != ref), sum(cityHash64(r)), sum(length(r))
FROM (SELECT if(c, s, 'x') AS r, arrayElement(['x', s], (c != 0) + 1) AS ref FROM v_if_string_runs) SETTINGS max_block_size = 7;

SELECT 'const_col', countIf(r != ref), sum(cityHash64(r)), sum(length(r))
FROM (SELECT if(c, 'yz', t) AS r, arrayElement([t, 'yz'], (c != 0) + 1) AS ref FROM v_if_string_runs) SETTINGS max_block_size = 1000;
SELECT 'empty_col', countIf(r != ref), sum(cityHash64(r)), sum(length(r))
FROM (SELECT if(c, '', s) AS r, arrayElement([s, ''], (c != 0) + 1) AS ref FROM v_if_string_runs) SETTINGS max_block_size = 7;

SELECT 'nullable_cond', countIf(r != ref), sum(cityHash64(r)), sum(length(r))
FROM (SELECT if(cn, s, t) AS r, arrayElement([t, s], (ifNull(cn, 0) != 0) + 1) AS ref FROM v_if_string_runs) SETTINGS max_block_size = 1000;
SELECT 'nullable_branch', countIf(NOT (r <=> ref)), countIf(r IS NULL), sum(cityHash64(ifNull(r, '-')))
FROM (SELECT if(c, ns, t) AS r, arrayElement([toNullable(t), ns], (c != 0) + 1) AS ref FROM v_if_string_runs) SETTINGS max_block_size = 1000;

SELECT 'case', countIf(r != ref), sum(cityHash64(r))
FROM (SELECT CASE WHEN c != 0 AND n % 3 != 0 THEN s ELSE '' END AS r, arrayElement(['', s], (c != 0 AND n % 3 != 0) + 1) AS ref FROM v_if_string_runs) SETTINGS max_block_size = 1000;

SELECT 'values', n, c, left(if(c, s, '') AS r, 12), length(r) FROM v_if_string_runs WHERE n IN (0, 1, 2, 96, 97, 4499, 4500, 7999) ORDER BY n;
SELECT 'empty_input', count() FROM (SELECT if(c, s, '') AS r FROM v_if_string_runs WHERE n > 100000);

-- The result is longer than both branches, so it grows beyond the reserved size.
SELECT 'grow', countIf(r != ref), sum(cityHash64(r)), sum(length(r))
FROM (SELECT if(c2, l1, l2) AS r, arrayElement([l2, l1], c2 + 1) AS ref
    FROM (SELECT toUInt8(number % 2) AS c2, if(number % 2 = 1, repeat('x', 50 + number % 7), '') AS l1, if(number % 2 = 0, repeat('y', 50 + number % 5), '') AS l2 FROM numbers(5000)))
SETTINGS max_block_size = 1000;

-- Only empty strings.
SELECT 'all_empty', countIf(r != ''), sum(length(r))
FROM (SELECT if(number % 2 = 0, '', substring(toString(number), 1, 0)) AS r FROM numbers(1000));

DROP VIEW v_if_string_runs;
