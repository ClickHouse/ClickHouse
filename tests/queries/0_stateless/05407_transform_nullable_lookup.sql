-- A NULL `x` matches only the first NULL `from` entry, whatever value is stored under the NULL.
SELECT x, transform(x, [NULL, 'a', 'b', NULL], [10, 20, 30, 40], 0) FROM (SELECT arrayJoin([NULL, 'a', 'b', 'c', '']::Array(Nullable(String))) AS x);
SELECT x, transform(nullIf(x, 'a'), ['a', 'b'], [20, 30], 0), transform(nullIf(x, ''), [NULL, ''], [10, 20], 0) FROM (SELECT arrayJoin(['a', 'b', 'c', '']) AS x);
SELECT x, transform(x, [NULL, 'a'], ['N', 'A'], 'D'), transform(x, ['a'], [1], NULL), transform(x, [NULL, 'a'], [NULL, 20], 0) FROM (SELECT arrayJoin([NULL, 'a', 'b', '']::Array(Nullable(String))) AS x);
SELECT x, CASE x WHEN 'a' THEN 1 WHEN 'b' THEN 2 ELSE 0 END, CASE x WHEN 'a' THEN 1 WHEN 'b' THEN 2 END FROM (SELECT arrayJoin([NULL, 'a', 'b', 'c']::Array(Nullable(String))) AS x);
SELECT x, transform(x, ['a'], [20], toUInt8(length(coalesce(x, 'nnn')))), transform(x, [NULL, 'a'], [10, 20], toUInt8(length(coalesce(x, 'nnn')))) FROM (SELECT arrayJoin([NULL, 'a', 'b', 'cc']::Array(Nullable(String))) AS x);
SELECT x, transform(x, [NULL, 'a'], [10, 20], 0) FROM (SELECT arrayJoin([NULL, 'a', 'b']::Array(LowCardinality(Nullable(String)))) AS x);
SELECT transform(NULL::Nullable(String), [NULL, 'a'], [10, 20], 0), transform('a'::Nullable(String), [NULL, 'a'], [10, 20], 0), transform(NULL::Nullable(String), ['a'], [20], 0);
SELECT x, transform(x, ['abc', NULL], [1, 2], 0) FROM (SELECT arrayJoin(['abc', NULL, 'ab', '']::Array(Nullable(FixedString(3)))) AS x);
SELECT x, transform(x, [NULL, 1, 2], [10, 20, 30], 99), transform(nullIf(x, 1), [1, 2], [20, 30], 99) FROM (SELECT arrayJoin([NULL, 0, 1, 2]::Array(Nullable(Int32))) AS x);
SELECT x, transform(x, [NULL, 0., -0., nan], [1, 2, 3, 4], 9) FROM (SELECT arrayJoin([NULL, 0., -0., nan]::Array(Nullable(Float64))) AS x);
SELECT x, transform(x, [NULL, toDecimal64(1.5, 2)], [1, 2], 0) FROM (SELECT arrayJoin([NULL, toDecimal64(1.5, 2), toDecimal64(0, 2)]) AS x);
SELECT x, transform(x, [NULL, toDate('2020-01-01')], [1, 2], 0) FROM (SELECT arrayJoin([NULL, toDate('2020-01-01'), toDate('1970-01-01')]) AS x);
SELECT x, transform(x, ['a', 'b'], [1, 2], 0), CASE x WHEN 'a' THEN 'A' WHEN 'b' THEN 'B' ELSE 'other' END FROM (SELECT arrayJoin(['a', 'b', NULL]::Array(Nullable(Enum8('a' = 1, 'b' = 2)))) AS x);

-- Several blocks with NULLs over values equal to a key, and NULL-only / NULL-free blocks.
SELECT sum(transform(x, [NULL, 3, 5], [100, 3, 5], 0)), countIf(transform(x, [NULL, 3, 5], [100, 3, 5], 0) = 100), sum(transform(x, [3, 5], [3, 5], 7)) FROM (SELECT if(number % 11 = 0, NULL, nullIf(toInt32(number % 10), 3)) AS x FROM numbers(300000)) SETTINGS max_block_size = 1000;
SELECT sum(transform(x, [NULL, 1], [100, 1], 0)), sum(transform(toNullable(toString(x)), ['1'], [1], 7)) FROM (SELECT if(intDiv(number, 1000) = 1, NULL, toInt32(number % 10)) AS x FROM numbers(3000)) SETTINGS max_block_size = 1000;
SELECT k, count() FROM (SELECT CASE nullIf(s, 'Sunday') WHEN 'Sunday' THEN 'weekend' WHEN 'Holiday' THEN 'off' ELSE 'weekday' END AS k FROM (SELECT arrayElement(['Sunday', 'Monday', 'Holiday'], number % 3 + 1) AS s FROM numbers(30000))) GROUP BY k ORDER BY k;
