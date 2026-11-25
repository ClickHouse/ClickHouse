WITH arrayStringConcat(arrayMap(i -> char((i + number) % 256), range(width))) AS value
SELECT countIf(repeat(value, 1) != value)
FROM numbers(17) AS source_rows
CROSS JOIN (SELECT arrayJoin([0, 1, 2, 3, 7, 14, 16, 17, 31, 64, 257]) AS width) AS widths;

WITH arrayStringConcat(arrayMap(i -> char((i + number) % 256), range(width))) AS value
SELECT countIf(repeat(value, toUInt256(1)) != value)
FROM numbers(17) AS source_rows
CROSS JOIN (SELECT arrayJoin([0, 1, 2, 3, 7, 14, 16, 17, 31, 64, 257]) AS width) AS widths;

SELECT countIf(repeat(materialize(''), toInt256(1)) != '') FROM numbers(17);
