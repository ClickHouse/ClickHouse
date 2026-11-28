WITH arrayStringConcat(arrayMap(i -> char((i + number) % 256), range(width))) AS value
SELECT countIf(repeat(value, repetitions) != arrayStringConcat(arrayWithConstant(repetitions, value)))
FROM numbers(4) AS source_rows
CROSS JOIN (SELECT arrayJoin([0, 1, 2, 3, 4, 7, 8, 9, 14, 15, 16, 17, 28, 31, 32, 33, 56, 63, 64, 65, 257]) AS width) AS widths
CROSS JOIN (SELECT arrayJoin([0, 1, 2, 3, 63, 64, 70, 71, 127, 128, 129]) AS repetitions) AS counts;

WITH arrayStringConcat(arrayMap(i -> char((i + number) % 256), range(width))) AS value
SELECT countIf(repeat(value, 70) != arrayStringConcat(arrayWithConstant(70, value)))
FROM numbers(4) AS source_rows
CROSS JOIN (SELECT arrayJoin([0, 1, 2, 3, 4, 7, 8, 9, 14, 15, 16, 17, 28, 31, 32, 33, 56, 63, 64, 65, 257]) AS width) AS widths;

SELECT countIf(repeat('a\0b', repetitions) != arrayStringConcat(arrayWithConstant(repetitions, 'a\0b')))
FROM (SELECT arrayJoin([0, 1, 2, 3, 63, 64, 70, 71, 127, 128, 129]) AS repetitions) AS counts;
