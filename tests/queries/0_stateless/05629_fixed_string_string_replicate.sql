-- `ARRAY JOIN` and `arrayJoin` replicate the other columns of the block, including `String` and `FixedString` ones.

SELECT n, fs, s, x
FROM (SELECT number AS n, toFixedString(toString(number), 3) AS fs, repeat('s', number) AS s, range(number) AS arr FROM numbers(4))
ARRAY JOIN arr AS x
ORDER BY n, x;

SELECT count(), sum(length(fs)), sum(length(s)), uniqExact(fs), uniqExact(s)
FROM (SELECT materialize(toFixedString('abc', 8)) AS fs, materialize(repeat('x', 100)) AS s, range(1000) AS arr FROM numbers(100))
ARRAY JOIN arr;

SELECT count(), uniqExact(hex(fs))
FROM (SELECT toFixedString(toString(number % 7), 16) AS fs, arrayJoin(range(number % 5)) AS x FROM numbers(1000));

SELECT replicate(toFixedString('ab', 2), [1, 2, 3]), replicate('ab', [1, 2]);
