-- The LIMIT BY hash tables below outgrow L2, so the row loop prefetches.
SELECT count(), sum(v) FROM (SELECT toUInt64(number % 300007) AS k, number AS v FROM numbers(1000000) LIMIT 2 BY k);
SELECT count(), sum(v) FROM (SELECT toUInt32(number % 1000) AS a, toUInt32(intDiv(number, 1000) % 300) AS b, number AS v FROM numbers(1000000) LIMIT 2 BY a, b);
SELECT count(), sum(v) FROM (SELECT toUInt32(number % 1000) AS a, toUInt32(intDiv(number, 1000) % 300) AS b, number AS v FROM numbers(1000000) LIMIT 1, 2 BY a, b);
SELECT count(), sum(v) FROM (SELECT toUInt64(number % 1000) AS a, toUInt32(intDiv(number, 1000) % 300) AS b, number AS v FROM numbers(1000000) LIMIT 2 BY a, b);
SELECT count(), sum(v) FROM (SELECT if(number % 300007 = 0, NULL, toUInt64(number % 300007)) AS k, number AS v FROM numbers(1000000) LIMIT 2 BY k);
SELECT count(), sum(v) FROM (SELECT toUInt64(intDiv(number, 3) % 300007) AS k, number AS v FROM numbers(1000000) LIMIT 2 BY k);
