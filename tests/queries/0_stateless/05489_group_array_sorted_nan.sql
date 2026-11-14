-- groupArraySorted over floats must order NaN last, like arraySort, regardless of N.

SELECT groupArraySorted(3)(x) FROM values('x Float64', (nan), (3), (-inf), (1));
SELECT groupArraySorted(3)(x) FROM values('x Float32', (nan), (3), (-inf), (1));
SELECT groupArraySorted(3)(x) FROM values('x BFloat16', (nan), (3), (-inf), (1));
SELECT groupArraySorted(10)(x) FROM values('x Float64', (nan), (3), (-inf), (1));

SELECT groupArraySorted(3)(x) = arraySlice(arraySort(groupArray(x)), 1, 3) FROM values('x Float64', (nan), (3), (-inf), (1));
SELECT groupArraySorted(3)(x) = arraySlice(arraySort(groupArray(x)), 1, 3) FROM values('x Float32', (nan), (3), (-inf), (1));

SELECT groupArraySorted(200)(x) = arraySlice(arraySort(groupArray(x)), 1, 200)
FROM (SELECT if(number % 100 = 0, nan, toFloat64(sipHash64(number) % 1000000)) AS x FROM numbers(2000));

SELECT groupArraySorted(200)(x) = arraySlice(arraySort(groupArray(x)), 1, 200)
FROM (SELECT toFloat32(if(number % 100 = 0, nan, sipHash64(number) % 1000000)) AS x FROM numbers(2000));

SELECT groupArraySorted(200)(x) = arraySlice(arraySort(groupArray(x)), 1, 200)
FROM (SELECT toBFloat16(if(number % 100 = 0, nan, sipHash64(number) % 1000000)) AS x FROM numbers(2000));

-- NaN with the sign bit set.
SELECT groupArraySorted(200)(x) = arraySlice(arraySort(groupArray(x)), 1, 200)
FROM (SELECT if(number % 100 = 0, reinterpretAsFloat64(toUInt64(18444492273895866368)), toFloat64(sipHash64(number) % 1000000)) AS x FROM numbers(2000));

SELECT groupArraySorted(200)(x) = arraySlice(arraySort(groupArray(x)), 1, 200)
FROM (SELECT if(number % 100 = 0, reinterpretAsFloat32(toUInt32(4290772992)), toFloat32(sipHash64(number) % 1000000)) AS x FROM numbers(2000));

-- Sort strategy (limit above 1000000).
SELECT groupArraySorted(1000001)(x) = arraySort(groupArray(x))
FROM (SELECT if(number % 100 = 0, nan, toFloat64(sipHash64(number) % 1000000)) AS x FROM numbers(2000));

SELECT groupArraySorted(1000001)(x) = arraySort(groupArray(x))
FROM (SELECT if(number % 100 = 0, reinterpretAsFloat64(toUInt64(18444492273895866368)), toFloat64(sipHash64(number) % 1000000)) AS x FROM numbers(2000));

SELECT groupArraySorted(1000001)(x) = arraySort(groupArray(x))
FROM (SELECT if(number % 100 = 0, reinterpretAsFloat32(toUInt32(4290772992)), toFloat32(sipHash64(number) % 1000000)) AS x FROM numbers(2000));

-- Merge of partial states.
SELECT groupArraySortedMerge(200)(s) = (
    SELECT arraySlice(arraySort(groupArray(x)), 1, 200)
    FROM (SELECT if(number % 100 = 0, nan, toFloat64(sipHash64(number) % 1000000)) AS x FROM numbers(2000)))
FROM (
    SELECT groupArraySortedState(200)(x) AS s
    FROM (SELECT number, if(number % 100 = 0, nan, toFloat64(sipHash64(number) % 1000000)) AS x FROM numbers(2000))
    GROUP BY number % 3);

SELECT groupArraySortedMerge(200)(s) = (
    SELECT arraySlice(arraySort(groupArray(x)), 1, 200)
    FROM (SELECT toFloat32(if(number % 100 = 0, nan, sipHash64(number) % 1000000)) AS x FROM numbers(2000)))
FROM (
    SELECT groupArraySortedState(200)(x) AS s
    FROM (SELECT number, toFloat32(if(number % 100 = 0, nan, sipHash64(number) % 1000000)) AS x FROM numbers(2000))
    GROUP BY number % 3);
