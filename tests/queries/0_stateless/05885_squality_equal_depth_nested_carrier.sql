SET in_null_as_unknown = 1;

SELECT [[toUInt8(1)]] IN [[CAST(toUInt64(1) AS Dynamic)]],
    [[toUInt8(1)]] IN materialize([[CAST(toUInt64(1) AS Dynamic)]]);

SELECT count()
FROM (SELECT [[toUInt8(1)]] AS a)
WHERE a IN materialize([[CAST(toUInt64(1) AS Dynamic)]]);
