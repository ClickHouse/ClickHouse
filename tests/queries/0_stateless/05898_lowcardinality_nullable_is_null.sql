-- UInt8, UInt16 and UInt32 indexes: the dictionary of one block has about 100, 1000 and 85000 entries.
SELECT 'UInt8', min(dumpColumnStructure(x) LIKE '%LowCardinality(size = 1000, UInt8(size = 1000)%'), sum(isNull(x)), sum(isNotNull(x)), sum(isNull(x) != isNull(n)), sum(isNotNull(x) != isNotNull(n))
FROM (SELECT if(number % 7 = 0, NULL, toString(number % 100)) AS n, toLowCardinality(n) AS x FROM numbers(1000))
SETTINGS max_block_size = 1000;

SELECT 'UInt16', min(dumpColumnStructure(x) LIKE '%LowCardinality(size = 10000, UInt16(size = 10000)%'), sum(isNull(x)), sum(isNotNull(x)), sum(isNull(x) != isNull(n)), sum(isNotNull(x) != isNotNull(n))
FROM (SELECT if(number % 7 = 0, NULL, toString(number % 1000)) AS n, toLowCardinality(n) AS x FROM numbers(10000))
SETTINGS max_block_size = 10000;

SELECT 'UInt32', min(dumpColumnStructure(x) LIKE '%LowCardinality(size = 100000, UInt32(size = 100000)%'), sum(isNull(x)), sum(isNotNull(x)), sum(isNull(x) != isNull(n)), sum(isNotNull(x) != isNotNull(n))
FROM (SELECT if(number % 7 = 0, NULL, toString(number)) AS n, toLowCardinality(n) AS x FROM numbers(100000))
SETTINGS max_block_size = 100000;

-- A constant argument and an empty input.
SELECT isNull(toLowCardinality(CAST(NULL, 'Nullable(String)'))), isNotNull(toLowCardinality(toNullable('a')));
SELECT count() FROM (SELECT toLowCardinality(toNullable(toString(number))) AS x FROM numbers(0)) WHERE x IS NULL;
