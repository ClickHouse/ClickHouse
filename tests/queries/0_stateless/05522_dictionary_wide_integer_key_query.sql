-- Direct and cache dictionaries with a ClickHouse source send their keys to the source in a query.
-- Keys of wide integer and Decimal types must not be parsed there as Float64 and lose precision.
-- https://github.com/ClickHouse/ClickHouse/issues/47965

DROP DICTIONARY IF EXISTS dict_uint256_direct;
DROP DICTIONARY IF EXISTS dict_uint256_cache;
DROP DICTIONARY IF EXISTS dict_int128_direct;
DROP DICTIONARY IF EXISTS dict_int128_cache;
DROP DICTIONARY IF EXISTS dict_decimal256_direct;
DROP DICTIONARY IF EXISTS dict_decimal256_cache;
DROP TABLE IF EXISTS table_uint256;
DROP TABLE IF EXISTS table_int128;
DROP TABLE IF EXISTS table_decimal256;

CREATE TABLE table_uint256 (k UInt256, v String) ENGINE = Memory;
INSERT INTO table_uint256 VALUES (1233333300000000000000000000000000000000000, 'lottery'), (42, 'small');

CREATE TABLE table_int128 (k Int128, v String) ENGINE = Memory;
INSERT INTO table_int128 VALUES (-123456789012345678901234567890123456789, 'negative'), (-42, 'small');

CREATE TABLE table_decimal256 (k Decimal256(2), v String) ENGINE = Memory;
INSERT INTO table_decimal256 VALUES ('123456789012345678901234567890123456789.01', 'decimal'), (4.2, 'small');

CREATE DICTIONARY dict_uint256_direct (k UInt256, v String) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'table_uint256')) LAYOUT(COMPLEX_KEY_DIRECT());
CREATE DICTIONARY dict_uint256_cache (k UInt256, v String) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'table_uint256')) LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 100)) LIFETIME(0);

CREATE DICTIONARY dict_int128_direct (k Int128, v String) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'table_int128')) LAYOUT(COMPLEX_KEY_DIRECT());
CREATE DICTIONARY dict_int128_cache (k Int128, v String) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'table_int128')) LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 100)) LIFETIME(0);

CREATE DICTIONARY dict_decimal256_direct (k Decimal256(2), v String) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'table_decimal256')) LAYOUT(COMPLEX_KEY_DIRECT());
CREATE DICTIONARY dict_decimal256_cache (k Decimal256(2), v String) PRIMARY KEY k
SOURCE(CLICKHOUSE(TABLE 'table_decimal256')) LAYOUT(COMPLEX_KEY_CACHE(SIZE_IN_CELLS 100)) LIFETIME(0);

SELECT 'UInt256';
SELECT
    dictGet('dict_uint256_direct', 'v', tuple(toUInt256('1233333300000000000000000000000000000000000'))),
    dictGet('dict_uint256_cache', 'v', tuple(toUInt256('1233333300000000000000000000000000000000000'))),
    dictGet('dict_uint256_direct', 'v', tuple(toUInt256(42))),
    dictGet('dict_uint256_cache', 'v', tuple(toUInt256(42)));
SELECT dictHas('dict_uint256_direct', tuple(toUInt256('1233333300000000000000000000000000000000001')));

SELECT 'Int128';
SELECT
    dictGet('dict_int128_direct', 'v', tuple(toInt128('-123456789012345678901234567890123456789'))),
    dictGet('dict_int128_cache', 'v', tuple(toInt128('-123456789012345678901234567890123456789'))),
    dictGet('dict_int128_direct', 'v', tuple(toInt128(-42))),
    dictGet('dict_int128_cache', 'v', tuple(toInt128(-42)));

SELECT 'Decimal256';
SELECT
    dictGet('dict_decimal256_direct', 'v', tuple(toDecimal256('123456789012345678901234567890123456789.01', 2))),
    dictGet('dict_decimal256_cache', 'v', tuple(toDecimal256('123456789012345678901234567890123456789.01', 2))),
    dictGet('dict_decimal256_direct', 'v', tuple(toDecimal256('4.2', 2))),
    dictGet('dict_decimal256_cache', 'v', tuple(toDecimal256('4.2', 2)));

DROP DICTIONARY dict_uint256_direct;
DROP DICTIONARY dict_uint256_cache;
DROP DICTIONARY dict_int128_direct;
DROP DICTIONARY dict_int128_cache;
DROP DICTIONARY dict_decimal256_direct;
DROP DICTIONARY dict_decimal256_cache;
DROP TABLE table_uint256;
DROP TABLE table_int128;
DROP TABLE table_decimal256;
