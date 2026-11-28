-- Tags: no-fasttest
-- Tag no-fasttest: ORC is not built in fasttest.

-- A truncated file of 104 bytes declares one list of 2^32 doubles. The children of lists and maps are read in chunks
-- that grow with the decoded data, so the missing data is reported without allocating memory for the declared length.
SELECT * FROM format(ORC, 'x Array(Float64)', unhex('4f5243ff808080801000000000000000000a060802100118060a060801100218081202080012020800120208000803102d1a0a08031000180e201c28012208080c1201011a01782205080a1201022202080630014000082910002202000c280082f403034f524311'))
SETTINGS max_memory_usage = 134217728; -- { serverError INCORRECT_DATA }

-- The children of lists and maps that take several chunks (of at least 2^20 values) are read back intact, including
-- a null that appears only in a later chunk, dictionary-encoded strings, and nested lists, maps and structs.
DROP TABLE IF EXISTS t_05894;
CREATE TABLE t_05894
(
    id UInt64,
    a Array(Nullable(Int64)),
    m Map(String, Array(String)),
    mm Map(String, UInt64),
    t Array(Tuple(s Nullable(String), d Decimal(20, 2), ts DateTime64(9, 'UTC')))
)
ENGINE = Memory;

INSERT INTO t_05894 SELECT
    number,
    arrayMap(x -> if(x = 2000000 + number, NULL, x), range(number * 1500000)),
    map('k', arrayMap(x -> toString(x % 7), range(number * 800000))),
    mapFromArrays(arrayMap(x -> toString(x), range(number * 600000)), range(number * 600000)),
    arrayMap(x -> (if(x % 3 = 0, NULL, toString(x % 5)), toDecimal128(x, 2), toDateTime64(x, 9, 'UTC')), range(number * 700000))
FROM numbers(3);

INSERT INTO FUNCTION file(current_database() || '_05894.orc', ORC)
SELECT * FROM t_05894
SETTINGS output_format_orc_dictionary_key_size_threshold = 1, engine_file_truncate_on_insert = 1;

SELECT count(), sum(length(a)), sum(arrayCount(x -> x IS NULL, a)), sum(sipHash64(id, a, m, mm, t)) = (SELECT sum(sipHash64(id, a, m, mm, t)) FROM t_05894)
FROM file(current_database() || '_05894.orc', ORC, 'id UInt64, a Array(Nullable(Int64)), m Map(String, Array(String)), mm Map(String, UInt64), t Array(Tuple(s Nullable(String), d Decimal(20, 2), ts DateTime64(9, \'UTC\')))')
SETTINGS input_format_orc_dictionary_as_low_cardinality = 0;

SELECT count(), sum(length(a)), sum(sipHash64(id, a, m, mm, t)) = (SELECT sum(sipHash64(id, a, m, mm, t)) FROM t_05894)
FROM file(current_database() || '_05894.orc', ORC, 'id UInt64, a Array(Nullable(Int64)), m Map(String, Array(String)), mm Map(String, UInt64), t Array(Tuple(s Nullable(String), d Decimal(20, 2), ts DateTime64(9, \'UTC\')))')
SETTINGS input_format_orc_dictionary_as_low_cardinality = 1;

DROP TABLE t_05894;
