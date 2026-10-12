SELECT
    mapGetOrDefault(map('a', 1, 'b', 2), 'a', 99),
    mapGetOrDefault(map('a', 1, 'b', 2), 'missing', 99),
    mapGetOrDefault(map('a', 1, 'a', 2), 'a', 99)
FORMAT TabSeparatedRaw;

SELECT
    mapGetOrDefault(map('a', 0), 'a', 42),
    mapGetOrDefault(map('a', 0), 'missing', 42)
FORMAT TabSeparatedRaw;

SELECT
    mapGetOrDefault(CAST(map('a', NULL), 'Map(String, Nullable(UInt8))'), 'a', 42),
    mapGetOrDefault(CAST(map('a', NULL), 'Map(String, Nullable(UInt8))'), 'missing', 42)
FORMAT TabSeparatedRaw;

SELECT
    mapGetOrDefault(CAST(map('a', toUInt8(1)), 'Map(String, UInt8)'), 'missing', NULL),
    toTypeName(mapGetOrDefault(CAST(map('a', toUInt8(1)), 'Map(String, UInt8)'), 'missing', NULL))
FORMAT TabSeparatedRaw;

SELECT
    mapGetOrDefault(CAST(map('a', toUInt8(1)), 'Map(String, UInt8)'), 'missing', toUInt16(300)),
    toTypeName(mapGetOrDefault(CAST(map('a', toUInt8(1)), 'Map(String, UInt8)'), 'missing', toUInt16(300)))
FORMAT TabSeparatedRaw;

SELECT mapGetOrDefault(map(), 'a', 99), toTypeName(mapGetOrDefault(map(), 'a', 99)) FORMAT TabSeparatedRaw;

SELECT number, mapGetOrDefault(map('a', number), 'missing', number + 100) FROM numbers(3) ORDER BY number FORMAT TabSeparatedRaw;

SELECT number, mapGetOrDefault(map('a', number, 'b', number + 10), if(number = 0, 'a', 'b'), 999) FROM numbers(2) ORDER BY number FORMAT TabSeparatedRaw;

SELECT number, mapGetOrDefault(map('a', 10, 'b', 20), if(number = 0, 'a', 'missing'), number + 100) FROM numbers(2) ORDER BY number FORMAT TabSeparatedRaw;

SELECT number, mapGetOrDefault(map('a', 10), 'missing', number + 200) FROM numbers(2) ORDER BY number FORMAT TabSeparatedRaw;

SELECT mapGetOrDefault(map(toUInt64(1), 'one'), toUInt8(1), 'missing') FORMAT TabSeparatedRaw;

SELECT mapGetOrDefault(map('a', 1), NULL, 42) FORMAT TabSeparatedRaw;

SELECT mapGetOrDefault(CAST(map(0, 'zero', 5, 'five'), 'Map(UInt8, String)'), toUInt16(256), 'missing') FORMAT TabSeparatedRaw;

SELECT mapGetOrDefault(CAST(map(0, 'zero', 5, 'five'), 'Map(LowCardinality(UInt8), String)'), toUInt16(256), 'missing') SETTINGS allow_suspicious_low_cardinality_types = 1 FORMAT TabSeparatedRaw;

SELECT
    mapGetOrDefault(CAST(map('a', 'x'), 'Map(LowCardinality(String), LowCardinality(String))'), 'a', 'missing'),
    toTypeName(mapGetOrDefault(CAST(map('a', 'x'), 'Map(LowCardinality(String), LowCardinality(String))'), 'a', 'missing'))
FORMAT TabSeparatedRaw;

SELECT mapGetOrDefault(
    CAST(map(CAST('first', 'Enum8(\'first\' = 1, \'second\' = 2)'), 10), 'Map(Enum8(\'first\' = 1, \'second\' = 2), UInt8)'),
    'first',
    99)
FORMAT TabSeparatedRaw;

SET short_circuit_function_evaluation = 'enable';
SELECT mapGetOrDefault(
    map('a', toInt64(1)),
    'a',
    intDiv(toInt64(number), toInt64(number) - toInt64(number)))
FROM numbers(1)
FORMAT TabSeparatedRaw;

SELECT throwIf(result != if(number = 0, toInt64(0), toInt64(10)))
FROM
(
    SELECT
        number,
        mapGetOrDefault(
            map('a', toInt64(number)),
            if(number = 0, 'a', 'missing'),
            intDiv(toInt64(10), toInt64(number))) AS result
    FROM numbers(2)
)
FORMAT Null;

SELECT throwIf(mapGetOrDefault(map(toFixedString('a', 4), 1), 'a', 99) != 1) FORMAT Null;

SELECT throwIf(
    mapGetOrDefault(
        CAST(map(toFixedString('a', 4), 1), 'Map(LowCardinality(FixedString(4)), UInt8)'),
        'a',
        99) != 1)
SETTINGS allow_suspicious_low_cardinality_types = 1
FORMAT Null;

SELECT throwIf(
    mapGetOrDefault(
        map(toDate('2024-01-01'), 'date'),
        toDateTime('2024-01-01 00:00:00'),
        'missing') != 'date')
FORMAT Null;

SELECT mapGetOrDefault([1, 2], 1, 0); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT mapGetOrDefault(map('a', 1), 'a'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT mapGetOrDefault(map('a', 1), 'missing', [1, 2]) SETTINGS use_variant_as_common_type = 0; -- { serverError NO_COMMON_TYPE }

-- NaN keys must match using Map key semantics, including mixed Float widths.
SELECT throwIf(mapGetOrDefault(map(toFloat64('nan'), 'hit'), toFloat64('nan'), 'miss') != 'hit') FORMAT Null;
SELECT throwIf(mapGetOrDefault(map(toFloat32('nan'), 'hit'), toFloat64('nan'), 'miss') != 'hit') FORMAT Null;
SELECT throwIf(mapGetOrDefault(map(toFloat64('nan'), 'first', toFloat64('nan'), 'second'), toFloat64('nan'), 'miss') != 'first') FORMAT Null;

-- Dynamic map keys and needles: NaN hit, ordinary hit, ordinary miss.
SELECT throwIf(
    mapGetOrDefault(
        map(if(number = 0, toFloat64('nan'), toFloat64(1)), if(number = 0, 'nan', 'one')),
        if(number = 0, toFloat64('nan'), if(number = 1, toFloat64(1), toFloat64(2))),
        'miss')
    != multiIf(number = 0, 'nan', number = 1, 'one', 'miss'))
FROM numbers(3) FORMAT Null;

SELECT throwIf(
    mapGetOrDefault(map(toFloat64('nan'), 'hit'), CAST(NULL, 'Nullable(Float64)'), 'miss') != 'miss')
FORMAT Null;

-- Cross-type Date / DateTime lookup through a non-constant Map.
SELECT throwIf(
    mapGetOrDefault(
        map(toDate('2024-01-01') + toIntervalDay(number), 'hit'),
        toDateTime('2024-01-01 00:00:00') + toIntervalDay(number),
        'miss') != 'hit')
FROM numbers(2) FORMAT Null;
