-- conv converts a Bool-backed first argument by its stored value, as toUInt8 does
SELECT conv(true, 10, 2), conv(false, 10, 2), conv(true, 36, 10), conv(false, 16, 10);
SELECT b, conv(b, 10, 2), conv(b, 16, 10), conv(b, 36, 10), conv(toUInt8(b), 36, 10) FROM (SELECT arrayJoin([false, true]) AS b) ORDER BY b;
SELECT conv(b, 10, 2) FROM (SELECT arrayJoin([true, NULL, false]::Array(Nullable(Bool))) AS b);
SELECT conv(CAST(true AS SimpleAggregateFunction(any, Bool)), 10, 2), conv(CAST(false AS SimpleAggregateFunction(any, Bool)), 36, 10);
SELECT conv(CAST(toUInt16(2) AS SimpleAggregateFunction(sumWithOverflow, Bool)), 10, 2);
