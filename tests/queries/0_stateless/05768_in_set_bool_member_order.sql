-- A typed `Bool` member holding the raw byte 2 is normalized to `true` wherever it sits in the set.
SELECT groupArray(n) FROM (SELECT n FROM values('n Int64', 0, 1, 2) WHERE n IN (0, reinterpret(toUInt8(2), 'Bool')));
SELECT groupArray(n) FROM (SELECT n FROM values('n Int64', 0, 1, 2) WHERE n IN (reinterpret(toUInt8(2), 'Bool'), 0));
SELECT groupArray(n) FROM (SELECT n FROM values('n Int64', 0, 1, 2) WHERE n IN (2, reinterpret(toUInt8(2), 'Bool')));
SELECT groupArray(n) FROM (SELECT n FROM values('n Int64', 0, 1, 2) WHERE n NOT IN (0, reinterpret(toUInt8(2), 'Bool')));
SELECT groupArray(n) FROM (SELECT n FROM values('n Float64', 0, 1, 2) WHERE n IN (0, reinterpret(toUInt8(2), 'Bool')));
SELECT groupArray(n) FROM (SELECT n FROM values('n UInt8', 0, 1, 2) WHERE n IN (0, reinterpret(toUInt8(2), 'Bool')));
