-- A serialized state of a -Map aggregate function with a `FixedString(N)` key must have keys of exactly N bytes.

SELECT anyMapMerge(CAST(unhex('0101610105') AS AggregateFunction(anyMap, Map(FixedString(1), UInt8))));
SELECT anyMapMerge(CAST(unhex('010261620105') AS AggregateFunction(anyMap, Map(FixedString(1), UInt8)))); -- { serverError INCORRECT_DATA }
SELECT anyMapMerge(CAST(unhex('01000105') AS AggregateFunction(anyMap, Map(FixedString(1), UInt8)))); -- { serverError INCORRECT_DATA }
