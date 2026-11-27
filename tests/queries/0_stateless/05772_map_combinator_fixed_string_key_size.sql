-- A serialized state of a -Map aggregate function with a `FixedString(N)` key must have keys of exactly N bytes.

SELECT groupArrayStateMapTupleMerge(CAST(unhex('010161012A0101620107') AS AggregateFunction(groupArrayStateMapTuple, Tuple(Map(FixedString(1), UInt8), Map(FixedString(1), UInt8))))) FORMAT Null;
SELECT groupArrayStateMapTupleMerge(CAST(unhex('0100012A010261620107') AS AggregateFunction(groupArrayStateMapTuple, Tuple(Map(FixedString(1), UInt8), Map(FixedString(1), UInt8))))); -- { serverError INCORRECT_DATA }

SELECT anyMapMerge(CAST(unhex('0101610105') AS AggregateFunction(anyMap, Map(FixedString(1), UInt8))));
SELECT anyMapMerge(CAST(unhex('010261620105') AS AggregateFunction(anyMap, Map(FixedString(1), UInt8)))); -- { serverError INCORRECT_DATA }
SELECT anyMapMerge(CAST(unhex('01000105') AS AggregateFunction(anyMap, Map(FixedString(1), UInt8)))); -- { serverError INCORRECT_DATA }
