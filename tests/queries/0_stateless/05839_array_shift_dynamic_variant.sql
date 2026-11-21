SELECT arrayShiftLeft(CAST([1, 2, 3], 'Dynamic'), 1);
SELECT arrayShiftRight([1, 2, 3], CAST(1, 'Dynamic'));
SELECT arrayShiftRight([1, 2, 3], CAST(1, 'Variant(UInt8, String)'));
SELECT arrayShiftLeft([1, 2, 3], 1, CAST(9, 'Variant(UInt8, String)'));
