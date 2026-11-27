SELECT arrayShiftLeft(CAST([1, 2, 3], 'Dynamic'), 1);
SELECT arrayShiftRight([1, 2, 3], CAST(1, 'Dynamic'));
SELECT arrayShiftRight([1, 2, 3], CAST(1, 'Variant(UInt8, String)'));
SELECT arrayShiftLeft([1, 2, 3], 1, CAST(9, 'Variant(UInt8, String)'));

-- A `Dynamic` or `Variant` default that the array elements already accept keeps the values and the `Array` result type.
SELECT arrayShiftLeft(CAST([1, 2, 3], 'Array(Dynamic)'), 1, CAST(NULL, 'Dynamic'));
SELECT arrayShiftLeft(CAST([1, 2, 3], 'Array(Dynamic)'), 0, CAST(NULL, 'Dynamic'));
SELECT arrayShiftLeft(CAST([1, 2, 3], 'Array(Dynamic)'), 1, CAST(9, 'Dynamic')) AS r, toTypeName(r), arrayPopBack(r);
SELECT arrayShiftLeft(CAST([1, 2, 3], 'Array(Dynamic)'), 1, if(number = 1, CAST(NULL, 'Dynamic'), number::Dynamic)) FROM numbers(3);
SELECT arrayShiftLeft(CAST([1, 2, 3], 'Array(Variant(UInt8, String))'), 1, CAST(9, 'Variant(UInt8, String)')) AS r, toTypeName(r);
SELECT arrayShiftRight(CAST([1, 2, 3], 'Array(Variant(UInt8, String))'), 1, CAST(9, 'Variant(UInt8, String)'));
SELECT arrayShiftLeft(CAST([1, 2, 3], 'Array(Variant(UInt8, String))'), 1, CAST(NULL, 'Variant(UInt8, String)'));

-- A `Dynamic` or `Variant` array is resolved before such a default, so the default is decided for the array's element type.
SELECT arrayShiftLeft(CAST(CAST([1, 2, 3], 'Array(Dynamic)'), 'Variant(Array(Dynamic))'), 1, CAST(NULL, 'Dynamic'));
SELECT arrayPopBack(arrayShiftLeft(CAST(CAST([1, 2, 3], 'Array(Dynamic)'), 'Variant(Array(Dynamic))'), 1, CAST(9, 'Dynamic')));
SELECT arrayShiftLeft(CAST(CAST([1, 2, 3], 'Array(Dynamic)'), 'Variant(Array(Dynamic))'), 1, if(number = 1, CAST(NULL, 'Dynamic'), number::Dynamic)) FROM numbers(3);
SELECT arrayShiftLeft(CAST(CAST([1, 2, 3], 'Array(Dynamic)'), 'Dynamic'), 1, CAST(NULL, 'Dynamic'));
