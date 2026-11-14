-- A `Variant` or `Dynamic` member of a composite constant is converted from its own alternative. A member
-- that is not representable in its target makes the whole value not representable, and `IN` skips it.
SELECT 'unrepresentable members';
SELECT CAST([1], 'Array(UInt8)') IN (CAST([256], 'Array(Dynamic)'));
SELECT ['x'] IN (CAST([NULL], 'Array(Dynamic)'));
SELECT tuple(toUInt8(1)) IN (tuple(CAST(256, 'Dynamic')));
SELECT map('k', toUInt8(1)) IN (CAST(map('k', 256), 'Map(String, Dynamic)'));
SELECT CAST([1], 'Array(UInt8)') IN (CAST([1], 'Array(Dynamic)'));
SELECT x FROM values('x Array(UInt8)', CAST([256], 'Array(Dynamic)')); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT x FROM values('x Array(String)', CAST([NULL], 'Array(Dynamic)')); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT x FROM values('x Array(Nullable(String))', CAST([NULL], 'Array(Dynamic)'));

-- A top-level `Variant` or `Dynamic` can hold a composite that has `Variant` or `Dynamic` members of its own.
SELECT 'top-level carrier holding a composite with nested carriers';
SELECT (1, ['2020-01-01']) IN ((1, CAST(CAST([toDate('2020-01-01')], 'Array(Dynamic)'), 'Dynamic')));
SELECT (1, ['18262']) IN ((1, CAST(CAST([toDate('2020-01-01')], 'Array(Dynamic)'), 'Dynamic')));
SELECT (1, ['2020-01-01']) IN ((1, CAST(CAST([toDate('2020-01-01')], 'Array(Dynamic)'), 'Variant(Array(Dynamic), UInt64)')));
SELECT x FROM values('x Array(String)', CAST(CAST([toDate('2020-01-01')], 'Array(Dynamic)'), 'Dynamic'));
-- The same value in the shared variant of a `Dynamic`.
SELECT x FROM values('x Array(String)', CAST(CAST([toDate('2020-01-01')], 'Array(Dynamic)'), 'Dynamic(max_types=0)'));

-- A composite with `Variant` or `Dynamic` members converted to a `String` as a whole.
SELECT 'whole composite to String';
SELECT CAST(CAST([toDate('2020-01-01')], 'Array(Dynamic)'), 'String');
SELECT x FROM values('x String', CAST([toDate('2020-01-01')], 'Array(Dynamic)'));
SELECT x FROM values('x String', CAST((true, toDate('2020-01-02')), 'Tuple(Variant(Bool, String), Dynamic)'));
SELECT x FROM values('x Tuple(String)', tuple(CAST([toDate('2020-01-01')], 'Array(Dynamic)')));
