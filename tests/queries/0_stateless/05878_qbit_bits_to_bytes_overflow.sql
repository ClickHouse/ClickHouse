-- https://github.com/ClickHouse/ClickHouse/issues/133909: a size near 2^64 in the internal form of the transposed
-- distance functions must not overflow `DataTypeQBit::bitsToBytes` (caught by the unsigned-overflow UBSan check).

-- Non-strided internal form: `qbit_size` near 2^64.
SELECT dotProductTransposedQuantized(toFixedString('a', 1), toUInt64(-4), [1.0]::Array(Float32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT L2DistanceTransposed(toFixedString('a', 1), toUInt64(-1), [1.0]::Array(Float32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Strided internal form: `stride` and `used_dims` near 2^64.
SELECT dotProductTransposed(toFixedString('a', 1), toUInt64(-4), toUInt64(-4), [1.0]::Array(Float32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT cosineDistanceTransposed(toFixedString('a', 1), toUInt64(-7), toUInt64(-7), [1.0]::Array(Float32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- The maximum QBit dimension is 1048576.
SELECT toTypeName(CAST(NULL, 'Nullable(QBit(Int8, 1048576))'));
SELECT CAST(NULL, 'Nullable(QBit(Int8, 1048577))'); -- { serverError BAD_ARGUMENTS }
SELECT CAST(NULL, 'Nullable(QBit(Int8, 2097152, 1048576))'); -- { serverError BAD_ARGUMENTS }

-- Sizes above the maximum are not accepted as the internal form, even when the bit-plane sizes match them.
SET allow_suspicious_fixed_string_types = 1;
SELECT L2DistanceTransposed(CAST('' AS FixedString(131073)), toUInt64(1048584), [1.0]::Array(Float32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT L2DistanceTransposed(CAST('' AS FixedString(131073)), toUInt64(1048584), toUInt64(1048584), [1.0]::Array(Float32)); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'ok';
