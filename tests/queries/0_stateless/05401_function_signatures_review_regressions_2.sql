-- Tags: no-fasttest
-- no-fasttest: `uniqTheta` requires the `datasketches` library

-- Sketches built from several `uniqTheta` arguments are accepted, mismatched ones are rejected.
SELECT finalizeAggregation(uniqThetaUnion(uniqThetaState(number, toString(number)), uniqThetaState(number, toString(number)))) FROM numbers(10);
SELECT finalizeAggregation(uniqThetaIntersect(uniqThetaState(number, toString(number)), uniqThetaState(number, toString(number)))) FROM numbers(10);
SELECT finalizeAggregation(uniqThetaNot(uniqThetaState(number, toString(number)), uniqThetaState(number, toString(number)))) FROM numbers(10);
SELECT uniqThetaUnion(uniqThetaState(number, toString(number)), uniqThetaState(number)) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT uniqThetaUnion(uniqThetaState(number), uniqThetaState(toString(number))) FROM numbers(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Pointwise operations on numeric indexed vectors require matching index and value types.
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(numericIndexedVectorBuild(map(toUInt8(1), toFloat64(10))), numericIndexedVectorBuild(map(toUInt8(1), toFloat64(20)))));
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(numericIndexedVectorBuild(map(toUInt8(1), toFloat64(10))), numericIndexedVectorBuild(map(toUInt32(1), toFloat64(20))))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(numericIndexedVectorBuild(map(toUInt8(1), toFloat64(10))), numericIndexedVectorBuild(map(toUInt8(1), toInt32(20))))); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Bit shifts, rotations and `bitTest` accept `IPv4` and `IPv6` operands.
SELECT bitShiftRight(toIPv4('192.168.0.1'), 8), bitShiftLeft(toIPv4('192.168.0.1'), 8), bitRotateLeft(toIPv4('192.168.0.1'), 8), bitRotateRight(toIPv4('192.168.0.1'), 8), bitTest(toIPv4('192.168.0.1'), 0);
SELECT bitShiftRight(toIPv6('::ffff'), 8), bitShiftLeft(toIPv6('::ffff'), 8);

-- `sign`, `roundAge` and `roundDuration` accept wide integers.
SELECT sign(toInt128(-1)), sign(toInt256(5)), sign(toUInt128(0)), sign(toUInt256(7));
SELECT roundAge(toUInt128(20)), roundAge(toInt256(30)), roundDuration(toUInt128(20)), roundDuration(toInt128(100));

-- `nowInBlock` takes a constant `FixedString` timezone.
SELECT timezoneOf(nowInBlock(toFixedString('Asia/Tokyo', 10))) SETTINGS allow_nonconst_timezone_arguments = 0;
SELECT timezoneOf(nowInBlock(toFixedString('Asia/Tokyo', 10))) SETTINGS allow_nonconst_timezone_arguments = 1;

-- `toInterval` parses a numeric `FixedString` value.
SELECT toInterval(toFixedString('2', 1), 'day') AS x, toTypeName(x);
