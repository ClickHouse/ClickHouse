-- A Size wider than UInt32 must be rejected instead of being reduced modulo 2^32.
SELECT timeSlots(toDateTime('2000-01-02 03:04:05', 'UTC'), toUInt32(10000), 4294967896); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT timeSlots(toDateTime('2000-01-02 03:04:05', 'UTC'), toUInt32(10000), 4294967296); -- { serverError ARGUMENT_OUT_OF_BOUND }
SELECT timeSlots(materialize(toDateTime('2000-01-02 03:04:05', 'UTC')), toUInt32(10000), toUInt64(18446744073709551615)); -- { serverError ARGUMENT_OUT_OF_BOUND }
-- Sizes that fit in UInt32 are still accepted at any integer width.
SELECT timeSlots(toDateTime('2000-01-02 03:04:05', 'UTC'), toUInt32(10000), 4294967295);
SELECT timeSlots(toDateTime('2000-01-02 03:04:05', 'UTC'), toUInt32(600), toUInt64(600));
