-- Tags: no-fasttest
-- no-fasttest: MsgPack format is not available in the fasttest environment

-- A MsgPack map whose first key has no inferable type (nil, empty array, empty map, nested map with a nil key)
-- must be reported as "cannot determine type", not dereferenced.

SET input_format_msgpack_number_of_columns = 1;

-- {1: 1}
SELECT toTypeName(c1), c1 FROM format(MsgPack, '\x81\x01\x01');

-- {nil: 1}, {[]: 1}, {{}: 1}, {{nil: 1}: 1}
DESC format(MsgPack, '\x81\xc0\x01'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DESC format(MsgPack, '\x81\x90\x01'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DESC format(MsgPack, '\x81\x80\x01'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }
DESC format(MsgPack, '\x81\x81\xc0\x01\x01'); -- { serverError CANNOT_EXTRACT_TABLE_STRUCTURE }

-- {nil: 1}, {2: 3}: the type is taken from the second row.
SELECT toTypeName(c1), c1 FROM format(MsgPack, '\x81\xc0\x01\x81\x02\x03');

-- {nil: 1} with schema_inference_hints, which the error message suggests.
SELECT toTypeName(c1), c1 FROM format(MsgPack, '\x81\xc0\x01') SETTINGS schema_inference_hints = 'c1 Map(Int64, Int64)';
