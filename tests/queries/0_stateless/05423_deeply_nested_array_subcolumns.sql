-- Enumerating the subcolumns of a deeply nested `Array` type used to take time cubic in the nesting depth
-- (seconds of CPU for a depth of 2000 in a release build), because `SerializationArray::supportsPooling`
-- walked the whole nested chain and was called for every level of every subcolumn.
-- The input is a chain of single-element MsgPack arrays (`0x91`), which schema inference turns into a deeply nested type.

SET input_format_msgpack_number_of_columns = 1, max_parser_depth = 100000, schema_inference_make_columns_nullable = 1;

DESC format(MsgPack, repeat('\x91', 2000) || '\x00') FORMAT Null;

SELECT toTypeName(c1) = repeat('Array(', 2000) || 'Nullable(Int64)' || repeat(')', 2000), c1.size0, length(toString(c1))
FROM format(MsgPack, repeat('\x91', 2000) || '\x00');

SELECT countSubstrings(toTypeName(c1.size1999), 'Array('), length(toString(c1.size1999))
FROM format(MsgPack, repeat('\x91', 2000) || '\x00');
