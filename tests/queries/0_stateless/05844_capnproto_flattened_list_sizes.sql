-- Tags: no-fasttest
-- Tag no-fasttest: the Cap'n Proto format is not built in the Fast test build.

-- Columns with flattened names (like `l.x` and `l.y`) are written to the same list, so the sizes of their arrays must match.
-- For top-level columns it is also checked on `INSERT` before the format, so the flattened names are put inside a `Tuple`
-- to reach the check in the format itself, like `SELECT ... FORMAT CapnProto` does.

SET output_format_parallel_formatting = 0;
SET input_format_parallel_parsing = 0;
SET engine_file_truncate_on_insert = 1;

SET format_schema_source = 'string';
SET format_schema = '@0xeed9ecfa9f9df117; struct E { x @0 :UInt64; y @1 :UInt64; } struct T { l @0 :List(E); } struct InGroup { t :group { l @0 :List(E); } } struct InStruct { t @0 :T; }';

SELECT 'a list in a group';
SET format_schema_message_name = 'InGroup';
INSERT INTO FUNCTION file(currentDatabase() || '_05844_1.capnp', CapnProto, 't Tuple(`l.x` Array(UInt64), `l.y` Array(UInt64))') SELECT ([], [2]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
INSERT INTO FUNCTION file(currentDatabase() || '_05844_1.capnp', CapnProto, 't Tuple(`l.x` Array(UInt64), `l.y` Array(UInt64))') SELECT ([1, 2], [2]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
INSERT INTO FUNCTION file(currentDatabase() || '_05844_1.capnp', CapnProto, 't Tuple(`l.x` Array(UInt64), `l.y` Array(UInt64))') VALUES (([], [])), (([1], [2])), (([3, 4], [5, 6]));
SELECT * FROM file(currentDatabase() || '_05844_1.capnp', CapnProto, 't Tuple(`l.x` Array(UInt64), `l.y` Array(UInt64))') ORDER BY length(t.1);

SELECT 'a list in a struct';
SET format_schema_message_name = 'InStruct';
INSERT INTO FUNCTION file(currentDatabase() || '_05844_2.capnp', CapnProto, 't Tuple(`l.x` Array(UInt64), `l.y` Array(UInt64))') SELECT ([], [2]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
INSERT INTO FUNCTION file(currentDatabase() || '_05844_2.capnp', CapnProto, 't Tuple(`l.x` Array(UInt64), `l.y` Array(UInt64))') SELECT ([1, 2], [2]); -- { serverError SIZES_OF_ARRAYS_DONT_MATCH }
INSERT INTO FUNCTION file(currentDatabase() || '_05844_2.capnp', CapnProto, 't Tuple(`l.x` Array(UInt64), `l.y` Array(UInt64))') VALUES (([], [])), (([1], [2])), (([3, 4], [5, 6]));
SELECT * FROM file(currentDatabase() || '_05844_2.capnp', CapnProto, 't Tuple(`l.x` Array(UInt64), `l.y` Array(UInt64))') ORDER BY length(t.1);
