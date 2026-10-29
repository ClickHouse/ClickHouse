-- Tags: no-fasttest
-- Tag no-fasttest: the Cap'n Proto format is not built in the Fast test build.

-- The only Cap'n Proto shape ClickHouse accepts for a Map column is the one documented in the
-- CapnProtoMapSerializer constructor comment in src/Formats/CapnProtoSerializer.cpp:
--     struct Map { struct Entry { key @0 :Key; value @1 :Value; } entries @0 :List(Entry); }
-- That shape has to round-trip the map value, and every other shape has to be rejected with
-- CAPN_PROTO_BAD_CAST rather than silently accepted.

-- Parallel formatting/parsing changes how many format objects are built, not the schema check
-- itself, but both are randomized, so pin them to keep the run deterministic.
SET output_format_parallel_formatting = 0;
SET input_format_parallel_parsing = 0;

SET format_schema_source = 'string';
-- Entry/OneField/Unnamed/ListPrim/ListUnion/ListOne/Good are the building blocks; every Root*
-- struct has a single field `m`, which is what the `m Map(String, Int32)` column binds to.
SET format_schema = '@0xb5e1c0d9a4f27361; struct Entry { key @0 :Text; value @1 :Int32; } struct OneField { key @0 :Text; } struct Unnamed { union { a @0 :Int32; b @1 :Text; } } struct ListPrim { entries @0 :List(Int32); } struct ListUnion { entries @0 :List(Unnamed); } struct ListOne { entries @0 :List(OneField); } struct Good { entries @0 :List(Entry); } struct RootPlain { m @0 :Int32; } struct RootUnion { m @0 :Unnamed; } struct RootTwo { m @0 :Entry; } struct RootNotList { m @0 :OneField; } struct RootListPrim { m @0 :ListPrim; } struct RootListUnion { m @0 :ListUnion; } struct RootListOne { m @0 :ListOne; } struct RootGood { m @0 :Good; }';

-- The documented shape: a struct holding one list of two-field entry structs. Both the writer
-- and the reader have to build the Map serializer, so the value has to come back unchanged.
SET format_schema_message_name = 'RootGood';
DROP TABLE IF EXISTS t_capnproto_map;
CREATE TABLE t_capnproto_map (m Map(String, Int32)) ENGINE = File(CapnProto);
INSERT INTO t_capnproto_map VALUES (map('a', 1, 'b', 2));
SELECT m FROM t_capnproto_map;
DROP TABLE t_capnproto_map;

-- `m` is not a struct at all.
SELECT count() FROM format(CapnProto, 'm Map(String, Int32)', '') SETTINGS format_schema_message_name = 'RootPlain'; -- { serverError CAPN_PROTO_BAD_CAST }

-- `m` is a struct whose fields are all members of an unnamed union.
SELECT count() FROM format(CapnProto, 'm Map(String, Int32)', '') SETTINGS format_schema_message_name = 'RootUnion'; -- { serverError CAPN_PROTO_BAD_CAST }

-- `m` is a struct with two fields instead of the one list field.
SELECT count() FROM format(CapnProto, 'm Map(String, Int32)', '') SETTINGS format_schema_message_name = 'RootTwo'; -- { serverError CAPN_PROTO_BAD_CAST }

-- `m` is a struct with one field, but that field is not a list.
SELECT count() FROM format(CapnProto, 'm Map(String, Int32)', '') SETTINGS format_schema_message_name = 'RootNotList'; -- { serverError CAPN_PROTO_BAD_CAST }

-- `m` is a struct with one list field, but the list elements are not structs.
SELECT count() FROM format(CapnProto, 'm Map(String, Int32)', '') SETTINGS format_schema_message_name = 'RootListPrim'; -- { serverError CAPN_PROTO_BAD_CAST }

-- The entry struct is a struct whose fields are all members of an unnamed union.
SELECT count() FROM format(CapnProto, 'm Map(String, Int32)', '') SETTINGS format_schema_message_name = 'RootListUnion'; -- { serverError CAPN_PROTO_BAD_CAST }

-- The entry struct has one field instead of the key/value pair.
SELECT count() FROM format(CapnProto, 'm Map(String, Int32)', '') SETTINGS format_schema_message_name = 'RootListOne'; -- { serverError CAPN_PROTO_BAD_CAST }
