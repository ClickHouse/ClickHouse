-- Tags: no-fasttest
-- Tag no-fasttest: the Cap'n Proto format is not built in the Fast test build.

-- A Cap'n Proto group (`g :group { ... }`, and a named union `u :union { ... }` as well) is stored inline
-- in the struct that contains it, it is not a separate object behind a pointer. The serializer used to treat
-- a group field as a pointer field of the parent struct. If the parent struct had no pointers, the pointer slot
-- was outside of the struct, the values of the group were written over that slot and then interpreted as
-- a wire pointer, which led to out-of-bounds writes in the message arena.

-- Parallel formatting/parsing changes how many format objects are built, pin them to keep the run deterministic.
SET output_format_parallel_formatting = 0;
SET input_format_parallel_parsing = 0;

-- The `file` table function is used on purpose: it is what makes the writer and the reader use the schema below.
SET engine_file_truncate_on_insert = 1;

SET format_schema_source = 'string';
SET format_schema = '@0xd508eaf2a79b4a10; struct Inner { a @0 :UInt64; g :group { x @1 :UInt64; } } struct Elem { g :group { x @0 :UInt64; s @1 :Text; } } struct TwoGroups { g :group { x @0 :UInt64; } m1 @1 :UInt64; m2 @2 :UInt64; h :group { y @3 :UInt64; } } struct WithPointers { s1 @0 :Text; g :group { x @1 :UInt64; s @2 :Text; n :group { y @3 :UInt32; } } s2 @4 :Text; } struct NamedUnion { n :union { value @0 :UInt64; null @1 :Void; } m @2 :UInt64; t :union { null @3 :Void; value @4 :Text; } } struct Nested { blob @0 :Data; inner @1 :Inner; elems @2 :List(Elem); }';

SELECT 'two groups in a struct without pointers';
SET format_schema_message_name = 'TwoGroups';
-- The values of `g.x` look like far pointers, they used to crash the server or to fail with "invalid segment id".
INSERT INTO FUNCTION file(currentDatabase() || '_05843_1.capnp', CapnProto, '`g.x` UInt64, m1 UInt64, m2 UInt64, `h.y` UInt64') VALUES (6, 1, 2, 3), (20971526, 4, 5, 6), (117440518, 7, 8, 9), (16045690984833335023, 1229782938533634594, 3689348815028241476, 18446744073709551615);
SELECT * FROM file(currentDatabase() || '_05843_1.capnp', CapnProto, '`g.x` UInt64, m1 UInt64, m2 UInt64, `h.y` UInt64') ORDER BY m1;

SELECT 'the same as tuples';
INSERT INTO FUNCTION file(currentDatabase() || '_05843_2.capnp', CapnProto, 'g Tuple(x UInt64), m1 UInt64, m2 UInt64, h Tuple(y UInt64)') VALUES ((6), 1, 2, (3)), ((20971526), 4, 5, (6));
SELECT * FROM file(currentDatabase() || '_05843_2.capnp', CapnProto, 'g Tuple(x UInt64), m1 UInt64, m2 UInt64, h Tuple(y UInt64)') ORDER BY m1;

SELECT 'groups in a struct with pointers';
SET format_schema_message_name = 'WithPointers';
INSERT INTO FUNCTION file(currentDatabase() || '_05843_3.capnp', CapnProto, 's1 String, g Tuple(x UInt64, s String, n Tuple(y UInt32)), s2 String') VALUES ('a', (6, 'b', (7)), 'c'), ('dd', (20971526, 'ee', (4294967295)), 'ff');
SELECT * FROM file(currentDatabase() || '_05843_3.capnp', CapnProto, 's1 String, g Tuple(x UInt64, s String, n Tuple(y UInt32)), s2 String') ORDER BY s1;

SELECT 'named unions';
SET format_schema_message_name = 'NamedUnion';
INSERT INTO FUNCTION file(currentDatabase() || '_05843_4.capnp', CapnProto, 'n Nullable(UInt64), m UInt64, t Nullable(String)') VALUES (6, 1, 'a'), (NULL, 2, NULL), (20971526, 3, NULL), (NULL, 4, 'b');
SELECT * FROM file(currentDatabase() || '_05843_4.capnp', CapnProto, 'n Nullable(UInt64), m UInt64, t Nullable(String)') ORDER BY m;

SELECT 'groups in a nested struct and in a list';
SET format_schema_message_name = 'Nested';
INSERT INTO FUNCTION file(currentDatabase() || '_05843_5.capnp', CapnProto, 'blob String, inner Tuple(a UInt64, g Tuple(x UInt64)), elems Array(Tuple(g Tuple(x UInt64, s String)))') VALUES ('0123WXYZ4567abcd89--efgh', (1, (7)), [((6, 'a')), ((20971526, 'b'))]), (unhex('0600400100000000'), (2, (20971526)), []);
SELECT hex(blob), inner, elems FROM file(currentDatabase() || '_05843_5.capnp', CapnProto, 'blob String, inner Tuple(a UInt64, g Tuple(x UInt64)), elems Array(Tuple(g Tuple(x UInt64, s String)))') ORDER BY inner.a;

SELECT 'schema inference';
SET format_schema_message_name = 'TwoGroups';
DESC format(CapnProto, '');
