-- Tags: no-fasttest
-- no-fasttest: ORC is not available in fasttest builds
-- An ORC list or map whose element, key or value struct has none of the fields of the requested Tuple keeps its
-- elements, and each of them is filled with default values.

-- { echo }

SET engine_file_truncate_on_insert = 1;
SET allow_experimental_nullable_tuple_type = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_05617.orc', 'ORC') SELECT
    number AS id,
    if(number = 0, [(1, 'a'), (2, 'b')], [])::Array(Tuple(a Int32, b String)) AS l,
    if(number = 0, map('k1', (1, 'a'), 'k2', (2, 'b')), map())::Map(String, Tuple(a Int32, b String)) AS m,
    map(CAST(tuple(toInt32(number)), 'Tuple(a Int32)'), 'v') AS mk,
    CAST((toInt32(number), [(toInt32(number), 'x')]), 'Tuple(a Int32, l Array(Tuple(x Int32, y String)))') AS t,
    [[(toInt32(number), 'x'), (1, 'y')]]::Array(Array(Tuple(a Int32, b String))) AS ll,
    map('k', [(toInt32(number), 'x')])::Map(String, Array(Tuple(a Int32, b String))) AS ml,
    if(number = 0, [(1, 'a'), NULL, (2, 'b')], [])::Array(Nullable(Tuple(a Int32, b String))) AS ln,
    [(toInt32(number), 'c')]::Array(Tuple(A Int32, b String)) AS lc,
    CAST((toInt32(number), map('k', (1, 'v'))), 'Tuple(a Int32, m Map(String, Tuple(a Int32, b String)))') AS tm,
    [(1, tuple(2)), (3, tuple(4))]::Array(Tuple(x Int32, z Tuple(w Int32))) AS n,
    [(1, [(1, 'a'), (2, 'b')])]::Array(Tuple(y Int32, x Array(Tuple(a Int32, b String)))) AS lx,
    if(number = 0, [tuple(tuple(1)), tuple(NULL)], [tuple(NULL)])::Array(Tuple(b Nullable(Tuple(a Int32)))) AS nb,
    map('k', (if(number = 0, tuple(1), NULL), if(number = 0, NULL, tuple('x'))))::Map(String, Tuple(b Nullable(Tuple(a Int32)), c Nullable(Tuple(x String)))) AS mb,
    CAST((toInt32(number), [tuple(if(number = 0, tuple(1), NULL))]), 'Tuple(a Int32, l Array(Tuple(b Nullable(Tuple(a Int32)))))') AS tb,
    if(number = 0, tuple(1, 'a'), NULL)::Nullable(Tuple(a Int32, b String)) AS ts,
    [CAST(tuple(toDateTime64('3000-01-01 00:00:00', 0, 'UTC')), 'Tuple(a DateTime64(0, \'UTC\'))')] AS ld,
    CAST([tuple(11, tuple(5))], 'Array(Tuple(w Int32, x Tuple(v Int32)))') AS an
FROM numbers(2);

-- A list element, a map value and a map key, also nested in a struct, a list or a map, and with NULL elements.
SELECT id, l FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, l Array(Tuple(q Nullable(Int32)))') ORDER BY id;
SELECT id, m FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, m Map(String, Tuple(q Nullable(String)))') ORDER BY id;
SELECT id, mk FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, mk Map(Tuple(z Nullable(Int32)), String)') ORDER BY id;
SELECT id, t FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, t Tuple(a Int32, l Array(Tuple(q Nullable(Int32))))') ORDER BY id;
SELECT id, ll FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, ll Array(Array(Tuple(q Nullable(Int32))))') ORDER BY id;
SELECT id, ml FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, ml Map(String, Array(Tuple(q Nullable(Int32))))') ORDER BY id;
SELECT id, ln FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, ln Array(Nullable(Tuple(q Nullable(Int32))))') ORDER BY id;
SELECT id, l FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, l Array(Tuple(q Nullable(Int32)))') ORDER BY id SETTINGS input_format_orc_row_batch_size = 1;

-- The same through a subcolumn of a struct and a subcolumn of a Nested column.
SELECT id, `t.a`, `t.l` FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, `t.a` Int32, `t.l` Array(Tuple(q Nullable(Int32)))') ORDER BY id;
SELECT id, `tm.m` FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, `tm.m` Map(String, Tuple(q Nullable(String)))') ORDER BY id;
SELECT id, `n.z` FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, `n.z` Array(Tuple(q Nullable(Int32)))') ORDER BY id;
SELECT id, `n.x`, `n.z` FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, `n.x` Array(Int32), `n.z` Array(Tuple(q Nullable(Int32)))') ORDER BY id;

-- A struct that contains such a list is read with its fields.
SELECT id, lx FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, lx Array(Tuple(x Array(Tuple(q Nullable(Int32)))))') ORDER BY id;

-- A struct read whole by another column keeps its data. optimize_functions_to_subcolumns = 0 keeps `t.l` itself in the read.
SELECT id, t, length(`t.l`) FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, t String, `t.l` Array(Tuple(q String, r String))') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;

-- A Tuple naming some of the fields, and case-insensitive matching.
SELECT id, l FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, l Array(Tuple(a Int32, q Nullable(Int32)))') ORDER BY id;
SELECT id, m FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, m Map(String, Tuple(a Int32, q Nullable(String)))') ORDER BY id;
SELECT id, lc FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, lc Array(Tuple(a Int32))') ORDER BY id SETTINGS input_format_orc_case_insensitive_column_matching = 1;

-- A non-nullable Date32 field gets the same default with and without a field that is in the file, an Enum gets its first value.
SELECT id, l, m FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, l Array(Tuple(q Date32, e Enum8(\'x\' = 1, \'y\' = 2))), m Map(String, Tuple(q Date32))') ORDER BY id;
SELECT id, l, m FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, l Array(Tuple(a Int32, q Date32)), m Map(String, Tuple(a Int32, q Date32))') ORDER BY id;

-- A requested struct field of such a struct keeps its presence, the same as when one of its fields is requested.
SELECT id, nb FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, nb Array(Tuple(b Nullable(Tuple(q Int32))))') ORDER BY id;
SELECT id, nb FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, nb Array(Tuple(b Nullable(Tuple(a Nullable(Int32), q Int32))))') ORDER BY id;
SELECT id, mb FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, mb Map(String, Tuple(c Nullable(Tuple(q Int32)), b Nullable(Tuple(r String))))') ORDER BY id;
SELECT id, mb FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, mb Map(String, Tuple(c Nullable(Tuple(x Nullable(String), q Int32)), b Nullable(Tuple(a Nullable(Int32), r String))))') ORDER BY id;

-- A NULL struct requested as non-Nullable is rejected or read as default, the same as when one of its fields is requested.
SELECT id, nb FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, nb Array(Tuple(b Tuple(q Int32)))') ORDER BY id SETTINGS input_format_null_as_default = 1;
SELECT id, nb FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, nb Array(Tuple(b Tuple(a Nullable(Int32), q Int32)))') ORDER BY id SETTINGS input_format_null_as_default = 1;
SELECT id, nb FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, nb Array(Tuple(b Tuple(q Int32)))') ORDER BY id SETTINGS input_format_null_as_default = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT id, nb FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, nb Array(Tuple(b Tuple(a Nullable(Int32), q Int32)))') ORDER BY id SETTINGS input_format_null_as_default = 0; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }

-- Two columns reading the same struct with different types get the defaults of their own type.
SELECT id, t, `t.l` FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, t Tuple(l Array(Tuple(q Nullable(Int32)))), `t.l` Array(Tuple(r Nullable(String), s Nullable(Int32)))') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, t, `t.l` FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, t Tuple(l Array(Tuple(e Enum8(\'x\' = 1)))), `t.l` Array(Tuple(r Nullable(String)))') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, `t.l`, t FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, `t.l` Array(Tuple(r Nullable(String), s Nullable(Int32))), t Tuple(l Array(Tuple(q Nullable(Int32))))') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;

-- Of two columns reading the same struct, one that requests a nested struct of it keeps that struct's presence.
SELECT id, tb, `tb.l` FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, tb Tuple(l Array(Tuple(q Nullable(Int32)))), `tb.l` Array(Tuple(b Nullable(Tuple(q Int32))))') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, `tb.l`, tb FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, `tb.l` Array(Tuple(b Nullable(Tuple(q Int32)))), tb Tuple(l Array(Tuple(q Nullable(Int32))))') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;
SELECT id, tb, `tb.a` FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, tb Tuple(l Array(Tuple(q Nullable(Int32)))), `tb.a` Int32') ORDER BY id SETTINGS optimize_functions_to_subcolumns = 0;

-- A top-level struct keeps its presence too, also when missing columns are not allowed.
SELECT id, ts FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, ts Nullable(Tuple(q Int32))') ORDER BY id;
SELECT id, ts FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, ts Nullable(Tuple(q Int32))') ORDER BY id SETTINGS input_format_orc_allow_missing_columns = 0;

-- A field read only for the size of its struct is not converted.
SELECT id, ld FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, ld Array(Tuple(q Int32))') ORDER BY id;

-- An unnamed Tuple is matched by position and must have as many elements as the struct.
SELECT id, ts FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, ts Nullable(Tuple(Int32, String, Int32))') ORDER BY id; -- { serverError TYPE_MISMATCH }
SELECT id, l FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, l Array(Tuple(Int32, String, Int32))') ORDER BY id; -- { serverError TYPE_MISMATCH }

-- The same for a Nested column that is not flattened.
SELECT id, l FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, l Nested(q Int32)') ORDER BY id SETTINGS flatten_nested = 0;
SELECT id, ln FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, ln Nested(q Nullable(Int32))') ORDER BY id SETTINGS flatten_nested = 0;
SELECT id, ln FROM file(currentDatabase() || '_05617.orc', 'ORC', 'id UInt64, ln Nested(a Nullable(Int32), q Nullable(Int32))') ORDER BY id SETTINGS flatten_nested = 0;
