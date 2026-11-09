-- Tags: no-fasttest
-- no-fasttest: ORC is not available in fasttest builds
-- An ORC list or map whose element, key or value struct has none of the fields of the requested Tuple keeps its
-- elements, and each of them is the default value of the Tuple. The expected values are the ones the Arrow reader returns.

-- { echo }

SET engine_file_truncate_on_insert = 1;

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
    [(1, [(1, 'a'), (2, 'b')])]::Array(Tuple(y Int32, x Array(Tuple(a Int32, b String)))) AS lx
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
