-- Tags: no-fasttest
-- Subcolumns of a `Map` read from Parquet, without reading the whole column in the same query.

SET engine_file_truncate_on_insert = 1;

INSERT INTO FUNCTION file(currentDatabase() || '_map_subcolumns.parquet')
SELECT number AS id,
    map('k' || toString(number), number, 'j', 10 + number) AS m,
    map('x', (number, toString(number)))::Map(String, Tuple(a UInt64, b String)) AS mt,
    (map('y', number), number)::Tuple(x Map(String, UInt64), z UInt64) AS t,
    [map('p', number), map('q', 2, 'r', 3)]::Array(Map(String, UInt64)) AS am
FROM numbers(3);

SELECT id, m.keys, m.values FROM file(currentDatabase() || '_map_subcolumns.parquet') ORDER BY id;
SELECT id, m.key, m.value FROM file(currentDatabase() || '_map_subcolumns.parquet', Parquet, 'id UInt64, m Array(Tuple(key String, value UInt64))') ORDER BY id;
SELECT id, mt.values.a, t.x.keys, am.keys FROM file(currentDatabase() || '_map_subcolumns.parquet') ORDER BY id;
SELECT id FROM file(currentDatabase() || '_map_subcolumns.parquet') WHERE has(m.keys, 'k1');
