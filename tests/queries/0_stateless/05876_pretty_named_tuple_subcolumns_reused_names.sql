SET output_format_pretty_color = 0;
SET output_format_pretty_named_tuples_as_subcolumns = 1;

SELECT 'A Tuple element reuses the name of a column of a different type';
SELECT 1 AS id, ('u1', 'alice')::Tuple(id String, name String) AS user FORMAT Pretty;
SELECT 1 AS id, ('u1', 'alice')::Tuple(id String, name String) AS user FORMAT PrettyCompact;
SELECT 1 AS id, ('u1', 'alice')::Tuple(id String, name String) AS user FORMAT PrettySpace;

SELECT 'A nested Tuple element reuses the name of an outer element';
SELECT (1, ('x', 2))::Tuple(id UInt8, parent Tuple(id String, n UInt8)) AS t FORMAT PrettyCompact;

SELECT 'Two Tuples have elements with the same name and different types';
SELECT tuple(1)::Tuple(a UInt8) AS t1, tuple('x')::Tuple(a String) AS t2 FORMAT PrettyCompact;

SELECT 'Nullable and LowCardinality differ from the plain type';
SELECT toNullable(1) AS id, (1, 'x')::Tuple(id UInt8, s String) AS t FORMAT PrettyCompact;
SELECT toLowCardinality('a') AS s, ('b', 1)::Tuple(s String, n UInt8) AS t FORMAT PrettyCompact;
