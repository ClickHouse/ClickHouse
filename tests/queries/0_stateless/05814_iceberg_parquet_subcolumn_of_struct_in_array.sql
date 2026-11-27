-- Tags: no-fasttest, no-replicated-database
-- Tag no-fasttest: needs Parquet and IcebergLocal
-- Tag no-replicated-database: the replicated DDL worker evaluates the `IcebergLocal` arguments without the session, so it does not see the `TEMPORARY TABLE iceberg_path`

-- A subcolumn of a struct inside an Iceberg list (`l.x`, `s.l.y`) read from Parquet data files.

SET allow_insert_into_iceberg = 1, use_iceberg_metadata_files_cache = 0, iceberg_delete_data_on_drop = 1, async_insert = 0;

CREATE TEMPORARY TABLE iceberg_path AS
WITH if(changed, trimBoth(value), 'user_files/') AS user_files_path
SELECT concat(
    if(startsWith(user_files_path, '/'), '', (SELECT path FROM system.disks WHERE name = 'default')),
    user_files_path, '/', currentDatabase(), '/05814_iceberg_parquet_subcolumn_of_struct_in_array/') AS path
FROM system.server_settings WHERE name = 'user_files_path';

CREATE TABLE t
(
    id Int64,
    l Array(Tuple(x Nullable(Int64), y Nullable(String))),
    s Tuple(l Array(Tuple(x Nullable(Int64), y Nullable(String))), z Nullable(Int64)),
    ll Array(Array(Tuple(x Int64, y String))),
    d Array(Tuple(t Tuple(p Nullable(Int64), q Nullable(String)), w Nullable(Int64)))
)
ENGINE = IcebergLocal((SELECT path FROM iceberg_path), 'Parquet');

INSERT INTO t SELECT number, [(number, 'a'), (number + 1, NULL)], ([(number, 'b')], number), [[(number, 'c')], [(number + 10, 'd')]], [((number, 'e'), number * 10)]
FROM numbers(3);

SELECT id, l.x FROM t ORDER BY id;
SELECT s.l.y FROM t WHERE id = 1;
SELECT id, ll.x, d.t.p, d.t, d.w FROM t ORDER BY id;
SELECT id, l.x, l.y, l FROM t ORDER BY id;
SELECT id FROM t WHERE has(l.x, 2) ORDER BY id;
SELECT id, l FROM t WHERE has(l.x, 2) ORDER BY id;
SELECT id, l.x, s.l.y FROM t PREWHERE id = 1;

DROP TABLE t SYNC;
DROP TABLE iceberg_path;
