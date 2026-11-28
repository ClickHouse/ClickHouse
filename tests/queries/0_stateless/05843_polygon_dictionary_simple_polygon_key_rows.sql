-- A simple-polygon key (one ring per row) must give one polygon per row. Every row after the first used to become a hole
-- of the first polygon, so points inside it were missed or attributed to the first row, and a third row indexed past
-- the end of an empty offsets array. The first two rows are the small triangles from the issue.

CREATE TABLE polygons (key Array(Tuple(Float64, Float64)), name String) ENGINE = Memory;
INSERT INTO polygons VALUES
    ([(1000, 1000), (1000.01, 1000), (1000, 1000.01)], 'small_1'),
    ([(1000.01, 1000.01), (1000.02, 1000.01), (1000.02, 1000.02)], 'small_2'),
    ([(0, 0), (10, 0), (10, 10), (0, 10)], 'square');

CREATE TABLE polygons_arrays (key Array(Array(Float64)), name String) ENGINE = Memory;
INSERT INTO polygons_arrays SELECT arrayMap(p -> [p.1, p.2], key), name FROM polygons;

CREATE TABLE points (x Float64, y Float64) ENGINE = Memory;
INSERT INTO points VALUES (1000.002, 1000.002), (1000.018, 1000.012), (5, 5), (1000.018, 1000.002), (20, 20);

CREATE DICTIONARY dict_polygon (key Array(Tuple(Float64, Float64)), name String) PRIMARY KEY key
    SOURCE(CLICKHOUSE(TABLE 'polygons')) LIFETIME(0) LAYOUT(POLYGON());
CREATE DICTIONARY dict_simple (key Array(Tuple(Float64, Float64)), name String) PRIMARY KEY key
    SOURCE(CLICKHOUSE(TABLE 'polygons')) LIFETIME(0) LAYOUT(POLYGON_SIMPLE());
CREATE DICTIONARY dict_index_each (key Array(Tuple(Float64, Float64)), name String) PRIMARY KEY key
    SOURCE(CLICKHOUSE(TABLE 'polygons')) LIFETIME(0) LAYOUT(POLYGON_INDEX_EACH());
CREATE DICTIONARY dict_index_cell (key Array(Tuple(Float64, Float64)), name String) PRIMARY KEY key
    SOURCE(CLICKHOUSE(TABLE 'polygons')) LIFETIME(0) LAYOUT(POLYGON_INDEX_CELL());
CREATE DICTIONARY dict_arrays (key Array(Array(Float64)), name String) PRIMARY KEY key
    SOURCE(CLICKHOUSE(TABLE 'polygons_arrays')) LIFETIME(0) LAYOUT(POLYGON());

SELECT
    x, y,
    dictGetOrDefault('dict_polygon', 'name', (x, y), '-'),
    dictGetOrDefault('dict_simple', 'name', (x, y), '-'),
    dictGetOrDefault('dict_index_each', 'name', (x, y), '-'),
    dictGetOrDefault('dict_index_cell', 'name', (x, y), '-'),
    dictGetOrDefault('dict_arrays', 'name', (x, y), '-')
FROM points
ORDER BY x, y;
