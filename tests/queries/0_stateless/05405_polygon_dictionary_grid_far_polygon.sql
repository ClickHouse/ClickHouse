-- A polygon far away from the others must not hide the polygons near the opposite side of the dictionary's bounding box.

DROP DICTIONARY IF EXISTS dict_grid_each;
DROP DICTIONARY IF EXISTS dict_grid_cell;
DROP DICTIONARY IF EXISTS dict_grid_each_leaf;
DROP DICTIONARY IF EXISTS dict_grid_cell_leaf;
DROP TABLE IF EXISTS polygons_grid;

CREATE TABLE polygons_grid (key Array(Array(Array(Tuple(Float64, Float64)))), name String) ENGINE = Memory;

CREATE DICTIONARY dict_grid_each (key Array(Array(Array(Tuple(Float64, Float64)))), name String)
PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'polygons_grid' DB currentDatabase())) LIFETIME(0) LAYOUT(POLYGON_INDEX_EACH());
CREATE DICTIONARY dict_grid_cell (key Array(Array(Array(Tuple(Float64, Float64)))), name String)
PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'polygons_grid' DB currentDatabase())) LIFETIME(0) LAYOUT(POLYGON_INDEX_CELL());

INSERT INTO polygons_grid VALUES
    ([[[(-1e9, -1e9), (0, -1e9), (0, 0), (-1e9, 0)]]], 'far'),
    ([[[(1, 1), (2, 1), (2, 2), (1, 2)]]], 's1'),
    ([[[(3, 3), (4, 3), (4, 4), (3, 4)]]], 's2'),
    ([[[(-260000000, 0.5), (-250000016, 0.5), (-250000016, 3.5), (-260000000, 3.5)]]], 'q');
SYSTEM RELOAD DICTIONARY dict_grid_each;
SYSTEM RELOAD DICTIONARY dict_grid_cell;

-- 81 x 81 points over [-1, 4] x [-1, 4], none on a polygon edge: how many each layout answers wrong.
SELECT countIf(expected IN ('s1', 's2')), countIf(expected = 'far'),
    countIf(dictGet('dict_grid_each', 'name', p) != expected), countIf(dictGet('dict_grid_cell', 'name', p) != expected)
FROM
(
    SELECT (-0.96875 + intDiv(number, 81) * 0.0625, -0.96875 + (number % 81) * 0.0625) AS p,
        multiIf(p.1 BETWEEN 1 AND 2 AND p.2 BETWEEN 1 AND 2, 's1', p.1 BETWEEN 3 AND 4 AND p.2 BETWEEN 3 AND 4, 's2',
            p.1 <= 0 AND p.2 <= 0, 'far', '') AS expected
    FROM numbers(81 * 81)
);

-- Inside `q`, 32 units left of -2.5e8, where the point's position in the bounding box rounds to a boundary between root cells.
SELECT dictGet('dict_grid_each', 'name', (-250000032., 1.5)), dictGet('dict_grid_cell', 'name', (-250000032., 1.5));

TRUNCATE TABLE polygons_grid;
INSERT INTO polygons_grid VALUES
    ([[[(-3e7, -3e7), (0, -3e7), (0, 0), (-3e7, 0)]]], 'far'),
    ([[[(1, 1), (2, 1), (2, 2), (1, 2)]]], 's1'),
    ([[[(3, 3), (4, 3), (4, 4), (3, 4)]]], 's2');
SYSTEM RELOAD DICTIONARY dict_grid_each;
SYSTEM RELOAD DICTIONARY dict_grid_cell;

SELECT countIf(expected IN ('s1', 's2')), countIf(expected = 'far'),
    countIf(dictGet('dict_grid_each', 'name', p) != expected), countIf(dictGet('dict_grid_cell', 'name', p) != expected)
FROM
(
    SELECT (-0.96875 + intDiv(number, 81) * 0.0625, -0.96875 + (number % 81) * 0.0625) AS p,
        multiIf(p.1 BETWEEN 1 AND 2 AND p.2 BETWEEN 1 AND 2, 's1', p.1 BETWEEN 3 AND 4 AND p.2 BETWEEN 3 AND 4, 's2',
            p.1 <= 0 AND p.2 <= 0, 'far', '') AS expected
    FROM numbers(81 * 81)
);

-- A side of the bounding box overflows Float32.
TRUNCATE TABLE polygons_grid;
INSERT INTO polygons_grid VALUES
    ([[[(-3e38, -3e38), (-1e38, -3e38), (-1e38, -1e38), (-3e38, -1e38)]]], 'a'),
    ([[[(1e38, 1e38), (3e38, 1e38), (3e38, 3e38), (1e38, 3e38)]]], 'b');
SYSTEM RELOAD DICTIONARY dict_grid_each; -- { serverError BAD_ARGUMENTS }
SYSTEM RELOAD DICTIONARY dict_grid_cell; -- { serverError BAD_ARGUMENTS }

-- A grid that is a single leaf does not route points, so it still loads.
CREATE DICTIONARY dict_grid_each_leaf (key Array(Array(Array(Tuple(Float64, Float64)))), name String)
PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'polygons_grid' DB currentDatabase())) LIFETIME(0) LAYOUT(POLYGON_INDEX_EACH(MAX_DEPTH 0));
CREATE DICTIONARY dict_grid_cell_leaf (key Array(Array(Array(Tuple(Float64, Float64)))), name String)
PRIMARY KEY key SOURCE(CLICKHOUSE(TABLE 'polygons_grid' DB currentDatabase())) LIFETIME(0) LAYOUT(POLYGON_INDEX_CELL(MAX_DEPTH 0));
SYSTEM RELOAD DICTIONARY dict_grid_each_leaf;
SYSTEM RELOAD DICTIONARY dict_grid_cell_leaf;
SELECT dictGet('dict_grid_each_leaf', 'name', (2e38, 2e38)), dictGet('dict_grid_each_leaf', 'name', (-2e38, -2e38)),
    dictHas('dict_grid_each_leaf', (0., 0.)), dictGet('dict_grid_cell_leaf', 'name', (2e38, 2e38)),
    dictGet('dict_grid_cell_leaf', 'name', (-2e38, -2e38)), dictHas('dict_grid_cell_leaf', (0., 0.));
DROP DICTIONARY dict_grid_each_leaf;
DROP DICTIONARY dict_grid_cell_leaf;

-- Sides that fit in Float32 still load.
TRUNCATE TABLE polygons_grid;
INSERT INTO polygons_grid VALUES
    ([[[(-1.5e38, -1.5e38), (-0.5e38, -1.5e38), (-0.5e38, -0.5e38), (-1.5e38, -0.5e38)]]], 'a'),
    ([[[(0.5e38, 0.5e38), (1.5e38, 0.5e38), (1.5e38, 1.5e38), (0.5e38, 1.5e38)]]], 'b');
SYSTEM RELOAD DICTIONARY dict_grid_each;
SYSTEM RELOAD DICTIONARY dict_grid_cell;
SELECT dictGet('dict_grid_each', 'name', (1.4e38, 1.4e38)), dictGet('dict_grid_each', 'name', (-1.4e38, -1.4e38)),
    dictHas('dict_grid_each', (0., 0.)), dictGet('dict_grid_cell', 'name', (1.4e38, 1.4e38)),
    dictGet('dict_grid_cell', 'name', (-1.4e38, -1.4e38)), dictHas('dict_grid_cell', (0., 0.));

DROP DICTIONARY dict_grid_each;
DROP DICTIONARY dict_grid_cell;
DROP TABLE polygons_grid;
