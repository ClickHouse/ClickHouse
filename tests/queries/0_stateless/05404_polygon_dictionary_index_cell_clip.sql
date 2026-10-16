-- POLYGON_INDEX_CELL clips polygons to its leaf cells; the clip must not lose parts of a polygon (issue #58639).

DROP DICTIONARY IF EXISTS dict_cell_clip;
DROP TABLE IF EXISTS polygons_cell_clip;

CREATE TABLE polygons_cell_clip (geometry Array(Array(Array(Tuple(Float64, Float64)))), id String) ENGINE = Memory;
INSERT INTO polygons_cell_clip VALUES
    ([[[(42.117824513, 18.750250825), (42.117473649, 18.750430642), (42.117189197, 18.749715446), (42.117550875, 18.749663098), (42.117824513, 18.750250825)]]], 'tiny'),
    ([[[(38.5382, 31.8467), (38.545, 31.8467), (38.545, 31.8523), (38.5382, 31.8523), (38.5382, 31.8467)]]], 'a'),
    ([[[(49.9624, 26.3588), (49.9883, 26.3588), (49.9883, 26.3747), (49.9624, 26.3747), (49.9624, 26.3588)]]], 'b'),
    ([[[(42.8429, 16.3953), (42.8522, 16.3953), (42.8522, 16.4032), (42.8429, 16.4032), (42.8429, 16.3953)]]], 'c'),
    ([[[(42.1957, 18.6386), (42.2284, 18.6386), (42.2284, 18.6713), (42.1957, 18.6713), (42.1957, 18.6386)]]], 'd');

CREATE DICTIONARY dict_cell_clip (geometry Array(Array(Array(Tuple(Float64, Float64)))), id String)
PRIMARY KEY geometry SOURCE(CLICKHOUSE(TABLE 'polygons_cell_clip' DB currentDatabase())) LIFETIME(0) LAYOUT(POLYGON_INDEX_CELL());

-- `tiny` straddles a cell boundary. Count grid points at least 2e-5 away from its edges that the dictionary does not find.
WITH [(42.117824513, 18.750250825), (42.117473649, 18.750430642), (42.117189197, 18.749715446), (42.117550875, 18.749663098)] AS tiny, 2e-5 AS d
SELECT countIf(deep) > 3000, countIf(deep AND dictGet('dict_cell_clip', 'id', p) != 'tiny')
FROM
(
    SELECT (42.1171 + (number % 100) * 0.0000075, 18.7496 + intDiv(number, 100) * 0.0000085) AS p,
        pointInPolygon((p.1 - d, p.2), tiny) AND pointInPolygon((p.1 + d, p.2), tiny)
            AND pointInPolygon((p.1, p.2 - d), tiny) AND pointInPolygon((p.1, p.2 + d), tiny) AS deep
    FROM numbers(10000)
);

DROP DICTIONARY dict_cell_clip;
TRUNCATE TABLE polygons_cell_clip;

-- Coordinates beyond sqrt(FLT_MAX): the clip must not come back empty.
INSERT INTO polygons_cell_clip VALUES ([[[(-1e20, 0), (0.5, 0), (1, 0.5), (0.5, 1), (-1e20, 1)]]], 'huge');
CREATE DICTIONARY dict_cell_clip (geometry Array(Array(Array(Tuple(Float64, Float64)))), id String)
PRIMARY KEY geometry SOURCE(CLICKHOUSE(TABLE 'polygons_cell_clip' DB currentDatabase())) LIFETIME(0) LAYOUT(POLYGON_INDEX_CELL());
SELECT p, dictHas('dict_cell_clip', p) FROM (SELECT arrayJoin([(-1e19, 0.5), (-1., 0.5), (0.55, 0.1), (0.9, 0.1), (5., 0.5)]) AS p);

DROP DICTIONARY dict_cell_clip;
DROP TABLE polygons_cell_clip;
