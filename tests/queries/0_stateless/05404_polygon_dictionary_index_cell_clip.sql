-- Polygon dictionaries split the plane into cells: a cell must not lose parts of a polygon (issue #58639) and must not be assigned to a polygon that does not cover it (issue #122018).

DROP DICTIONARY IF EXISTS dict_cell_clip;
DROP DICTIONARY IF EXISTS dict_each_clip;
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

-- Two small building outlines (issue #122018).
TRUNCATE TABLE polygons_cell_clip;
INSERT INTO polygons_cell_clip VALUES
    ([[[(-0.1241457, 51.5139486), (-0.124238, 51.5138956), (-0.1241828, 51.5138584), (-0.1241115, 51.5138993), (-0.1239821, 51.5138119), (-0.1239211, 51.5138469), (-0.1239383, 51.5138585), (-0.1239878, 51.5138919), (-0.1240322, 51.5139219), (-0.1240827, 51.513956), (-0.1241057, 51.5139715), (-0.1241271, 51.5139593), (-0.1241457, 51.5139486)]]], '0'),
    ([[[(-0.119172, 51.5130896), (-0.1191978, 51.513098), (-0.1195412, 51.5126671), (-0.1195825, 51.5126148), (-0.1195523, 51.5126049), (-0.1195247, 51.5125957), (-0.1194496, 51.5125697), (-0.119423, 51.5126024), (-0.1191336, 51.5125175), (-0.1191623, 51.512479), (-0.1191067, 51.5124617), (-0.1190079, 51.5124309), (-0.1189624, 51.5124953), (-0.118909, 51.5125629), (-0.1188363, 51.512641), (-0.1187677, 51.5127115), (-0.118569, 51.5128696), (-0.1186599, 51.5129074), (-0.1186865, 51.5129184), (-0.118718, 51.5129314), (-0.1187566, 51.5128923), (-0.1191106, 51.5130048), (-0.1190705, 51.5130559), (-0.1191434, 51.51308), (-0.119172, 51.5130896)]]], '1');
CREATE DICTIONARY dict_cell_clip (geometry Array(Array(Array(Tuple(Float64, Float64)))), id String)
PRIMARY KEY geometry SOURCE(CLICKHOUSE(TABLE 'polygons_cell_clip' DB currentDatabase())) LIFETIME(0) LAYOUT(POLYGON());
CREATE DICTIONARY dict_each_clip (geometry Array(Array(Array(Tuple(Float64, Float64)))), id String)
PRIMARY KEY geometry SOURCE(CLICKHOUSE(TABLE 'polygons_cell_clip' DB currentDatabase())) LIFETIME(0) LAYOUT(POLYGON_INDEX_EACH());
SELECT dictGet('dict_cell_clip', 'id', (-0.11900975455216113, 51.51286227598678)), dictGet('dict_each_clip', 'id', (-0.11900975455216113, 51.51286227598678));
-- Points at least 2e-5 inside a building or outside both.
WITH [(-0.1241457, 51.5139486), (-0.124238, 51.5138956), (-0.1241828, 51.5138584), (-0.1241115, 51.5138993), (-0.1239821, 51.5138119), (-0.1239211, 51.5138469), (-0.1239383, 51.5138585), (-0.1239878, 51.5138919), (-0.1240322, 51.5139219), (-0.1240827, 51.513956), (-0.1241057, 51.5139715), (-0.1241271, 51.5139593)] AS b0,
    [(-0.119172, 51.5130896), (-0.1191978, 51.513098), (-0.1195412, 51.5126671), (-0.1195825, 51.5126148), (-0.1195523, 51.5126049), (-0.1195247, 51.5125957), (-0.1194496, 51.5125697), (-0.119423, 51.5126024), (-0.1191336, 51.5125175), (-0.1191623, 51.512479), (-0.1191067, 51.5124617), (-0.1190079, 51.5124309), (-0.1189624, 51.5124953), (-0.118909, 51.5125629), (-0.1188363, 51.512641), (-0.1187677, 51.5127115), (-0.118569, 51.5128696), (-0.1186599, 51.5129074), (-0.1186865, 51.5129184), (-0.118718, 51.5129314), (-0.1187566, 51.5128923), (-0.1191106, 51.5130048), (-0.1190705, 51.5130559), (-0.1191434, 51.51308)] AS b1,
    2e-5 AS d
SELECT countIf(e = '0') > 20 AND countIf(e = '1') > 1000 AND countIf(e = '') > 30000,
    countIf(e != '?' AND dictGet('dict_cell_clip', 'id', p) != e),
    countIf(e != '?' AND dictGet('dict_each_clip', 'id', p) != e)
FROM
(
    SELECT p,
        pointInPolygon((p.1 - d, p.2), b0) + pointInPolygon((p.1 + d, p.2), b0) + pointInPolygon((p.1, p.2 - d), b0) + pointInPolygon((p.1, p.2 + d), b0) + pointInPolygon(p, b0) AS k0,
        pointInPolygon((p.1 - d, p.2), b1) + pointInPolygon((p.1 + d, p.2), b1) + pointInPolygon((p.1, p.2 - d), b1) + pointInPolygon((p.1, p.2 + d), b1) + pointInPolygon(p, b1) AS k1,
        multiIf(k1 = 5, '1', k0 = 5, '0', k0 + k1 = 0, '', '?') AS e
    FROM (SELECT (-0.12425 + (number % 200) * 0.0000285, 51.51242 + intDiv(number, 200) * 0.0000078) AS p FROM numbers(40000))
);
DROP DICTIONARY dict_cell_clip;
DROP DICTIONARY dict_each_clip;

DROP TABLE polygons_cell_clip;
