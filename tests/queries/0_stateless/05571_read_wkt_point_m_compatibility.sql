SELECT readWKTPoint('POINT M (1 2)'), readWKT('POINT M (1 2)');
SELECT readWKTPoint('POINT M (3 4 99)'), readWKT('POINT M (3 4 99)');
SELECT readWKTPoint('  point\tm (5 6 )  '), readWKT('  point\tm (5 6 )  ');
SELECT readWKTPoint('  point\tm (7 8 99 )  '), readWKT('  point\tm (7 8 99 )  ');

SELECT readWKTPoint(wkt), readWKT(wkt)
FROM
(
    SELECT arrayJoin(['POINT (11 22)', 'POINT M (33 44)', 'POINT M (55 66 99)', 'POINT (77 88)']) AS wkt
);

SELECT readWKTPoint('POINT M (1)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKT('POINT M (1)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTPoint('POINT M (1 2 3 4)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKT('POINT M (1 2 3 4)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTPoint('POINT M (1 2 garbage)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKT('POINT M (1 2 garbage)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTPoint('POINT M (1 2'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKT('POINT M (1 2'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTPoint('POINT M (1 2,3 4)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKT('POINT M (1 2,3 4)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTPoint('POINT M EMPTY'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKT('POINT M EMPTY'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTPoint('POINT Z (1 2)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKT('POINT Z (1 2)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTPoint('POINT ZM (1 2)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKT('POINT ZM (1 2)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTLineString('LINESTRING Z (1 2,3 4)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTLineString('LINESTRING ZM (1 2,3 4)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTLineString('LINESTRING M (1 2,3 4 5 6)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTLineString('LINESTRING M (1,3 4)'); -- { serverError CANNOT_PARSE_TEXT }

-- The two-ordinate `M` form was always accepted for every geometry type, and stays accepted along with the three-ordinate form.
SELECT readWKTLineString('LINESTRING M (1 2,3 4)'), readWKT('LINESTRING M (1 2,3 4)');
SELECT readWKTLineString('LINESTRING M (1 2 9,3 4 9)'), readWKT('LINESTRING M (1 2 9,3 4 9)');
SELECT readWKTLineString('linestring m ( 1 2 , 3 4 )'), readWKT('linestring m ( 1 2 , 3 4 )');
SELECT readWKTMultiPoint('MULTIPOINT M ((1 2),(3 4))'), readWKT('MULTIPOINT M ((1 2),(3 4))');
SELECT readWKTMultiPoint('MULTIPOINT M ((1 2 9),(3 4 9))'), readWKT('MULTIPOINT M ((1 2 9),(3 4 9))');
SELECT readWKTRing('POLYGON M ((0 0,10 0,10 10,0 0))');
SELECT readWKTRing('POLYGON M ((0 0 9,10 0 9,10 10 9,0 0 9))');
SELECT readWKTPolygon('POLYGON M ((0 0,10 0,10 10,0 0))'), readWKT('POLYGON M ((0 0,10 0,10 10,0 0))');
SELECT readWKTPolygon('POLYGON M ((0 0 9,10 0 9,10 10 9,0 0 9))'), readWKT('POLYGON M ((0 0 9,10 0 9,10 10 9,0 0 9))');
SELECT readWKTMultiLineString('MULTILINESTRING M ((1 2,3 4),(5 6,7 8))'), readWKT('MULTILINESTRING M ((1 2,3 4),(5 6,7 8))');
SELECT readWKTMultiLineString('MULTILINESTRING M ((1 2 9,3 4 9),(5 6 9,7 8 9))'), readWKT('MULTILINESTRING M ((1 2 9,3 4 9),(5 6 9,7 8 9))');
SELECT readWKTMultiPolygon('MULTIPOLYGON M (((0 0,10 0,10 10,0 0)))'), readWKT('MULTIPOLYGON M (((0 0,10 0,10 10,0 0)))');
SELECT readWKTMultiPolygon('MULTIPOLYGON M (((0 0 9,10 0 9,10 10 9,0 0 9)))'), readWKT('MULTIPOLYGON M (((0 0 9,10 0 9,10 10 9,0 0 9)))');
