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
SELECT readWKTLineString('LINESTRING M (1 2,3 4)'); -- { serverError CANNOT_PARSE_TEXT }
SELECT readWKTMultiPoint('MULTIPOINT M ((1 2))'); -- { serverError CANNOT_PARSE_TEXT }
