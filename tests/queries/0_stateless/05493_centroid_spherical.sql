-- Tags: no-fasttest
-- Tag no-fasttest: needs s2

SELECT 'centroidSpherical Point';
SELECT centroidSpherical((2., 3.));

SELECT 'centroidSpherical Ring';
SELECT round(centroidSpherical([(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]).1, 6), round(centroidSpherical([(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]).2, 6);

SELECT 'centroidSpherical Polygon';
SELECT round(centroidSpherical([[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]]).1, 6), round(centroidSpherical([[(4.346693, 50.858306), (4.367945, 50.852455), (4.366227, 50.840809), (4.344961, 50.833264), (4.338074, 50.848677), (4.346693, 50.858306)]]).2, 6);

SELECT 'centroidSpherical LineString';
SELECT round(centroidSpherical(CAST([(0., 0.), (10., 0.)], 'LineString')).1, 6), round(centroidSpherical(CAST([(0., 0.), (10., 0.)], 'LineString')).2, 6);

SELECT 'centroidSpherical MultiLineString';
SELECT round(centroidSpherical(CAST([[(0., 0.), (5., 0.)], [(5., 0.), (10., 0.)]], 'MultiLineString')).1, 6), round(centroidSpherical(CAST([[(0., 0.), (5., 0.)], [(5., 0.), (10., 0.)]], 'MultiLineString')).2, 6);

SELECT 'centroidSpherical MultiPolygon';
SELECT round(centroidSpherical([[[(0., 0.), (0., 2.), (2., 2.), (2., 0.), (0., 0.)]], [[(4., 4.), (4., 6.), (6., 6.), (6., 4.), (4., 4.)]]]).1, 6), round(centroidSpherical([[[(0., 0.), (0., 2.), (2., 2.), (2., 0.), (0., 0.)]], [[(4., 4.), (4., 6.), (6., 6.), (6., 4.), (4., 4.)]]]).2, 6);

SELECT 'centroidSpherical date line crossing LineString';
SELECT round(centroidSpherical(CAST([(179., 0.), (-179., 0.)], 'LineString')).1, 6), round(centroidSpherical(CAST([(179., 0.), (-179., 0.)], 'LineString')).2, 6);

SELECT 'centroidSpherical near North Pole';
-- The longitude of a point at the pole is arbitrary.
SELECT round(centroidSpherical([(0., 89.), (120., 89.), (240., 89.), (0., 89.)]).2, 6);

SELECT 'centroidSpherical Polygon with hole';
SELECT round(centroidSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(3., 3.), (3., 7.), (7., 7.), (7., 3.), (3., 3.)]]).1, 6), round(centroidSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(3., 3.), (3., 7.), (7., 7.), (7., 3.), (3., 3.)]]).2, 6);

SELECT 'centroidSpherical square';
SELECT round(centroidSpherical([(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]).1, 6), round(centroidSpherical([(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]).2, 6);

SELECT 'centroidSpherical Geometry Point';
SELECT centroidSpherical(readWKT('POINT(2 3)'));
SELECT 'centroidSpherical Geometry Polygon';
SELECT round(centroidSpherical(readWKT('POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))')).1, 6), round(centroidSpherical(readWKT('POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))')).2, 6);

SELECT 'centroidSpherical globe-spanning polygon (triggers adaptive origin)';
SELECT round(centroidSpherical([(0., 0.), (90., 0.), (180., 0.), (-90., 0.), (0., 0.)]).2, 6);

SELECT centroidSpherical(42); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT 'centroidSpherical empty polygon (degenerate)';
SELECT round(centroidSpherical([[(0., 0.), (0., 0.), (0., 0.), (0., 0.)]]).1, 6), round(centroidSpherical([[(0., 0.), (0., 0.), (0., 0.), (0., 0.)]]).2, 6);

SELECT 'centroidSpherical MultiPoint';
SELECT round(centroidSpherical(CAST([(0., 0.), (3., 5.), (6., 0.)], 'MultiPoint')).1, 6), round(centroidSpherical(CAST([(0., 0.), (3., 5.), (6., 0.)], 'MultiPoint')).2, 6);

SELECT 'centroidSpherical MultiPoint along meridian';
SELECT round(centroidSpherical(CAST([(0., 10.), (0., 20.), (0., 30.)], 'MultiPoint')).1, 6), round(centroidSpherical(CAST([(0., 10.), (0., 20.), (0., 30.)], 'MultiPoint')).2, 6);

SELECT 'centroidSpherical empty MultiPoint (degenerate)';
SELECT round(centroidSpherical(CAST([], 'MultiPoint')).1, 6), round(centroidSpherical(CAST([], 'MultiPoint')).2, 6);

SELECT 'centroidSpherical Polygon with off-center hole, hole wound like the outer ring and reversed, and unclosed';
SELECT round(c.1, 6), round(c.2, 6) FROM (SELECT centroidSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(1., 1.), (1., 3.), (3., 3.), (3., 1.), (1., 1.)]]) AS c);
SELECT round(c.1, 6), round(c.2, 6) FROM (SELECT centroidSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.), (1., 1.)]]) AS c);
SELECT round(c.1, 6), round(c.2, 6) FROM (SELECT centroidSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(1., 1.), (3., 1.), (3., 3.), (1., 3.)]]) AS c);

SELECT 'centroidSpherical MultiPolygon with mixed winding';
SELECT round(c.1, 6), round(c.2, 6) FROM (SELECT centroidSpherical([[[(0., 0.), (0., 2.), (2., 2.), (2., 0.), (0., 0.)]], [[(4., 4.), (6., 4.), (6., 6.), (4., 6.), (4., 4.)]]]) AS c);

-- A block with more rows than member geometries: an empty row must not shift or drop the following rows.
SELECT 'centroidSpherical MultiLineString empty row followed by nonempty rows';
SELECT g, round(c.1, 6), round(c.2, 6)
FROM (SELECT g, centroidSpherical(g) AS c FROM (SELECT CAST(arrayJoin([[], [[(10., 10.), (12., 12.)]]]), 'MultiLineString') AS g));

SELECT 'centroidSpherical MultiPolygon empty row followed by nonempty rows';
SELECT g, round(c.1, 6), round(c.2, 6)
FROM (SELECT g, centroidSpherical(g) AS c FROM (SELECT CAST(arrayJoin([[], [[[(0., 0.), (0., 4.), (4., 4.), (4., 0.), (0., 0.)]]]]), 'MultiPolygon') AS g));

-- A ring of zero area has the centroid of its boundary, which does not depend on the starting vertex.
SELECT 'centroidSpherical zero-area rings along the equator and a meridian';
SELECT round(c.1, 6), round(c.2, 6) FROM (SELECT centroidSpherical([(0., 0.), (5., 0.), (10., 0.), (0., 0.)]) AS c);
SELECT round(c.1, 6), round(c.2, 6) FROM (SELECT centroidSpherical([(5., 0.), (10., 0.), (0., 0.), (5., 0.)]) AS c);
SELECT round(c.1, 6), round(c.2, 6) FROM (SELECT centroidSpherical([(0., 0.), (0., 5.), (0., 10.), (0., 0.)]) AS c);

SELECT 'centroidSpherical ring around a pole in the reverse orientation';
SELECT round(centroidSpherical([(0., 89.), (240., 89.), (120., 89.), (0., 89.)]).2, 6);
SELECT round(centroidSpherical([[(0., -80.), (-120., -80.), (-240., -80.), (0., -80.)]]).2, 6);
SELECT round(centroidSpherical([[(0., -80.), (-240., -80.), (-120., -80.), (0., -80.)]]).2, 6);

SELECT 'centroidSpherical antipodal MultiPoint';
SELECT centroidSpherical(CAST([(0., 0.), (180., 0.)], 'MultiPoint'));

SELECT 'centroidSpherical of empty geometries';
SELECT centroidSpherical(CAST([], 'Ring')), centroidSpherical(CAST([], 'Polygon')), centroidSpherical(CAST([], 'LineString')), centroidSpherical(CAST(NULL, 'Geometry'));

SELECT centroidSpherical([(0., 0.), (0., 100.), (10., 0.), (0., 0.)]); -- { serverError BAD_ARGUMENTS }
SELECT centroidSpherical((0., -91.)); -- { serverError BAD_ARGUMENTS }
SELECT centroidSpherical(CAST(CAST([[(0., 0.), (nan, 1.), (1., 1.), (0., 0.)]], 'Polygon'), 'Geometry')); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- Such rings used to fail an assertion in `S2::GetSignedArea`. They have zero area, so the result is the centroid of the boundary.
SELECT 'centroidSpherical rings going back and forth along a great circle';
SELECT round(c.1, 6), round(c.2, 6) FROM (SELECT centroidSpherical([(90., 0.), (45., 0.), (30., 0.), (60., 0.)]) AS c);
SELECT round(c.1, 6), round(c.2, 6) FROM (SELECT centroidSpherical([(0., 45.), (0., -30.), (0., -45.), (0., 0.)]) AS c);

-- The input found by the AST fuzzer: its edges join antipodal points, so their paths are undefined.
SELECT centroidSpherical([(-90., 0.), (90., 0.), (180., 0.), (0., 0.), (0., 0.)]); -- { serverError BAD_ARGUMENTS }
SELECT boundingBoxSpherical([(-90., 0.), (90., 0.), (180., 0.), (0., 0.), (0., 0.)]); -- { serverError BAD_ARGUMENTS }
SELECT centroidSpherical(CAST([(0., -90.), (10., 90.)], 'LineString')); -- { serverError BAD_ARGUMENTS }
SELECT centroidSpherical(CAST([[(0., 0.), (1., 1.)], [(-90., 0.), (450., 0.)]], 'MultiLineString')); -- { serverError BAD_ARGUMENTS }
-- The closing edge of the unclosed hole.
SELECT centroidSpherical([[(0., 0.), (0., 10.), (10., 10.), (10., 0.), (0., 0.)], [(1., 1.), (2., 2.), (-179., -1.)]]); -- { serverError BAD_ARGUMENTS }
SELECT centroidSpherical(CAST(CAST([(0., 0.), (180., 0.)], 'LineString'), 'Geometry')); -- { serverError BAD_ARGUMENTS }

-- Both halves have the same area, so whatever the winding, the interior is the half on the right of the ring, and the
-- centroid is inside the bounding box. The polygon and the multipolygon of the ring are the same.
SELECT 'centroidSpherical and boundingBoxSpherical of rings splitting the sphere into two equal halves';
SELECT
    name,
    reversed,
    round(c.2, 6),
    round(b[1].2, 6),
    round(b[3].2, 6),
    c.2 BETWEEN b[1].2 AND b[3].2 AND positiveModulo(c.1 - b[1].1, 360.) <= b[2].1 - b[1].1,
    centroidSpherical([ring]) = c AND centroidSpherical([[ring]]) = c
        AND boundingBoxSpherical([ring]) = b AND boundingBoxSpherical([[ring]]) = b
FROM
(
    SELECT name, reversed, ring, centroidSpherical(ring) AS c, boundingBoxSpherical(ring) AS b
    FROM
    (
        SELECT name, reversed, if(reversed, arrayReverse(r), r) AS ring
        FROM values('name String, r Array(Tuple(Float64, Float64))',
            ('equator', [(0., 0.), (90., 0.), (180., 0.), (-90., 0.)]),
            ('tilted great circle', [(0., 0.), (90., 45.), (180., 0.), (-90., -45.)]),
            ('centrally symmetric', [(0., 0.), (45., 30.), (90., 45.), (135., 30.), (180., 0.), (-135., -30.), (-90., -45.), (-45., -30.)]))
        ARRAY JOIN [false, true] AS reversed
    )
)
ORDER BY name, reversed;

SELECT 'centroidSpherical Point with a longitude outside [-180, 180]';
SELECT centroidSpherical((200., 10.)), centroidSpherical(CAST(CAST((200., 10.), 'Point'), 'Geometry')), centroidSpherical(CAST([(200., 10.)], 'MultiPoint'));

-- The ring is a hemisphere up to about 5e-14 steradians, more than the rounding error. Repeating a vertex must not make it
-- a hemisphere, whose interior is the half on the right, instead of the smaller region.
SELECT 'centroidSpherical and boundingBoxSpherical nearly a hemisphere, with a repeated vertex';
SELECT
    d,
    reversed,
    round(centroidSpherical(r).2, 3),
    round(centroidSpherical(arrayConcat(arrayWithConstant(100, r[1]), r)).2, 3),
    boundingBoxSpherical(r),
    boundingBoxSpherical(arrayConcat(arrayWithConstant(100, r[1]), r)) = boundingBoxSpherical(r)
FROM
(
    SELECT d, reversed, if(reversed, arrayReverse(ring), ring) AS r
    FROM (SELECT d, [(0., 0.), (120., 0.), (240., d)] AS ring FROM values('d Float64', 1.4e-12, -1.4e-12))
    ARRAY JOIN [false, true] AS reversed
)
ORDER BY d, reversed;
