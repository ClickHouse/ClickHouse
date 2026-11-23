-- Coordinates are written with the shortest representation that round-trips, not 6 significant digits.

SELECT wkt((139.699325900317, 35.67639759983872)::Point);
SELECT wkt((123456.789012, 1.23456789)::Point);
SELECT wkt(materialize((139.699325900317, 35.67639759983872)::Point));
SELECT wkt((0.1, 1e-7)::Point);
SELECT wkt((1e21, 123456789012.)::Point);
SELECT wkt((1.7976931348623157e308, 5e-324)::Point);
SELECT wkt((-0., 2.5)::Point);
SELECT wkt([(139.6, 35.69451), (139.60001, 35.69452), (139.6, 35.69453)]::Ring);
SELECT wkt([(139.6, 35.69451), (139.60001, 35.69452)]::LineString);
SELECT wkt([(139.6, 35.69451), (139.60001, 35.69452)]::MultiPoint);
SELECT wkt([[(139.6, 35.69451), (139.60001, 35.69452)]]::MultiLineString);
SELECT wkt([[(139.6, 35.69451), (139.60001, 35.69452), (139.6, 35.69453)]]::Polygon);
SELECT wkt([[[(139.6, 35.69451), (139.60001, 35.69452), (139.6, 35.69453)]]]::MultiPolygon);

-- The result can be read back without loss.
SELECT count() FROM (SELECT (number / 7 - 180, number / 3 - 90)::Point AS p FROM numbers(1000)) WHERE readWKT(wkt(p)) != p;
SELECT count() FROM (SELECT (sin(number) * 180, cos(number) * 90)::Point AS p FROM numbers(1000)) WHERE readWKT(wkt(p)) != p;
