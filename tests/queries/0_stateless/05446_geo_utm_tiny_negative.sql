-- band letter and round trip for latitudes just below the equator
-- https://github.com/ClickHouse/ClickHouse/issues/127516

SELECT '-- band letter and round trip for latitudes just below the equator';
WITH (-3., -1e-15) AS p, geoToUTM(p.1, p.2) AS u, UTMToGeo(u.1, u.2, u.3, u.4) AS g
    SELECT u.4, abs(g.1 - p.1) < 1e-6 AND abs(g.2 - p.2) < 1e-6;
WITH (-3., -1e-14) AS p, geoToUTM(p.1, p.2) AS u, UTMToGeo(u.1, u.2, u.3, u.4) AS g
    SELECT u.4, abs(g.1 - p.1) < 1e-6 AND abs(g.2 - p.2) < 1e-6;
WITH (-3., -1e-308) AS p, geoToUTM(p.1, p.2) AS u, UTMToGeo(u.1, u.2, u.3, u.4) AS g
    SELECT u.4, abs(g.1 - p.1) < 1e-6 AND abs(g.2 - p.2) < 1e-6;
WITH (-3., -4.9406564584124654e-324) AS p, geoToUTM(p.1, p.2) AS u, UTMToGeo(u.1, u.2, u.3, u.4) AS g
    SELECT u.4, abs(g.1 - p.1) < 1e-6 AND abs(g.2 - p.2) < 1e-6;

SELECT '-- controls: zero and positive latitudes stay northern';
WITH (-3., 0.) AS p, geoToUTM(p.1, p.2) AS u, UTMToGeo(u.1, u.2, u.3, u.4) AS g
    SELECT u.4, abs(g.1 - p.1) < 1e-6 AND abs(g.2 - p.2) < 1e-6;
WITH (-3., 4.) AS p, geoToUTM(p.1, p.2) AS u, UTMToGeo(u.1, u.2, u.3, u.4) AS g
    SELECT u.4, abs(g.1 - p.1) < 1e-6 AND abs(g.2 - p.2) < 1e-6;
