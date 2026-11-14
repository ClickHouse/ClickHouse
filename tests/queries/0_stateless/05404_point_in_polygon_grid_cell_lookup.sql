-- A constant polygon with a vertex far from the point must give the same answer as a non-constant one.
-- Provenance: https://github.com/ClickHouse/ClickHouse/issues/125289

-- Past the side of the bounding box opposite to a far vertex.
SELECT pointInPolygon((0.5, 5.), [(0., 0.), (1., 0.), (1., -1e20), (0., -1e20)]), pointInPolygon((0.5, 5.), materialize([(0., 0.), (1., 0.), (1., -1e20), (0., -1e20)]));
SELECT pointInPolygon((5., 0.5), [(0., 0.), (0., 1.), (-1e20, 1.), (-1e20, 0.)]), pointInPolygon((5., 0.5), materialize([(0., 0.), (0., 1.), (-1e20, 1.), (-1e20, 0.)]));
-- Next to an edge, with far vertices on both sides.
SELECT pointInPolygon((-1e-10, 0.5), [(-1e-11, 0.), (1e18, 0.), (1e18, 1.), (-1e18, 1.), (-1e18, 0.9), (-1e-11, 0.9)]), pointInPolygon((-1e-10, 0.5), materialize([(-1e-11, 0.), (1e18, 0.), (1e18, 1.), (-1e18, 1.), (-1e18, 0.9), (-1e-11, 0.9)]));
SELECT pointInPolygon((0.5, -1e-10), [(0., -1e-11), (0., 1e18), (1., 1e18), (1., -1e18), (0.9, -1e18), (0.9, -1e-11)]), pointInPolygon((0.5, -1e-10), materialize([(0., -1e-11), (0., 1e18), (1., 1e18), (1., -1e18), (0.9, -1e18), (0.9, -1e-11)]));
SELECT pointInPolygon((-1e-10, 0.5), [[[(-1e-11, 0.), (1e18, 0.), (1e18, 1.), (-1e18, 1.), (-1e18, 0.9), (-1e-11, 0.9)]]]);
-- Near the side opposite to a far vertex, outside and inside.
SELECT pointInPolygon((0.9, 0.1), [(-1e20, 0.), (0.5, 0.), (1., 0.5), (0.5, 1.), (-1e20, 1.)]), pointInPolygon((0.55, 0.1), [(-1e20, 0.), (0.5, 0.), (1., 0.5), (0.5, 1.), (-1e20, 1.)]), pointInPolygon((0.9, 0.1), materialize([(-1e20, 0.), (0.5, 0.), (1., 0.5), (0.5, 1.), (-1e20, 1.)])), pointInPolygon((0.55, 0.1), materialize([(-1e20, 0.), (0.5, 0.), (1., 0.5), (0.5, 1.), (-1e20, 1.)]));
SELECT pointInPolygon((0.1, 0.9), [(0., -1e20), (1., -1e20), (1., 0.5), (0.5, 1.), (0., 0.5)]), pointInPolygon((0.5, 0.9), [(0., -1e20), (1., -1e20), (1., 0.5), (0.5, 1.), (0., 0.5)]), pointInPolygon((0.1, 0.9), materialize([(0., -1e20), (1., -1e20), (1., 0.5), (0.5, 1.), (0., 0.5)])), pointInPolygon((0.5, 0.9), materialize([(0., -1e20), (1., -1e20), (1., 0.5), (0.5, 1.), (0., 0.5)]));
SELECT pointInPolygon((0.9, 0.1), [[[(-1e20, 0.), (0.5, 0.), (1., 0.5), (0.5, 1.), (-1e20, 1.)]]]), pointInPolygon((0.55, 0.1), [[[(-1e20, 0.), (0.5, 0.), (1., 0.5), (0.5, 1.), (-1e20, 1.)]]]);

-- Primary key analysis and a full scan agree.
DROP TABLE IF EXISTS pip_pk;
DROP TABLE IF EXISTS pip_nopk;
CREATE TABLE pip_pk (x Float64, y Float64) ENGINE = MergeTree ORDER BY (x, y) SETTINGS index_granularity = 8, index_granularity_bytes = 0, min_bytes_for_wide_part = 0;
CREATE TABLE pip_nopk (x Float64, y Float64) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO pip_pk SELECT (number % 100) * 0.1, (intDiv(number, 100) % 100) * 0.1 FROM numbers(10000);
INSERT INTO pip_nopk SELECT * FROM pip_pk;
SELECT (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(-0.05, 0.05), (1.05, 0.05), (1.05, -1e20), (-0.05, -1e20)]) SETTINGS force_primary_key = 1), (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(-0.05, 0.05), (1.05, 0.05), (1.05, -1e20), (-0.05, -1e20)]));
SELECT (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(-1e20, 0.05), (0.53, 0.05), (1.03, 0.55), (0.53, 1.05), (-1e20, 1.05)]) SETTINGS force_primary_key = 1), (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(-1e20, 0.05), (0.53, 0.05), (1.03, 0.55), (0.53, 1.05), (-1e20, 1.05)]));
SELECT (SELECT count() FROM pip_pk WHERE pointInPolygon((x, y), [(0.05, -1e20), (1.05, -1e20), (1.05, 0.53), (0.55, 1.03), (0.05, 0.53)]) SETTINGS force_primary_key = 1), (SELECT count() FROM pip_nopk WHERE pointInPolygon((x, y), [(0.05, -1e20), (1.05, -1e20), (1.05, 0.53), (0.55, 1.03), (0.05, 0.53)]));
DROP TABLE pip_pk;
DROP TABLE pip_nopk;
