-- A range join of many rows against a few intervals, every join kind, checked against the
-- block nested loop join (`join_algorithm = 'hash'` has no keys for these conditions). The
-- few right-side rows land far apart in the IEJoin L1 order, among 200000 left-side rows.

SET query_plan_join_swap_table = 0;

DROP TABLE IF EXISTS ie_points;
DROP TABLE IF EXISTS ie_ranges;

CREATE TABLE ie_points (id UInt32, x Int32) ENGINE = MergeTree ORDER BY id;
CREATE TABLE ie_ranges (id UInt32, lo Int32, hi Int32) ENGINE = MergeTree ORDER BY id;

INSERT INTO ie_points SELECT number, number % 20 FROM numbers(200000);
INSERT INTO ie_ranges VALUES (1, 0, 15), (2, 1, 1), (3, 2, 2), (4, 3, 15), (5, 4, 6), (6, 5, 5), (7, 6, 6), (8, 7, 15),
    (9, 8, 12), (10, 9, 9), (11, 10, 10), (12, 11, 11), (13, 12, 12), (14, 13, 15), (15, 14, 14), (16, 15, 15), (17, 30, 40);

SELECT count() > 0 FROM (EXPLAIN SELECT count() FROM ie_points p JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'ie_join') WHERE explain LIKE '%IEJoin%';

SELECT kind, ie = oracle FROM
(
    SELECT 'INNER' AS kind,
        (SELECT count(), sum(cityHash64(p.id, r.id)) FROM ie_points p INNER JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'ie_join') AS ie,
        (SELECT count(), sum(cityHash64(p.id, r.id)) FROM ie_points p INNER JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'hash') AS oracle
    UNION ALL SELECT 'INNER residual',
        (SELECT count(), sum(cityHash64(p.id, r.id)) FROM ie_points p INNER JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi AND (p.id + r.id) % 3 = 0 SETTINGS join_algorithm = 'ie_join'),
        (SELECT count(), sum(cityHash64(p.id, r.id)) FROM ie_points p INNER JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi AND (p.id + r.id) % 3 = 0 SETTINGS join_algorithm = 'hash')
    UNION ALL SELECT 'LEFT',
        (SELECT count(), sum(cityHash64(p.id, r.id)) FROM ie_points p LEFT JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'ie_join'),
        (SELECT count(), sum(cityHash64(p.id, r.id)) FROM ie_points p LEFT JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'hash')
    UNION ALL SELECT 'RIGHT',
        (SELECT count(), sum(cityHash64(p.id, r.id)) FROM ie_points p RIGHT JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'ie_join'),
        (SELECT count(), sum(cityHash64(p.id, r.id)) FROM ie_points p RIGHT JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'hash')
    UNION ALL SELECT 'FULL',
        (SELECT count(), sum(cityHash64(p.id, r.id)) FROM ie_points p FULL JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'ie_join'),
        (SELECT count(), sum(cityHash64(p.id, r.id)) FROM ie_points p FULL JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'hash')
    UNION ALL SELECT 'LEFT SEMI',
        (SELECT count(), sum(cityHash64(p.id)) FROM ie_points p LEFT SEMI JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'ie_join'),
        (SELECT count(), sum(cityHash64(p.id)) FROM ie_points p LEFT SEMI JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'hash')
    UNION ALL SELECT 'LEFT ANTI',
        (SELECT count(), sum(cityHash64(p.id)) FROM ie_points p LEFT ANTI JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'ie_join'),
        (SELECT count(), sum(cityHash64(p.id)) FROM ie_points p LEFT ANTI JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'hash')
    UNION ALL SELECT 'RIGHT SEMI',
        (SELECT count(), sum(cityHash64(r.id)) FROM ie_points p RIGHT SEMI JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'ie_join'),
        (SELECT count(), sum(cityHash64(r.id)) FROM ie_points p RIGHT SEMI JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'hash')
    UNION ALL SELECT 'RIGHT ANTI',
        (SELECT count(), sum(cityHash64(r.id)) FROM ie_points p RIGHT ANTI JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'ie_join'),
        (SELECT count(), sum(cityHash64(r.id)) FROM ie_points p RIGHT ANTI JOIN ie_ranges r ON p.x >= r.lo AND p.x <= r.hi SETTINGS join_algorithm = 'hash')
)
ORDER BY kind;

DROP TABLE ie_points;
DROP TABLE ie_ranges;
