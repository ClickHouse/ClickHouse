-- The t-test aggregate functions already evaluate a Student-t tail internally, for a statistic
-- they derive from the sample themselves. `studentTCDF` must agree with them, so that the p-value
-- an aggregate reports is reproducible from the t-statistic it reports and the two cannot
-- silently drift apart.
--
-- Comparisons are relative rather than absolute: these p-values reach 1e-42, where an absolute
-- tolerance would pass without testing anything.

DROP TABLE IF EXISTS student_t_cdf_consistency;
CREATE TABLE student_t_cdf_consistency (g UInt8, v Float64) ENGINE = Memory;

INSERT INTO student_t_cdf_consistency
SELECT number % 7 AS g, sin(number * 0.7) * 3 + g AS v FROM numbers(700);

-- One-sample: degrees of freedom are n - 1. The groups span p from 0.99 down to 1e-42.
SELECT max(abs(reported - recomputed) / reported) < 1e-12
FROM
(
    SELECT (r.2) AS reported, 2 * studentTCDF(-abs(r.1), n - 1) AS recomputed
    FROM (SELECT studentTTestOneSample(v, 1.0) AS r, count() AS n FROM student_t_cdf_consistency GROUP BY g)
);

-- Two-sample Student's t pools the degrees of freedom as n1 + n2 - 2.
SELECT max(abs(reported - recomputed) / reported) < 1e-12
FROM
(
    SELECT (r.2) AS reported, 2 * studentTCDF(-abs(r.1), n - 2) AS recomputed
    FROM (SELECT studentTTest(v, g > 3) AS r, count() AS n FROM student_t_cdf_consistency GROUP BY g % 2)
);

DROP TABLE student_t_cdf_consistency;
