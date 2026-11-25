-- Tags: no-fasttest
-- Tag no-fasttest: PromQL needs ANTLR4, which is disabled in the fast-test build.

-- PromQL functions `sort_by_label` and `sort_by_label_desc` order an instant vector by the values of the specified
-- labels, compared in natural order (`cpu2` before `cpu10`), as in Prometheus. Series with equal values of these
-- labels are ordered by all their labels, so the result is deterministic. The order is applied only when
-- `sort_by_label*` is the root of an instant query: under another function, inside a subquery, or in a range query,
-- the result keeps the usual order by labels.

SET enable_time_series_table = 1;
SET session_timezone = 'UTC';

DROP TABLE IF EXISTS ts;
CREATE TABLE ts ENGINE = TimeSeries;

-- The data of `functions.test` for `sort_by_label` and `sort_by_label_desc` (Prometheus): one sample every 300 seconds,
-- 11 samples 0, step, ..., 10 * step at 0, 300, ..., 3000 seconds. `step = 0` stands for the series of 11 NaN samples.

-- Scenarios: one, two and three labels; both directions; no label names; an empty label name; a label that no
-- series has; a filter before the sort; the cases where `sort_by_label*` is not the root; a range query.
-- Series that are equal by the listed labels are ordered by all their labels (`group`, `instance`, `job`), so the
-- label lists below are chosen to give an order that this tie-break alone would not give.
INSERT INTO ts (metric_name, tags, samples)
SELECT 'http_requests', tags, arrayMap(i -> (toDateTime64(i * 300, 3), if(step = 0, nan, toFloat64(i * step))), range(11))
FROM VALUES('tags Map(String, String), step UInt32',
    (map('job', 'api-server', 'instance', '0', 'group', 'production'), 10),
    (map('job', 'api-server', 'instance', '1', 'group', 'production'), 20),
    (map('job', 'api-server', 'instance', '0', 'group', 'canary'), 30),
    (map('job', 'api-server', 'instance', '1', 'group', 'canary'), 40),
    (map('job', 'api-server', 'instance', '2', 'group', 'canary'), 0),
    (map('job', 'app-server', 'instance', '0', 'group', 'production'), 50),
    (map('job', 'app-server', 'instance', '1', 'group', 'production'), 60),
    (map('job', 'app-server', 'instance', '0', 'group', 'canary'), 70),
    (map('job', 'app-server', 'instance', '1', 'group', 'canary'), 80),
    (map('job', 'api-server', 'instance', '2', 'group', 'production'), 10));

-- Scenario: numbers are compared as numbers, not as text: 2 before 10, and 21 before 100.
INSERT INTO ts (metric_name, tags, samples)
SELECT 'cpu_time_total', tags, arrayMap(i -> (toDateTime64(i * 300, 3), toFloat64(i * step)), range(11))
FROM VALUES('tags Map(String, String), step UInt32',
    (map('job', 'cpu', 'cpu', '0'), 10),
    (map('job', 'cpu', 'cpu', '1'), 10),
    (map('job', 'cpu', 'cpu', '2'), 10),
    (map('job', 'cpu', 'cpu', '3'), 10),
    (map('job', 'cpu', 'cpu', '10'), 10),
    (map('job', 'cpu', 'cpu', '11'), 10),
    (map('job', 'cpu', 'cpu', '12'), 10),
    (map('job', 'cpu', 'cpu', '20'), 10),
    (map('job', 'cpu', 'cpu', '21'), 10),
    (map('job', 'cpu', 'cpu', '100'), 10));

-- Scenarios: a value made of several pieces. `instance` mixes digits and text (`4m5`, `4m600`, `4m1000`);
-- `release` is a version (`1.2.3`, `1.11.3`, `1.111.3`). The two labels give opposite orders of the same series.
INSERT INTO ts (metric_name, tags, samples)
SELECT 'node_uname_info', tags, arrayMap(i -> (toDateTime64(i * 300, 3), toFloat64(i * step)), range(11))
FROM VALUES('tags Map(String, String), step UInt32',
    (map('job', 'node_exporter', 'instance', '4m600', 'release', '1.2.3'), 10),
    (map('job', 'node_exporter', 'instance', '4m5', 'release', '1.11.3'), 10),
    (map('job', 'node_exporter', 'instance', '4m1000', 'release', '1.111.3'), 10));

-- The data below is not from `functions.test`. Every series has one sample, 1, at 3000 seconds, and is inserted
-- out of order.

-- Scenario: text next to digits. A digit piece goes before a text piece, so `host2` and `host10` go before `host-1`,
-- although `-` is before the digits in ASCII. A value that is the beginning of another one goes first (`host`).
-- Checked on Prometheus 3.16.0: host, host1, host2, host10, host-1, host.1, hostA, host_1.
INSERT INTO ts (metric_name, tags, samples)
SELECT 'natural_order', map('host', host), [(toDateTime64(3000, 3), 1)]
FROM VALUES('host String', 'host_1', 'host10', 'hostA', 'host-1', 'host', 'host2', 'host.1', 'host1');

-- Scenario: digit and text pieces alternate several times, as in `4m5656host87767` (digits, text, digits, text, digits).
-- Each value differs from `4m5656host87767` at another piece: the 1st (`10`), the 2nd (`n`), the 3rd (`600`),
-- the 4th (`node`), the 5th (`9`), or by having one piece less (`4m5656host`) or one more (`4m5656host87767a`).
-- Checked on Prometheus 3.16.0: 4m600host1, 4m5656host, 4m5656host9, 4m5656host87767, 4m5656host87767a, 4m5656node1,
-- 4n1host1, 10m1host1.
INSERT INTO ts (metric_name, tags, samples)
SELECT 'alternating_pieces', map('id', id), [(toDateTime64(3000, 3), 1)]
FROM VALUES('id String', '4m5656node1', '10m1host1', '4m5656host87767', '4m5656host', '4n1host1', '4m5656host9',
    '4m5656host87767a', '4m600host1');

-- Scenario: the first character is before `0` in ASCII (`+`, `-`, `.`, `/`), a digit, or after `9` (`:`, `A`, `_`, `a`).
-- Checked on Prometheus 3.16.0: +1, -1, .1, /1, 1, :1, A1, _1, a1.
INSERT INTO ts (metric_name, tags, samples)
SELECT 'first_character', map('id', id), [(toDateTime64(3000, 3), 1)]
FROM VALUES('id String', 'a1', '/1', ':1', '-1', '1', '_1', '+1', 'A1', '.1');

-- Scenario: leading zeros. `7` and `0007` are the same number, and so are `0` and `00`. Prometheus returns such a pair
-- in an order that depends on the order of its input, which a table cannot reproduce, so here they are ordered by labels, as text:
-- 0, 00, 0007, 7, 08, 9, 010.
INSERT INTO ts (metric_name, tags, samples)
SELECT 'leading_zeros', map('id', id), [(toDateTime64(3000, 3), 1)]
FROM VALUES('id String', '9', '0007', '010', '0', '7', '08', '00');

-- Scenario: numbers that do not fit into 64 bits. 9223372036854775807 is the largest Int64 and 18446744073709551616
-- is 2^64. Here they are still compared as numbers: 5, 9223372036854775807, 9223372036854775808,
-- 18446744073709551616, 99999999999999999999, 100000000000000000000.
-- Prometheus 3.16.0 differs: it compares a number above Int64 as text, and returns
-- 100000000000000000000, 18446744073709551616, 5, 9223372036854775807, 9223372036854775808, 99999999999999999999.
INSERT INTO ts (metric_name, tags, samples)
SELECT 'big_numbers', map('id', id), [(toDateTime64(3000, 3), 1)]
FROM VALUES('id String', '99999999999999999999', '5', '18446744073709551616', '9223372036854775808',
    '100000000000000000000', '9223372036854775807');

SELECT '-- one label';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(http_requests, "instance")', 3000);

-- Within one `instance` the series are ordered by `job` and then by `group`; the tie-break alone would put `group` first.
SELECT '-- two labels';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(http_requests, "instance", "job")', 3000);

SELECT '-- three labels';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(http_requests, "job", "instance", "group")', 3000);

SELECT '-- desc one label';
SELECT * FROM prometheusQuery('ts', 'sort_by_label_desc(http_requests, "instance")', 3000);

SELECT '-- desc two labels';
SELECT * FROM prometheusQuery('ts', 'sort_by_label_desc(http_requests, "instance", "job")', 3000);

SELECT '-- numbers';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(cpu_time_total, "cpu")', 3000);

SELECT '-- numbers desc';
SELECT * FROM prometheusQuery('ts', 'sort_by_label_desc(cpu_time_total, "cpu")', 3000);

SELECT '-- mixed pieces';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(node_uname_info, "instance")', 3000);

SELECT '-- versions';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(node_uname_info, "release")', 3000);

SELECT '-- natural order';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(natural_order, "host")', 3000);

SELECT '-- natural order desc';
SELECT * FROM prometheusQuery('ts', 'sort_by_label_desc(natural_order, "host")', 3000);

SELECT '-- alternating pieces';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(alternating_pieces, "id")', 3000);

SELECT '-- alternating pieces desc';
SELECT * FROM prometheusQuery('ts', 'sort_by_label_desc(alternating_pieces, "id")', 3000);

SELECT '-- first character';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(first_character, "id")', 3000);

-- Differs from Prometheus, which returns equal numbers (`7` and `0007`) in an order that depends on the order of its input.
SELECT '-- leading zeros';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(leading_zeros, "id")', 3000);

SELECT '-- leading zeros desc';
SELECT * FROM prometheusQuery('ts', 'sort_by_label_desc(leading_zeros, "id")', 3000);

-- Differs from Prometheus, which compares a number above Int64 as text.
SELECT '-- big numbers';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(big_numbers, "id")', 3000);

SELECT '-- big numbers desc';
SELECT * FROM prometheusQuery('ts', 'sort_by_label_desc(big_numbers, "id")', 3000);

-- Without label names the series are ordered by all their labels.
SELECT '-- no labels';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(http_requests)', 3000);

SELECT '-- desc no labels';
SELECT * FROM prometheusQuery('ts', 'sort_by_label_desc(http_requests)', 3000);

-- No series has a label with an empty name, so it decides nothing, as in Prometheus.
SELECT '-- empty label name';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(http_requests, "")', 3000);

SELECT '-- empty then label';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(http_requests, "", "instance")', 3000);

-- No series has the label `cpu`, so all the series are equal by it and are ordered by all their labels.
SELECT '-- missing label';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(http_requests, "cpu")', 3000);

-- Only the series of `cpu_time_total` have the label `cpu`. A series without the label goes first here.
-- Prometheus returns such series in an order that depends on the order of its input.
SELECT '-- partly missing label';
SELECT * FROM prometheusQuery('ts', 'sort_by_label({job=~"cpu|node_exporter"}, "cpu")', 3000);

-- The series removed by the filter (and the one with NaN) are not in the result.
SELECT '-- filtered';
SELECT * FROM prometheusQuery('ts', 'sort_by_label(http_requests > 300, "instance")', 3000);

-- Not the root: `abs` is. Prometheus keeps the order of `sort_by_label_desc` here; this implementation orders by labels.
SELECT '-- under abs';
SELECT * FROM prometheusQuery('ts', 'abs(sort_by_label_desc(http_requests, "instance"))', 3000);

SELECT '-- in subquery';
SELECT * FROM prometheusQuery('ts', 'last_over_time(sort_by_label_desc(http_requests, "instance")[5m:1m])', 3000);

-- In a range query `sort_by_label*` has no effect and the rows stay ordered by labels, as in Prometheus.
SELECT '-- range query';
SELECT * FROM prometheusQueryRange('ts', 'sort_by_label_desc(http_requests, "instance")', 2400, 3000, 300);

-- The first argument must be an instant vector, and each other argument must be a string.
SELECT '-- wrong arguments';
SELECT * FROM prometheusQuery('ts', 'sort_by_label()', 3000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'sort_by_label_desc()', 3000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'sort_by_label(1)', 3000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'sort_by_label("instance")', 3000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'sort_by_label(http_requests[5m], "instance")', 3000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'sort_by_label(http_requests, 1)', 3000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'sort_by_label(http_requests, instance)', 3000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'sort_by_label(http_requests, "instance", 1)', 3000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }
SELECT * FROM prometheusQuery('ts', 'sort_by_label_desc(http_requests, 1)', 3000); -- { serverError CANNOT_EXECUTE_PROMQL_QUERY }

DROP TABLE ts;
