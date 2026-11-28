-- an `IPv4` compares with an `IPv6` as the mapped address, the constraint graph must not compare their constants as they are

SET convert_query_to_cnf = 1, optimize_substitute_columns = 0;

DROP TABLE IF EXISTS t_ipv4_bound;
DROP TABLE IF EXISTS t_ip_two_columns;
DROP TABLE IF EXISTS t_ip_two_bounds;

-- the constraint bound is an `IPv4`, the query constant is an `IPv6`
CREATE TABLE t_ipv4_bound (ip IPv4, CONSTRAINT c CHECK ip = toIPv4('127.0.0.1')) ENGINE = Memory;
INSERT INTO t_ipv4_bound VALUES ('127.0.0.1');
SELECT 'ipv4 bound', count() FROM t_ipv4_bound WHERE ip = toIPv6('::ffff:127.0.0.1') SETTINGS optimize_using_constraints = 0;
SELECT 'ipv4 bound', count() FROM t_ipv4_bound WHERE ip = toIPv6('::ffff:127.0.0.1') SETTINGS optimize_using_constraints = 1;
SELECT 'ipv4 bound', count() FROM t_ipv4_bound WHERE ip < toIPv6('::ffff:127.0.0.2') SETTINGS optimize_using_constraints = 1;

-- two columns with constraints of different families, the query uses only the `IPv4` column
CREATE TABLE t_ip_two_columns
(
    v4 IPv4,
    v6 IPv6,
    CONSTRAINT c4 CHECK v4 = toIPv4('127.0.0.1'),
    CONSTRAINT c6 CHECK v6 = toIPv6('::ffff:127.0.0.1')
) ENGINE = Memory;
INSERT INTO t_ip_two_columns VALUES ('127.0.0.1', '::ffff:127.0.0.1');
SELECT 'two columns', count() FROM t_ip_two_columns WHERE v4 = toIPv4('127.0.0.1') SETTINGS optimize_using_constraints = 0;
SELECT 'two columns', count() FROM t_ip_two_columns WHERE v4 = toIPv4('127.0.0.1') SETTINGS optimize_using_constraints = 1;
SELECT 'two columns', count() FROM t_ip_two_columns WHERE v6 = toIPv6('::ffff:127.0.0.1') SETTINGS optimize_using_constraints = 1;

-- one column with upper bounds of both families
CREATE TABLE t_ip_two_bounds
(
    ip IPv4,
    CONSTRAINT c4 CHECK ip < toIPv4('2.0.0.0'),
    CONSTRAINT c6 CHECK ip < toIPv6('::ffff:3.0.0.0')
) ENGINE = Memory;
INSERT INTO t_ip_two_bounds VALUES ('1.0.0.1');
SELECT 'two bounds', count() FROM t_ip_two_bounds WHERE ip > toIPv4('1.0.0.0') SETTINGS optimize_using_constraints = 0;
SELECT 'two bounds', count() FROM t_ip_two_bounds WHERE ip > toIPv4('1.0.0.0') SETTINGS optimize_using_constraints = 1;
SELECT 'two bounds', count() FROM t_ip_two_bounds WHERE ip > toIPv6('::ffff:1.0.0.0') SETTINGS optimize_using_constraints = 1;

DROP TABLE t_ipv4_bound;
DROP TABLE t_ip_two_columns;
DROP TABLE t_ip_two_bounds;
