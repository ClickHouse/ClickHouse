-- `FQDN` is a constant on a single server, like `hostName`, so a condition on it can use the primary key.
SELECT isConstant(FQDN()), isConstant(fullHostName()), FQDN() = fullHostName();

DROP TABLE IF EXISTS t_fqdn;
CREATE TABLE t_fqdn (host String, x UInt64) ENGINE = MergeTree ORDER BY host;
INSERT INTO t_fqdn SELECT if(number = 0, FQDN(), 'other-' || toString(number)), number FROM numbers(10);

SELECT count() FROM t_fqdn WHERE host = FQDN() SETTINGS force_primary_key = 1;

-- In a distributed query every server evaluates it itself.
SELECT count() FROM remote('127.0.0.{1,2}', currentDatabase(), t_fqdn) WHERE host = FQDN();
SELECT DISTINCT FQDN() = (SELECT FQDN()) FROM remote('127.0.0.{1,2}', system.one);

DROP TABLE t_fqdn;
