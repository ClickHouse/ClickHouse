SET in_null_as_unknown = 1;

SELECT toUUID('00000000-0000-0000-0000-000000000001') NOT IN materialize('bad-uuid');
SELECT toIPv4('1.2.3.4') NOT IN materialize('bad-ip');

DROP TABLE IF EXISTS squality_in_uuid_check;
CREATE TABLE squality_in_uuid_check
(
    u UUID,
    v String,
    CONSTRAINT c CHECK u NOT IN (v, 'also-bad-uuid')
) ENGINE = Memory;
INSERT INTO squality_in_uuid_check
SELECT toUUID('00000000-0000-0000-0000-000000000001'), materialize('bad-uuid');
SELECT count() FROM squality_in_uuid_check;
DROP TABLE squality_in_uuid_check;

DROP TABLE IF EXISTS squality_in_ipv4_check;
CREATE TABLE squality_in_ipv4_check
(
    u IPv4,
    v String,
    CONSTRAINT c CHECK u NOT IN (v, 'also-bad-ip')
) ENGINE = Memory;
INSERT INTO squality_in_ipv4_check SELECT toIPv4('1.2.3.4'), materialize('bad-ip');
SELECT count() FROM squality_in_ipv4_check;
DROP TABLE squality_in_ipv4_check;
