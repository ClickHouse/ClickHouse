SET in_null_as_unknown = 1;

DROP TABLE IF EXISTS squality_uuid_identifier_check_in;
CREATE TABLE squality_uuid_identifier_check_in
(
    x UUID,
    y UUID,
    s String,
    CONSTRAINT c CHECK x NOT IN (y, s)
) ENGINE = Memory;

INSERT INTO squality_uuid_identifier_check_in VALUES
    ('00000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000002', 'bad-uuid');
SELECT x NOT IN (y, s) FROM squality_uuid_identifier_check_in;

DROP TABLE squality_uuid_identifier_check_in;
