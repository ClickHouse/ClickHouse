DROP TABLE IF EXISTS map_test;

CREATE TABLE map_test
(
    row_id UInt32,
    map Map(String, String),
    INDEX map_bloom_filter_keys mapKeys(map) TYPE bloom_filter GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id;

INSERT INTO map_test VALUES (0, {'K0':'V0'}), (1, {'K1':'V1'});

SELECT 'Equals with existing key';
SELECT * FROM map_test WHERE map['K0'] = 'V0';

SELECT 'Equals with non existing key';
SELECT * FROM map_test WHERE map['K2'] = 'V2';

SELECT 'Equals with non existing key and default value';
SELECT * FROM map_test WHERE map['K3'] = '';

SELECT 'Not equals with existing key';
SELECT * FROM map_test WHERE map['K0'] != 'V0';

SELECT 'Not equals with non existing key';
SELECT * FROM map_test WHERE map['K2'] != 'V2';

SELECT 'Not equals with non existing key and default value';
SELECT * FROM map_test WHERE map['K3'] != '';

DROP TABLE map_test;
