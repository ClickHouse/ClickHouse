DROP TABLE IF EXISTS map_test_index_map_keys;
CREATE TABLE map_test_index_map_keys
(
    row_id UInt32,
    map Map(String, String),
    INDEX map_bloom_filter_keys mapKeys(map) TYPE bloom_filter GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id;

INSERT INTO map_test_index_map_keys VALUES (0, {'K0':'V0'}), (1, {'K1':'V1'});

SELECT 'Map bloom filter mapKeys';

SELECT 'Equals with existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K0'] = 'V0';
SELECT 'Equals with non existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K2'] = 'V2';
SELECT 'Equals with non existing key and default value';
SELECT * FROM map_test_index_map_keys WHERE map['K3'] = '';
SELECT 'Not equals with existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K0'] != 'V0';
SELECT 'Not equals with non existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K2'] != 'V2';
SELECT 'Not equals with non existing key and default value';
SELECT * FROM map_test_index_map_keys WHERE map['K3'] != '';

SELECT 'IN with existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K0'] IN 'V0';
SELECT 'IN with non existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K2'] IN 'V2';
SELECT 'IN with non existing key and default value';
SELECT * FROM map_test_index_map_keys WHERE map['K3'] IN '';
SELECT 'NOT IN with existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K0'] NOT IN 'V0';
SELECT 'NOT IN with non existing key';
SELECT * FROM map_test_index_map_keys WHERE map['K2'] NOT IN 'V2';
SELECT 'NOT IN with non existing key and default value';
SELECT * FROM map_test_index_map_keys WHERE map['K3'] NOT IN '';

SELECT 'MapContains with existing key';
SELECT * FROM map_test_index_map_keys WHERE mapContains(map, 'K0');
SELECT 'MapContains with non existing key';
SELECT * FROM map_test_index_map_keys WHERE mapContains(map, 'K2');
SELECT 'MapContains with non existing key and default value';
SELECT * FROM map_test_index_map_keys WHERE mapContains(map, '');

SELECT 'Has with existing key';
SELECT * FROM map_test_index_map_keys WHERE has(map, 'K0');
SELECT 'Has with non existing key';
SELECT * FROM map_test_index_map_keys WHERE has(map, 'K2');
SELECT 'Has with non existing key and default value';
SELECT * FROM map_test_index_map_keys WHERE has(map, '');

DROP TABLE map_test_index_map_keys;

DROP TABLE IF EXISTS map_test_index_map_values;
CREATE TABLE map_test_index_map_values
(
    row_id UInt32,
    map Map(String, String),
    INDEX map_bloom_filter_values mapValues(map) TYPE bloom_filter GRANULARITY 1
) Engine=MergeTree() ORDER BY row_id;

INSERT INTO map_test_index_map_values VALUES (0, {'K0':'V0'}), (1, {'K1':'V1'});

SELECT 'Map bloom filter mapValues';

SELECT 'IN with existing key';
SELECT * FROM map_test_index_map_values WHERE map['K0'] IN 'V0';
SELECT 'IN with non existing key';
SELECT * FROM map_test_index_map_values WHERE map['K2'] IN 'V2';
SELECT 'IN with non existing key and default value';
SELECT * FROM map_test_index_map_values WHERE map['K3'] IN '';
SELECT 'NOT IN with existing key';
SELECT * FROM map_test_index_map_values WHERE map['K0'] NOT IN 'V0';
SELECT 'NOT IN with non existing key';
SELECT * FROM map_test_index_map_values WHERE map['K2'] NOT IN 'V2';
SELECT 'NOT IN with non existing key and default value';
SELECT * FROM map_test_index_map_values WHERE map['K3'] NOT IN '';

DROP TABLE map_test_index_map_values;
