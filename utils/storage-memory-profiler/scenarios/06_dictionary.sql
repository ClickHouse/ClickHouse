-- Test memory for dictionaries
CREATE DICTIONARY test_dict (
    id UInt64,
    name String
) PRIMARY KEY id
SOURCE(NULL())
LAYOUT(FLAT())
LIFETIME(MIN 0 MAX 1000);

-- Load dictionary
SELECT dictGetOrDefault('test_dict', 'name', toUInt64(1), 'missing');
