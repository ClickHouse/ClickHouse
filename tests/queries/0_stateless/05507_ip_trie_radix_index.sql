-- With the parameter `RADIX_PREFIX`, `ip_trie` narrows down its binary search with a radix index on the top bits of addresses,
-- for each address family with at least 10000 subnets. Results must not depend on it: check lookups with and without each index, including
-- addresses around the range of IPv4-mapped addresses, which have their own index in a dictionary with IPv6 subnets.
-- With a value, `RADIX_PREFIX` chooses the size of the index whatever the number of subnets. Without it, there is no index.

DROP DICTIONARY IF EXISTS ip_trie_radix_v4;
DROP DICTIONARY IF EXISTS ip_trie_radix_v6;
DROP DICTIONARY IF EXISTS ip_trie_radix_mixed;
DROP DICTIONARY IF EXISTS ip_trie_radix_few_ipv4;
DROP DICTIONARY IF EXISTS ip_trie_radix_few_ipv6;
DROP DICTIONARY IF EXISTS ip_trie_radix_small;
DROP DICTIONARY IF EXISTS ip_trie_radix_below_threshold;
DROP DICTIONARY IF EXISTS ip_trie_radix_at_threshold;
DROP DICTIONARY IF EXISTS ip_trie_radix_few_ipv4_off;
DROP DICTIONARY IF EXISTS ip_trie_radix_few_ipv6_off;
DROP DICTIONARY IF EXISTS ip_trie_radix_below_threshold_off;
DROP DICTIONARY IF EXISTS ip_trie_radix_at_threshold_off;
DROP DICTIONARY IF EXISTS ip_trie_radix_v4_off;
DROP DICTIONARY IF EXISTS ip_trie_radix_v4_bits_8;
DROP DICTIONARY IF EXISTS ip_trie_radix_v4_bits_22;
DROP DICTIONARY IF EXISTS ip_trie_radix_v6_off;
DROP DICTIONARY IF EXISTS ip_trie_radix_v6_bits_8;
DROP DICTIONARY IF EXISTS ip_trie_radix_v6_bits_22;
DROP DICTIONARY IF EXISTS ip_trie_radix_mixed_off;
DROP DICTIONARY IF EXISTS ip_trie_radix_mixed_bits_8;
DROP DICTIONARY IF EXISTS ip_trie_radix_small_off;
DROP DICTIONARY IF EXISTS ip_trie_radix_small_bits_8;
DROP DICTIONARY IF EXISTS ip_trie_radix_small_bits_22;
DROP DICTIONARY IF EXISTS ip_trie_radix_small_bits_14;
DROP DICTIONARY IF EXISTS ip_trie_radix_small_bits_16;
DROP DICTIONARY IF EXISTS ip_trie_radix_small_bits_18;
DROP DICTIONARY IF EXISTS ip_trie_radix_small_bits_20;
DROP DICTIONARY IF EXISTS ip_trie_radix_small_bits_24;
DROP TABLE IF EXISTS ip_trie_radix_src;
DROP TABLE IF EXISTS ip_trie_radix_queries_v4;
DROP TABLE IF EXISTS ip_trie_radix_queries_v6;

CREATE TABLE ip_trie_radix_src (kind UInt8, prefix String, value UInt64) ENGINE = Memory;

-- Kind 1: 1000 IPv4 subnets /16 starting from 1.0.0.0.
INSERT INTO ip_trie_radix_src
SELECT 1, IPv4NumToString(toUInt32(16777216 + number * 65536)) || '/16', 1000000 + number
FROM numbers(1000);

-- Kind 2: nested in them, a /24 for each even block, and a /32 in each thousandth block. More than enough subnets for an index.
INSERT INTO ip_trie_radix_src
SELECT 2, IPv4NumToString(toUInt32(16777216 + number * 256)) || '/24', 2000000 + number
FROM numbers(240000) WHERE number % 2 = 0;

INSERT INTO ip_trie_radix_src
SELECT 2, IPv4NumToString(toUInt32(16777216 + number * 256 + 1)) || '/32', 3000000 + number
FROM numbers(240000) WHERE number % 1000 = 0;

-- Kind 3: more than enough IPv6 subnets for an index. A /32 for each value of the top 22 bits, nested in 2a00::/16 for the first ones,
-- and a /48 in each thousandth of them.
INSERT INTO ip_trie_radix_src VALUES (3, '2a00::/16', 400);

INSERT INTO ip_trie_radix_src
SELECT 3, format('{}:{}::/32', substring(hex(h), 1, 4), substring(hex(h), 5, 4)), 4000000 + number
FROM (SELECT number, toUInt32(0x2A000000 + number * 1024) AS h FROM numbers(120000));

INSERT INTO ip_trie_radix_src
SELECT 3, format('{}:{}:1::/48', substring(hex(h), 1, 4), substring(hex(h), 5, 4)), 5000000 + number
FROM (SELECT number, toUInt32(0x2A000000 + number * 1024) AS h FROM numbers(120000) WHERE number % 1000 = 0);

-- Kind 4: subnets around the IPv4-mapped addresses.
INSERT INTO ip_trie_radix_src VALUES (4, '::/0', 1), (4, '::ffff:0:0/96', 2);

-- Kinds 5 and 6: one subnet below the number of subnets that gets an index, and the one that reaches it.
INSERT INTO ip_trie_radix_src
SELECT 5, IPv4NumToString(toUInt32(1677721600 + number)) || '/32', number + 1
FROM numbers(9999);

INSERT INTO ip_trie_radix_src VALUES (6, '100.0.39.15/32', 10000);

CREATE DICTIONARY ip_trie_radix_v4 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind IN (1, 2)')) LAYOUT(IP_TRIE(RADIX_PREFIX)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_v6 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 3')) LAYOUT(IP_TRIE(RADIX_PREFIX)) LIFETIME(0);

-- Both indexes.
CREATE DICTIONARY ip_trie_radix_mixed (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind IN (1, 2, 3, 4)')) LAYOUT(IP_TRIE(RADIX_PREFIX)) LIFETIME(0);

-- Only the index for IPv6 addresses.
CREATE DICTIONARY ip_trie_radix_few_ipv4 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind IN (1, 3, 4)')) LAYOUT(IP_TRIE(RADIX_PREFIX)) LIFETIME(0);

-- Only the index for IPv4-mapped addresses.
CREATE DICTIONARY ip_trie_radix_few_ipv6 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind IN (1, 2, 4)')) LAYOUT(IP_TRIE(RADIX_PREFIX)) LIFETIME(0);

-- No index.
CREATE DICTIONARY ip_trie_radix_small (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(RADIX_PREFIX)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_below_threshold (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 5')) LAYOUT(IP_TRIE(RADIX_PREFIX)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_at_threshold (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind IN (5, 6)')) LAYOUT(IP_TRIE(RADIX_PREFIX)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_few_ipv4_off (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind IN (1, 3, 4)')) LAYOUT(IP_TRIE()) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_few_ipv6_off (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind IN (1, 2, 4)')) LAYOUT(IP_TRIE()) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_below_threshold_off (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 5')) LAYOUT(IP_TRIE()) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_at_threshold_off (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind IN (5, 6)')) LAYOUT(IP_TRIE()) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_v4_off (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind IN (1, 2)')) LAYOUT(IP_TRIE()) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_v4_bits_8 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind IN (1, 2)')) LAYOUT(IP_TRIE(RADIX_PREFIX 8)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_v4_bits_22 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind IN (1, 2)')) LAYOUT(IP_TRIE(RADIX_PREFIX 22)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_v6_off (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 3')) LAYOUT(IP_TRIE()) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_v6_bits_8 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 3')) LAYOUT(IP_TRIE(RADIX_PREFIX 8)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_v6_bits_22 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 3')) LAYOUT(IP_TRIE(RADIX_PREFIX 22)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_mixed_off (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind IN (1, 2, 3, 4)')) LAYOUT(IP_TRIE()) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_mixed_bits_8 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind IN (1, 2, 3, 4)')) LAYOUT(IP_TRIE(RADIX_PREFIX 8)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_small_off (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE()) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_small_bits_8 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(RADIX_PREFIX 8)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_small_bits_14 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(RADIX_PREFIX 14)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_small_bits_16 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(RADIX_PREFIX 16)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_small_bits_18 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(RADIX_PREFIX 18)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_small_bits_20 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(RADIX_PREFIX 20)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_small_bits_24 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(RADIX_PREFIX 24)) LIFETIME(0);

CREATE DICTIONARY ip_trie_radix_small_bits_22 (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(RADIX_PREFIX 22)) LIFETIME(0);

-- A size that is not allowed is rejected by the CREATE query: above, between and below the allowed ones, and not an integer.
CREATE DICTIONARY ip_trie_radix_bits_invalid (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(RADIX_PREFIX 25)) LIFETIME(0); -- { serverError BAD_ARGUMENTS }
CREATE DICTIONARY ip_trie_radix_bits_invalid (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(RADIX_PREFIX 15)) LIFETIME(0); -- { serverError BAD_ARGUMENTS }
CREATE DICTIONARY ip_trie_radix_bits_invalid (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(RADIX_PREFIX 7)) LIFETIME(0); -- { serverError BAD_ARGUMENTS }
CREATE DICTIONARY ip_trie_radix_bits_invalid (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(RADIX_PREFIX 0)) LIFETIME(0); -- { serverError BAD_ARGUMENTS }
CREATE DICTIONARY ip_trie_radix_bits_invalid (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(RADIX_PREFIX '16')) LIFETIME(0); -- { serverError BAD_ARGUMENTS }
CREATE DICTIONARY ip_trie_radix_bits_invalid (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(RADIX_PREFIX 16.5)) LIFETIME(0); -- { serverError BAD_ARGUMENTS }
SELECT count() FROM system.dictionaries WHERE database = currentDatabase() AND name = 'ip_trie_radix_bits_invalid';

-- Only this parameter can go without a value.
CREATE DICTIONARY ip_trie_radix_other_parameter (prefix String, value UInt64) PRIMARY KEY prefix
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src' WHERE 'kind = 1')) LAYOUT(IP_TRIE(OTHER_PARAMETER)) LIFETIME(0); -- { serverError BAD_ARGUMENTS }
CREATE DICTIONARY ip_trie_radix_other_layout (id UInt64, value UInt64) PRIMARY KEY id
SOURCE(CLICKHOUSE(TABLE 'ip_trie_radix_src')) LAYOUT(FLAT(RADIX_PREFIX)) LIFETIME(0); -- { serverError BAD_ARGUMENTS }

CREATE TABLE ip_trie_radix_queries_v4 (address UInt32, expected UInt64, expected_parent UInt64) ENGINE = Memory;

INSERT INTO ip_trie_radix_queries_v4
SELECT
    address,
    multiIf(
        NOT covered, 0,
        block < 240000 AND block % 1000 = 0 AND host = 1, 3000000 + block,
        block < 240000 AND block % 2 = 0, 2000000 + block,
        1000000 + intDiv(block, 256)),
    if(covered, 1000000 + intDiv(block, 256), 0)
FROM
(
    SELECT
        address,
        toInt64(address) - 16777216 AS offset,
        offset >= 0 AND offset < 65536000 AS covered,
        intDiv(offset, 256) AS block,
        offset % 256 AS host
    FROM
    (
        SELECT toUInt32(15777216 + number * 229) AS address FROM numbers(300000)
        UNION ALL SELECT toUInt32(16777216 + number * 256000 + 1) FROM numbers(240)
        UNION ALL SELECT toUInt32(16777216 + number * 256000 + 2) FROM numbers(240)
        UNION ALL SELECT arrayJoin([0, 16777215, 16777216, 82313215, 82313216, 4294967295]::Array(UInt32))
    )
);

CREATE TABLE ip_trie_radix_queries_v6 (address IPv6, expected UInt64) ENGINE = Memory;

INSERT INTO ip_trie_radix_queries_v6
SELECT
    toIPv6(format('{}:{}:{}::5', substring(hex(h), 1, 4), substring(hex(h), 5, 4), hex(third))),
    multiIf(
        on_boundary AND block < 120000 AND block % 1000 = 0 AND third = 1, 5000000 + block,
        on_boundary AND block < 120000, 4000000 + block,
        intDiv(h, 65536) = 0x2A00, 400,
        0)
FROM
(
    SELECT
        h,
        third,
        toInt64(h) - 0x2A000000 AS offset,
        offset >= 0 AND offset % 1024 = 0 AS on_boundary,
        intDiv(offset, 1024) AS block
    FROM
    (
        SELECT toUInt32(0x2A000000 - 5000 + number * 307) AS h, toUInt16(number % 3) AS third FROM numbers(300000)
        UNION ALL SELECT toUInt32(0x2A000000 + number * 1024000), toUInt16(number % 2) FROM numbers(120)
    )
);

SELECT 'Addresses';
SELECT count(), countIf(expected = 0), countIf(expected >= 1000000 AND expected < 2000000), countIf(expected >= 2000000 AND expected < 3000000), countIf(expected >= 3000000)
FROM ip_trie_radix_queries_v4;
SELECT count(), countIf(expected = 0), countIf(expected = 400), countIf(expected >= 4000000 AND expected < 5000000), countIf(expected >= 5000000)
FROM ip_trie_radix_queries_v6;

SELECT 'Only IPv4 subnets';
SELECT countIf(dictGetUInt64('ip_trie_radix_v4', 'value', toIPv4(address)) != expected) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_v4', 'value', toIPv6(toIPv4(address))) != expected) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictHas('ip_trie_radix_v4', toIPv4(address)) != (expected != 0)) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_v4', 'value', address) != 0) FROM ip_trie_radix_queries_v6;

SELECT 'Only IPv6 subnets';
SELECT countIf(dictGetUInt64('ip_trie_radix_v6', 'value', address) != expected) FROM ip_trie_radix_queries_v6;
SELECT countIf(dictHas('ip_trie_radix_v6', address) != (expected != 0)) FROM ip_trie_radix_queries_v6;
SELECT countIf(dictGetUInt64('ip_trie_radix_v6', 'value', toIPv4(address)) != 0) FROM ip_trie_radix_queries_v4;

SELECT 'IPv4 and IPv6 subnets';
SELECT countIf(dictGetUInt64('ip_trie_radix_mixed', 'value', toIPv4(address)) != if(expected = 0, 2, expected)) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_mixed', 'value', toIPv6(toIPv4(address))) != if(expected = 0, 2, expected)) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_mixed', 'value', address) != if(expected = 0, 1, expected)) FROM ip_trie_radix_queries_v6;

SELECT 'Few IPv4 subnets';
SELECT countIf(dictGetUInt64('ip_trie_radix_few_ipv4', 'value', toIPv4(address)) != if(expected_parent = 0, 2, expected_parent)) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_few_ipv4', 'value', address) != if(expected = 0, 1, expected)) FROM ip_trie_radix_queries_v6;

SELECT 'Few IPv6 subnets';
SELECT countIf(dictGetUInt64('ip_trie_radix_few_ipv6', 'value', toIPv4(address)) != if(expected = 0, 2, expected)) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_few_ipv6', 'value', address) != 1) FROM ip_trie_radix_queries_v6;

SELECT 'Few subnets';
SELECT countIf(dictGetUInt64('ip_trie_radix_small', 'value', toIPv4(address)) != expected_parent) FROM ip_trie_radix_queries_v4;

SELECT 'Around the number of subnets that gets an index';
SELECT
    address,
    dictGetUInt64('ip_trie_radix_below_threshold', 'value', toIPv4(address)),
    dictGetUInt64('ip_trie_radix_at_threshold', 'value', toIPv4(address))
FROM (SELECT arrayJoin(['99.255.255.255', '100.0.0.0', '100.0.39.14', '100.0.39.15', '100.0.39.16']) AS address);

SELECT 'Around the IPv4-mapped addresses';
SELECT
    address,
    dictGetUInt64('ip_trie_radix_mixed', 'value', toIPv6(address)),
    dictGetUInt64('ip_trie_radix_few_ipv4', 'value', toIPv6(address)),
    dictGetUInt64('ip_trie_radix_few_ipv6', 'value', toIPv6(address))
FROM (SELECT arrayJoin(['::', '::1', '::fffe:ffff:ffff', '::ffff:0:0', '::ffff:1.0.0.1', '::ffff:255.255.255.255', '::1:0:0:0', '2a00::', 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff']) AS address);

SELECT 'The parameter without a value survives a restart of the dictionary';
DETACH DICTIONARY ip_trie_radix_v4;
ATTACH DICTIONARY ip_trie_radix_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_v4', 'value', toIPv4(address)) != expected) FROM ip_trie_radix_queries_v4;
SELECT formatQuerySingleLine('CREATE DICTIONARY d (prefix String, value UInt64) PRIMARY KEY prefix SOURCE(NULL()) LAYOUT(IP_TRIE(RADIX_PREFIX)) LIFETIME(0)');
SELECT formatQuerySingleLine('CREATE DICTIONARY d (prefix String, value UInt64) PRIMARY KEY prefix SOURCE(NULL()) LAYOUT(IP_TRIE(ACCESS_TO_KEY_FROM_ATTRIBUTES 1 RADIX_PREFIX)) LIFETIME(0)');
SELECT formatQuerySingleLine('CREATE DICTIONARY d (prefix String, value UInt64) PRIMARY KEY prefix SOURCE(NULL()) LAYOUT(IP_TRIE(RADIX_PREFIX 16)) LIFETIME(0)');
SELECT formatQuerySingleLine('CREATE DICTIONARY d (prefix String, value UInt64) PRIMARY KEY prefix SOURCE(NULL()) LAYOUT(IP_TRIE) LIFETIME(0)');

SYSTEM RELOAD DICTIONARY ip_trie_radix_v4_off;
SYSTEM RELOAD DICTIONARY ip_trie_radix_v6_off;
SYSTEM RELOAD DICTIONARY ip_trie_radix_mixed_off;
SYSTEM RELOAD DICTIONARY ip_trie_radix_small_off;
SYSTEM RELOAD DICTIONARY ip_trie_radix_few_ipv4_off;
SYSTEM RELOAD DICTIONARY ip_trie_radix_few_ipv6_off;
SYSTEM RELOAD DICTIONARY ip_trie_radix_below_threshold_off;
SYSTEM RELOAD DICTIONARY ip_trie_radix_at_threshold_off;
SELECT 'Number of indexes';
-- Each dictionary is compared with the same one without the parameter. An index of the default size takes 4 * (2^16 + 1) bytes.
SELECT d.name, (d.bytes_allocated - off.bytes_allocated) / (4 * (65536 + 1))
FROM system.dictionaries AS d INNER JOIN system.dictionaries AS off ON off.database = d.database AND off.name = d.name || '_off'
WHERE d.database = currentDatabase() AND d.name NOT LIKE '%bits%' AND NOT endsWith(d.name, '_off') ORDER BY d.name;

SELECT 'Chosen size of the index';
SELECT countIf(dictGetUInt64('ip_trie_radix_v4_off', 'value', toIPv4(address)) != expected) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_v4_bits_8', 'value', toIPv4(address)) != expected) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_v4_bits_22', 'value', toIPv4(address)) != expected) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_v6_off', 'value', address) != expected) FROM ip_trie_radix_queries_v6;
SELECT countIf(dictGetUInt64('ip_trie_radix_v6_bits_8', 'value', address) != expected) FROM ip_trie_radix_queries_v6;
SELECT countIf(dictGetUInt64('ip_trie_radix_v6_bits_22', 'value', address) != expected) FROM ip_trie_radix_queries_v6;
SELECT countIf(dictGetUInt64('ip_trie_radix_mixed_off', 'value', toIPv4(address)) != if(expected = 0, 2, expected)) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_mixed_off', 'value', address) != if(expected = 0, 1, expected)) FROM ip_trie_radix_queries_v6;
SELECT countIf(dictGetUInt64('ip_trie_radix_mixed_bits_8', 'value', toIPv4(address)) != if(expected = 0, 2, expected)) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_mixed_bits_8', 'value', address) != if(expected = 0, 1, expected)) FROM ip_trie_radix_queries_v6;
SELECT countIf(dictGetUInt64('ip_trie_radix_small_off', 'value', toIPv4(address)) != expected_parent) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_small_bits_8', 'value', toIPv4(address)) != expected_parent) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_small_bits_14', 'value', toIPv4(address)) != expected_parent) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_small_bits_16', 'value', toIPv4(address)) != expected_parent) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_small_bits_18', 'value', toIPv4(address)) != expected_parent) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_small_bits_20', 'value', toIPv4(address)) != expected_parent) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_small_bits_24', 'value', toIPv4(address)) != expected_parent) FROM ip_trie_radix_queries_v4;
SELECT countIf(dictGetUInt64('ip_trie_radix_small_bits_22', 'value', toIPv4(address)) != expected_parent) FROM ip_trie_radix_queries_v4;

SELECT 'Memory of the index: 4 * (2^RADIX_PREFIX + 1) bytes for each one';
SELECT name, bytes_allocated - (SELECT bytes_allocated FROM system.dictionaries WHERE database = currentDatabase() AND name = 'ip_trie_radix_v4_off')
FROM system.dictionaries WHERE database = currentDatabase() AND name IN ('ip_trie_radix_v4', 'ip_trie_radix_v4_bits_8', 'ip_trie_radix_v4_bits_22') ORDER BY name;
SELECT name, bytes_allocated - (SELECT bytes_allocated FROM system.dictionaries WHERE database = currentDatabase() AND name = 'ip_trie_radix_v6_off')
FROM system.dictionaries WHERE database = currentDatabase() AND name IN ('ip_trie_radix_v6', 'ip_trie_radix_v6_bits_8', 'ip_trie_radix_v6_bits_22') ORDER BY name;
SELECT name, bytes_allocated - (SELECT bytes_allocated FROM system.dictionaries WHERE database = currentDatabase() AND name = 'ip_trie_radix_mixed_off')
FROM system.dictionaries WHERE database = currentDatabase() AND name IN ('ip_trie_radix_mixed_bits_8') ORDER BY name;
SELECT name, bytes_allocated - (SELECT bytes_allocated FROM system.dictionaries WHERE database = currentDatabase() AND name = 'ip_trie_radix_small_off')
FROM system.dictionaries WHERE database = currentDatabase() AND name IN ('ip_trie_radix_small_bits_8', 'ip_trie_radix_small_bits_14', 'ip_trie_radix_small_bits_16', 'ip_trie_radix_small_bits_18', 'ip_trie_radix_small_bits_20', 'ip_trie_radix_small_bits_22', 'ip_trie_radix_small_bits_24') ORDER BY name;

DROP DICTIONARY ip_trie_radix_v4;
DROP DICTIONARY ip_trie_radix_v6;
DROP DICTIONARY ip_trie_radix_mixed;
DROP DICTIONARY ip_trie_radix_few_ipv4;
DROP DICTIONARY ip_trie_radix_few_ipv6;
DROP DICTIONARY ip_trie_radix_small;
DROP DICTIONARY ip_trie_radix_below_threshold;
DROP DICTIONARY ip_trie_radix_at_threshold;
DROP DICTIONARY ip_trie_radix_few_ipv4_off;
DROP DICTIONARY ip_trie_radix_few_ipv6_off;
DROP DICTIONARY ip_trie_radix_below_threshold_off;
DROP DICTIONARY ip_trie_radix_at_threshold_off;
DROP DICTIONARY ip_trie_radix_v4_off;
DROP DICTIONARY ip_trie_radix_v4_bits_8;
DROP DICTIONARY ip_trie_radix_v4_bits_22;
DROP DICTIONARY ip_trie_radix_v6_off;
DROP DICTIONARY ip_trie_radix_v6_bits_8;
DROP DICTIONARY ip_trie_radix_v6_bits_22;
DROP DICTIONARY ip_trie_radix_mixed_off;
DROP DICTIONARY ip_trie_radix_mixed_bits_8;
DROP DICTIONARY ip_trie_radix_small_off;
DROP DICTIONARY ip_trie_radix_small_bits_8;
DROP DICTIONARY ip_trie_radix_small_bits_22;
DROP DICTIONARY ip_trie_radix_small_bits_14;
DROP DICTIONARY ip_trie_radix_small_bits_16;
DROP DICTIONARY ip_trie_radix_small_bits_18;
DROP DICTIONARY ip_trie_radix_small_bits_20;
DROP DICTIONARY ip_trie_radix_small_bits_24;
DROP TABLE ip_trie_radix_src;
DROP TABLE ip_trie_radix_queries_v4;
DROP TABLE ip_trie_radix_queries_v6;
