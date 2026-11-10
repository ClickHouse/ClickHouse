-- isIPAddressInRange with a NULL literal (Nullable(Nothing)) must return 0 like a typed NULL.
-- https://github.com/ClickHouse/ClickHouse/issues/65911

-- Typed NULL (already returns 0).
SELECT isIPAddressInRange(CAST(NULL AS Nullable(String)), '192.168.44.0/24'), toTypeName(isIPAddressInRange(CAST(NULL AS Nullable(String)), '192.168.44.0/24'));
SELECT isIPAddressInRange(materialize(CAST(NULL AS Nullable(String))), '192.168.44.0/24'), toTypeName(isIPAddressInRange(materialize(CAST(NULL AS Nullable(String))), '192.168.44.0/24'));
SELECT isIPAddressInRange(CAST(NULL AS Nullable(IPv4)), '192.168.44.0/24'), isIPAddressInRange(materialize(CAST(NULL AS Nullable(IPv6))), '::/0');
SELECT isIPAddressInRange(CAST(NULL AS Nullable(String)), 'abc'); -- { serverError CANNOT_PARSE_TEXT }

-- NULL literal.
SELECT isIPAddressInRange(NULL, '192.168.44.0/24'), toTypeName(isIPAddressInRange(NULL, '192.168.44.0/24'));
SELECT isIPAddressInRange(NULL, materialize('192.168.44.0/24'));
SELECT isIPAddressInRange(materialize(NULL), '192.168.44.0/24'), toTypeName(isIPAddressInRange(materialize(NULL), '192.168.44.0/24'));
SELECT isIPAddressInRange(materialize(NULL), materialize('192.168.44.0/24'));
SELECT x, isIPAddressInRange(x, '192.168.44.0/24') FROM (SELECT arrayJoin([NULL, NULL]) AS x);
SELECT isIPAddressInRange(NULL, 'abc'); -- { serverError CANNOT_PARSE_TEXT }
SELECT isIPAddressInRange(materialize(NULL), 'abc'); -- { serverError CANNOT_PARSE_TEXT }
