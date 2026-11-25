-- Bounds on the work of resolving symbol names from the compact symbol table (the `.clickhouse.symbols` section
-- of release builds). A build that keeps `.symtab` decodes nothing: its counters stay zero and the bounds hold.

SET allow_introspection_functions = 1;

-- Without these counters the bounds below would hold for any build.
SELECT count() FROM system.events WHERE event IN ('SymbolNameCacheMisses', 'SymbolNameDecompressedBytes') SETTINGS system_events_show_zero_values = 1;

-- Another set of addresses in every run of the test, so that their names are not in the cache yet.
CREATE TEMPORARY TABLE symbol_addresses (address UInt64);
INSERT INTO symbol_addresses SELECT address_begin FROM system.symbols ORDER BY sipHash64(address_begin, currentDatabase()), address_begin LIMIT 1000;

-- 200 lookups of each address on average, in random order.
WITH (SELECT groupArray(address) FROM symbol_addresses) AS addresses
SELECT count() FROM numbers(200000) WHERE NOT ignore(addressToSymbol(addresses[1 + cityHash64(number) % 1000]))
SETTINGS max_threads = 1, log_comment = '05875_symbol_name_lookup_cost_repeated';

SYSTEM FLUSH LOGS query_log;

-- Repeated lookups come from the cache of decoded names: less than half of them decode a name.
-- Decoding a name decompresses a small granule: less than 64 KiB on average.
SELECT
    ProfileEvents['SymbolNameCacheMisses'] < 100000,
    ProfileEvents['SymbolNameDecompressedBytes'] <= ProfileEvents['SymbolNameCacheMisses'] * 65536
FROM system.query_log
WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment = '05875_symbol_name_lookup_cost_repeated';
