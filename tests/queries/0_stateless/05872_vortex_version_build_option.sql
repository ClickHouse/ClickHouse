-- Tags: no-fasttest, no-msan

SELECT value FROM system.build_options WHERE name = 'USE_VORTEX';
SELECT match(value, '^\\d+\\.\\d+\\.\\d+$') FROM system.build_options WHERE name = 'VORTEX_VERSION';
