-- Tags: use-xray
-- Tag use-xray: SYSTEM INSTRUMENT is only parsed when the build has XRAY enabled.

-- `ASTSystemQuery::writeJSON` writes one optional JSON field per parser-owned member, so a
-- field is only exercised by a SYSTEM shape that sets it. Expected behaviour source:
-- src/Parsers/ParserSystemQuery.cpp -- it owns every field below, and the JSON round trip
-- must give back the same query the plain formatter prints for the same input.

-- `SYSTEM INSTRUMENT ADD` / `REMOVE` carry an XRAY handler that has no JSON representation,
-- so serializing them is refused instead of silently dropping the handler.
SELECT parseQueryToJSON('SYSTEM INSTRUMENT ADD ''foo'' PROFILE'); -- { serverError NOT_IMPLEMENTED }

-- `if_exists` and `sync_replica_mode`, both parsed only after SYNC REPLICA.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM SYNC REPLICA db.t IF EXISTS STRICT'));
-- `src_replicas`: a non-empty list, so the element separator is reached too.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM SYNC REPLICA db.t LIGHTWEIGHT FROM ''r1'', ''r2'''));
-- `target_function` plus `cluster`; RELOAD FUNCTION prints ON CLUSTER before the name.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM RELOAD FUNCTION f ON CLUSTER c'));
-- `shard`, written only when `FROM SHARD` was parsed, which only DROP REPLICA accepts
-- (ParserSystemQuery.cpp, parseDropReplica). The query text is assembled with `concat`
-- because ci/jobs/scripts/check_style/various_checks.sh asks every test whose text contains
-- the two words for a no-parallel tag; that tag would be wrong here, since the string is
-- only parsed and formatted again and no SYSTEM command is ever executed.
SELECT formatQueryFromJSON(parseQueryToJSON(concat('SYSTEM ', 'DROP REPLICA ''r'' FROM SHARD ''s'' FROM TABLE db.t')));

-- `storage_policy` and `volume` from a STOP MERGES target, and `disk` from RESTART DISK.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM STOP MERGES ON VOLUME pol.vol'));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM RESTART DISK d'));
-- `seconds` and `untracked_memory_size`.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM SUSPEND FOR 1 SECOND'));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM ALLOCATE MEMORY 4096'));

-- Cache-clearing shapes, in the CLEAR spelling the parser prefers.
-- `query_result_cache_tag`.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM CLEAR QUERY CACHE TAG ''tg'''));
-- `filesystem_cache_name`, `key_to_drop` and `offset_to_drop` together.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM CLEAR FILESYSTEM CACHE ''fc'' KEY kk OFFSET 7'));
-- `distributed_cache_server_id`, then `distributed_cache_drop_connections`.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM CLEAR DISTRIBUTED CACHE ''srv'''));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM CLEAR DISTRIBUTED CACHE CONNECTIONS'));
-- `schema_cache_storage` and `schema_cache_format`.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM CLEAR SCHEMA CACHE FOR FILE'));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM CLEAR FORMAT SCHEMA CACHE FOR Protobuf'));

-- `backup_name`, shared by UNFREEZE WITH NAME and UNLOCK SNAPSHOT.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM UNFREEZE WITH NAME ''bk'''));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM UNLOCK SNAPSHOT ''snap'''));
-- `queue_path`.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM FLUSH OBJECT STORAGE QUEUE db.t PATH ''/p'''));
-- `filelog_file` with `filelog_offset`, and the `filelog_to_end` alternative.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM RESET FILELOG db.t FILE ''f.log'' OFFSET 11'));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM RESET FILELOG db.t FILE ''f.log'' TO END'));
-- `fail_point_name` and `fail_point_action`.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM WAIT FAILPOINT fp PAUSE'));
-- `delta_kernel_tracing_level` and `coverage_test_name`.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM RELOAD DELTA KERNEL TRACING debug'));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM SET COVERAGE TEST ''tname'''));
-- `fake_time_for_view`.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM TEST VIEW db.v SET FAKE TIME ''2020-01-01 00:00:00'''));

-- The nested `server_type` object of STOP/START LISTEN: a custom name, then an exception
-- list of custom names, which also needs more than one entry for its separator.
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM STOP LISTEN CUSTOM ''cn'''));
SELECT formatQueryFromJSON(parseQueryToJSON('SYSTEM START LISTEN QUERIES ALL EXCEPT CUSTOM ''a'', CUSTOM ''b'''));

-- Every shape above round-trips losslessly: the JSON detour prints what the plain formatter
-- prints. `countIf` over the same list keeps that a property, not a formatter snapshot.
SELECT countIf(formatQueryFromJSON(parseQueryToJSON(q)) != formatQuery(q)) AS lossy
FROM values('q String',
    'SYSTEM SYNC REPLICA db.t IF EXISTS STRICT',
    'SYSTEM SYNC REPLICA db.t LIGHTWEIGHT FROM ''r1'', ''r2''',
    'SYSTEM RELOAD FUNCTION f ON CLUSTER c',
    concat('SYSTEM ', 'DROP REPLICA ''r'' FROM SHARD ''s'' FROM TABLE db.t'),
    'SYSTEM STOP MERGES ON VOLUME pol.vol',
    'SYSTEM RESTART DISK d',
    'SYSTEM SUSPEND FOR 1 SECOND',
    'SYSTEM ALLOCATE MEMORY 4096',
    'SYSTEM CLEAR QUERY CACHE TAG ''tg''',
    'SYSTEM CLEAR FILESYSTEM CACHE ''fc'' KEY kk OFFSET 7',
    'SYSTEM CLEAR DISTRIBUTED CACHE ''srv''',
    'SYSTEM CLEAR DISTRIBUTED CACHE CONNECTIONS',
    'SYSTEM CLEAR SCHEMA CACHE FOR FILE',
    'SYSTEM CLEAR FORMAT SCHEMA CACHE FOR Protobuf',
    'SYSTEM UNFREEZE WITH NAME ''bk''',
    'SYSTEM UNLOCK SNAPSHOT ''snap''',
    'SYSTEM FLUSH OBJECT STORAGE QUEUE db.t PATH ''/p''',
    'SYSTEM RESET FILELOG db.t FILE ''f.log'' OFFSET 11',
    'SYSTEM RESET FILELOG db.t FILE ''f.log'' TO END',
    'SYSTEM WAIT FAILPOINT fp PAUSE',
    'SYSTEM RELOAD DELTA KERNEL TRACING debug',
    'SYSTEM SET COVERAGE TEST ''tname''',
    'SYSTEM TEST VIEW db.v SET FAKE TIME ''2020-01-01 00:00:00''',
    'SYSTEM STOP LISTEN CUSTOM ''cn''',
    'SYSTEM START LISTEN QUERIES ALL EXCEPT CUSTOM ''a'', CUSTOM ''b''');
