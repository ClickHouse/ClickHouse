#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

set -euo pipefail

python3 - <<'PY'
import json
import os
import shlex
import subprocess
import uuid

client = shlex.split(os.environ["CLICKHOUSE_CLIENT"])
prefix = "fp_execution_" + uuid.uuid4().hex
common = dict(
    enable_analyzer=1, enable_parallel_replicas=0, join_algorithm="hash", hash_join_layout="serial",
    max_threads=1, max_block_size=65536, preferred_block_size_bytes=0,
    query_plan_join_shard_by_pk_ranges=0, query_plan_read_in_order_through_join=0,
    query_plan_join_swap_table=0, query_plan_optimize_join_order_limit=1, query_plan_convert_join_to_in=0,
    enable_join_key_only_hash_tables=1, enable_join_runtime_filters=0, enable_join_fixed_hash_table_conversion=0,
    enable_hash_join_row_store=0, collect_hash_table_stats_during_joins=0, any_join_distinct_right_table_keys=0,
    max_bytes_before_external_join=0, max_bytes_ratio_before_external_join=0,
    join_use_nulls=0, aggregate_functions_null_for_empty=0, use_query_cache=0,
    min_insert_block_size_rows=0, min_insert_block_size_bytes=0, max_insert_block_size=65536,
    log_queries=1, log_profile_events=1, log_queries_probability=1, log_queries_min_query_duration_ms=0,
    log_queries_min_type="QUERY_FINISH", experimental_join_fingerprint_mode=0,
)
tables, views = [], []


def query(sql, mode=0, query_id=None):
    settings = {**common, "experimental_join_fingerprint_mode": mode}
    args = client + [f"--{key}={value}" for key, value in settings.items()]
    if query_id is not None:
        args += ["--query_id", query_id]
    return subprocess.check_output(args + ["--format=TabSeparatedRaw", "--query", sql], text=True).strip()


def table(suffix, columns):
    name = prefix + "_" + suffix
    query(f"CREATE TABLE {name} ({columns}) ENGINE=Memory")
    tables.append(name)
    return name


def check(label, sql, expected, mode):
    query_id = prefix + "_" + label
    actual = tuple(map(int, query(sql, mode, query_id).split("\t")))
    assert actual == expected, (label, actual, expected)
    query("SYSTEM FLUSH LOGS query_log")
    rows = query(f"SELECT toJSONString(ProfileEvents) FROM system.query_log "
                 f"WHERE current_database = currentDatabase() AND query_id='{query_id}' AND type='QueryFinish'").splitlines()
    assert len(rows) == 1, (label, rows)
    return {key: int(value) for key, value in json.loads(rows[0]).items()}


def fingerprints(events, rows, simd):
    for key, expected in {"Tables": 1, "SIMDTables": simd, "Rows": rows}.items():
        assert events.get("HashJoinFingerprint" + key, 0) == expected, (key, events)
    assert events.get("HashJoinFingerprintBytes", 0) > 0, events


try:
    build = table("build", "k UInt64, v UInt64")
    probe = table("probe", "id UInt64, k UInt64")
    outer = table("outer", "k UInt64, w UInt64")
    maximum = 2**64 - 1
    right = [(0, 0), (1, 7), (1, 11), (maximum, maximum), (42, 13)]
    left = [(1, 0), (2, 1), (3, maximum), (4, 1), (5, 99)]
    query(f"INSERT INTO {build} VALUES " + ",".join(str(row) for row in right))
    query(f"INSERT INTO {probe} VALUES " + ",".join(str(row) for row in left))
    query(f"INSERT INTO {outer} VALUES (0,3),(1,5)")
    matches = [(i, v) for i, k in left for key, v in right if k == key]
    all_expected = (len(matches), sum(i for i, _ in matches), sum(v for _, v in matches))
    present = [i for i, k in left if any(key == k for key, _ in right)]
    for mode in (1, 2):
        events = check(f"forced_all_{mode}", f"SELECT count(),sum(p.id),sum(toUInt128(b.v)) "
                       f"FROM {probe} p ALL INNER JOIN {build} b USING(k)", all_expected, mode)
        fingerprints(events, 4, int(mode == 2))
        events = check(f"forced_semi_{mode}", f"SELECT count(),sum(p.id) "
                       f"FROM {probe} p LEFT SEMI JOIN (SELECT k FROM {build}) b USING(k)",
                       (len(present), sum(present)), mode)
        fingerprints(events, 4, int(mode == 2))
        print(f"forced {mode}: ALL and SEMI")

    # The aggregation keeps the inner and outer joins in separate query scopes.
    # Different build cardinalities identify the enabled join through `HashJoinFingerprintRows`.
    for outer_mode, inner_mode in ((0, 2), (2, 0)):
        inner = (f"SELECT p.id,p.k,sum(toUInt128(b.v)) AS v FROM {probe} p "
                 f"ALL INNER JOIN {build} b USING(k) GROUP BY p.id,p.k "
                 f"SETTINGS experimental_join_fingerprint_mode={inner_mode}")
        view = prefix + f"_view_{inner_mode}"
        query(f"CREATE VIEW {view} AS {inner}")
        views.append(view)
        sources = {"subquery": ("", f"({inner})"), "cte": (f"WITH t AS ({inner}) ", "t"), "view": ("", view)}
        for form, (with_clause, source) in sources.items():
            events = check(f"scope_{form}_{outer_mode}_{inner_mode}",
                           f"{with_clause}SELECT count(),sum(t.id),sum(t.v),sum(o.w) "
                           f"FROM {source} t ALL INNER JOIN {outer} o USING(k)", (3, 7, 36, 13), outer_mode)
            fingerprints(events, 4 if inner_mode == 2 else 2, 1)
            assert events.get("HashJoinAutoFingerprintSelected", 0) == 0, events
        print(f"scoped outer {outer_mode}, inner {inner_mode}: subquery, CTE and view")

    processor = query("SELECT value FROM system.build_options WHERE name='SYSTEM_PROCESSOR'").lower()
    assert processor, "Missing server architecture"
    if processor.startswith(("aarch64", "arm64")):
        large_build = table("large_build", "k UInt64, v UInt64")
        large_probe = table("large_probe", "id UInt64")
        build_rows, probe_rows, first_block = 131072, 4194304, 65536
        multiplier = 11400714819323198485
        query(f"INSERT INTO {large_build} SELECT number*toUInt64({multiplier}),toUInt64(7) FROM numbers({build_rows})")
        query(f"INSERT INTO {large_probe} SELECT number FROM numbers({probe_rows})")
        miss = f"(id+toUInt64({build_rows}))*toUInt64({multiplier})"
        for phase in (False, True):
            key = f"if(id<{first_block},{miss},id%{build_rows}*toUInt64({multiplier}))" if phase else miss
            # A single reader and explicit ordering retain a full miss block before the successful suffix.
            source = f"(SELECT id,{key} AS k FROM {large_probe} ORDER BY id)"
            count = probe_rows - first_block if phase else 0
            id_sum = (probe_rows*(probe_rows-1) - first_block*(first_block-1))//2 if phase else 0
            sql = f"SELECT count(),sum(p.id),sum(b.v) FROM {source} p ALL INNER JOIN {large_build} b USING(k)"
            for mode in (0, 4):
                events = check(f"auto_{int(phase)}_{mode}", sql, (count, id_sum, 7*count), mode)
                if mode == 0:
                    assert events.get("HashJoinFingerprintTables", 0) == 0, events
                    continue
                fingerprints(events, build_rows, 1)
                for key, expected in {"Candidates": 1, "Selected": 1, "Rejected": 0, "Disabled": int(phase),
                                      "InitialSampleRows": 4096, "InitialSampleHits": 0,
                                      "InitialDistinctKeys": 4096}.items():
                    assert events.get("HashJoinAutoFingerprint" + key, 0) == expected, (key, events)
                assert events.get("HashJoinAutoFingerprintProbeEstimate", 0) >= probe_rows, events
                assert (events.get("HashJoinAutoFingerprintSampleHits", 0) > 0) == phase, events
    else:
        events = check("auto_uncalibrated", f"SELECT count(),sum(p.id),sum(toUInt128(b.v)) "
                       f"FROM {probe} p ALL INNER JOIN {build} b USING(k)", all_expected, 4)
        assert events.get("HashJoinAutoFingerprintCandidates", 0) == 1, events
        assert events.get("HashJoinAutoFingerprintRejected", 0) == 1, events
        assert events.get("HashJoinFingerprintTables", 0) == 0, events
    # Only these automatic activation assertions are architecture-specific; forced indexes run everywhere.
    print("automatic architecture-specific assertions passed")
finally:
    for name in reversed(views):
        query(f"DROP VIEW IF EXISTS {name}")
    for name in reversed(tables):
        query(f"DROP TABLE IF EXISTS {name}")
PY
