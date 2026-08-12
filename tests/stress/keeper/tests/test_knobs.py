"""Helper-level tests for the workload.clients / workload.concurrency knobs.

These run without containers or keeper binaries: plain pytest with
PYTHONPATH=".:tests/stress:ci".  They take no `scenario` fixture, so the
scenario loader does not parametrize them and CI collection of this
directory just runs them as fast unit tests.
"""
import threading
import time

import pytest

from keeper.framework.core.settings import DEFAULT_WORKLOAD_CONFIG
from keeper.tests.test_scenarios import _build_bench_step
from keeper.workloads.keeper_bench import BENCH_SETUP_DONE_MARKER, KeeperBench


class _StubNode:
    def __init__(self, is_zookeeper=False):
        self.name = "stub1"
        self.ip_address = "127.0.0.1"
        self.is_zookeeper = is_zookeeper


def test_knobs_only_scenario_keeps_default_workload(monkeypatch):
    monkeypatch.delenv("KEEPER_WORKLOAD_CONFIG", raising=False)
    ctx = {}
    scenario = {"workload": {"clients": 7, "concurrency": 3}}
    _build_bench_step(scenario, [_StubNode()], ctx)
    wl = ctx["workload"]
    assert wl["config"].endswith(DEFAULT_WORKLOAD_CONFIG)
    assert not wl.get("replay")
    assert wl["clients"] == 7
    assert wl["concurrency"] == 3


def test_keeper_bench_rejects_clients_on_zookeeper():
    with pytest.raises(ValueError, match="not supported"):
        KeeperBench(
            nodes=[_StubNode(is_zookeeper=True)], ctx={}, cfg_path="wl.yaml",
            duration_s=30, replay_path=None, clients=5,
        )


def test_keeper_bench_rejects_concurrency_on_replay():
    with pytest.raises(ValueError, match="not supported"):
        KeeperBench(
            nodes=[_StubNode()], ctx={}, cfg_path="wl.yaml",
            duration_s=30, replay_path="/tmp/req.log", concurrency=3,
        )


def _run_expecting_value_error(monkeypatch, tmp_path, node, replay_path, env_val, match):
    """Call run() with the network wait stubbed out; the guard must raise before
    any bench subprocess is spawned."""
    cfg = tmp_path / "wl.yaml"
    cfg.write_text("concurrency: 2\n")
    kb = KeeperBench(
        nodes=[node], ctx={}, cfg_path=str(cfg), duration_s=30, replay_path=replay_path,
    )
    monkeypatch.setattr(kb, "_wait_for_any_server", lambda *a, **k: True)

    def _no_subprocess(*a, **k):
        raise AssertionError("bench subprocess must not be spawned")

    monkeypatch.setattr(kb, "_run_bench_subprocess", _no_subprocess)
    monkeypatch.setenv("KEEPER_BENCH_CLIENTS", env_val)
    with pytest.raises(ValueError, match=match):
        kb.run()


def test_env_clients_rejects_non_positive(monkeypatch, tmp_path):
    _run_expecting_value_error(
        monkeypatch, tmp_path, _StubNode(), None, "0", match="must be > 0"
    )


def test_env_clients_rejected_on_zookeeper(monkeypatch, tmp_path):
    _run_expecting_value_error(
        monkeypatch, tmp_path, _StubNode(is_zookeeper=True), None, "8",
        match="not supported",
    )


def test_env_clients_rejected_on_replay(monkeypatch, tmp_path):
    _run_expecting_value_error(
        monkeypatch, tmp_path, _StubNode(), "/tmp/req.log", "8",
        match="not supported",
    )


def _sharded_bench(tmp_path):
    cfg = tmp_path / "wl.yaml"
    cfg.write_text("concurrency: 2\n")
    return KeeperBench(
        nodes=[_StubNode()], ctx={}, cfg_path=str(cfg), duration_s=30, replay_path=None,
    )


def test_run_sharded_aborts_when_shard0_exits_without_marker(monkeypatch, tmp_path):
    """9000 clients -> 3 shards; shard 0 exits without printing the setup-done
    marker, so shards 1..2 must never be started and shard 0's thread must be
    cleaned up before the abort propagates (no leaked background bench)."""
    kb = _sharded_bench(tmp_path)
    calls = []

    def _stub(bench_cfg, cfg_path):
        calls.append(bench_cfg)
        time.sleep(4)  # keep shard 0 alive so the abort path exercises the join
        return "", None, None

    monkeypatch.setattr(kb, "_run_bench_subprocess", _stub)
    with pytest.raises(AssertionError, match="setup-done marker"):
        kb._run_sharded({"concurrency": 2}, 9000, 90)
    assert len(calls) == 1
    assert not [t for t in threading.enumerate() if t.name.startswith("bench-shard-")]


def test_run_sharded_launches_all_shards_after_marker(monkeypatch, tmp_path):
    """When shard 0 prints the marker (even exiting immediately after), all
    shards launch and the per-shard summaries are merged."""
    kb = _sharded_bench(tmp_path)
    calls = []

    def _stub(bench_cfg, cfg_path):
        calls.append(bench_cfg)
        stderr = tmp_path / f"stderr_{len(calls)}.log"
        stderr.write_text(BENCH_SETUP_DONE_MARKER)
        kb.bench_error_path = str(stderr)
        return '{"ops": 10, "errors": 0}', None, str(stderr)

    monkeypatch.setattr(kb, "_run_bench_subprocess", _stub)
    merged = kb._run_sharded({"concurrency": 2}, 9000, 90)
    assert len(calls) == 3
    assert merged["shards"] == 3
    assert merged["ops"] == 30
