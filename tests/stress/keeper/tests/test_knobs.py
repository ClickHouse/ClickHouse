"""Helper-level tests for the workload.clients / workload.concurrency knobs.

These run without containers or keeper binaries: plain pytest with
PYTHONPATH=".:tests/stress:ci".  They take no `scenario` fixture, so the
scenario loader does not parametrize them and CI collection of this
directory just runs them as fast unit tests.
"""
import pytest

from keeper.framework.core.settings import DEFAULT_WORKLOAD_CONFIG
from keeper.tests.test_scenarios import _build_bench_step
from keeper.workloads.keeper_bench import KeeperBench


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
