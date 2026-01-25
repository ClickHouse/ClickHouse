import os
import pathlib
import time

import pytest
from keeper.framework.core.cluster import ClusterBuilder
from keeper.framework.core.settings import DEFAULT_READY_TIMEOUT
from keeper.framework.core.util import env_int, env_bool, parse_bool, wait_until
from keeper.framework.io.probes import count_leaders, four, mntr
from keeper.framework.core.settings import keeper_node_names

pytest_plugins = ["keeper.pytest_plugins.scenario_loader"]


def pytest_configure(config):
    os.environ.setdefault(
        "KEEPER_METRICS_FILE",
        f"/tmp/keeper_metrics_{int(time.time())}_{os.getpid()}.jsonl",
    )

def pytest_addoption(parser):
    pa = parser.addoption
    pa("--commit-sha", action="store", default=os.environ.get("COMMIT_SHA", "local"))
    # Sink URL is resolved via CI ClickHouse helper; no explicit option needed
    pa("--duration", type=int, default=None)
    pa(
        "--matrix-backends",
        action="store",
        default=os.environ.get("KEEPER_MATRIX_BACKENDS", "default"),
    )
    pa(
        "--matrix-topologies",
        action="store",
        default=os.environ.get("KEEPER_MATRIX_TOPOLOGIES", 3),
    )
    pa(
        "--seed",
        type=int,
        default=env_int("KEEPER_SEED", int.from_bytes(os.urandom(4), "big")),
    )
    pa(
        "--keep-containers-on-fail",
        action="store_true",
        default=env_bool("KEEPER_KEEP_ON_FAIL", False),
    )
    pa(
        "--faults",
        type=parse_bool,
        default=env_bool("KEEPER_FAULTS", True),
        help="Enable fault injection (true/false/1/0, default from KEEPER_FAULTS env var)",
    )
    pa(
        "--keeper-include-ids",
        action="store",
        default=os.environ.get("KEEPER_INCLUDE_IDS", ""),
    )
    pa(
        "--replay",
        action="store",
        default=os.environ.get("KEEPER_REPLAY_PATH", ""),
        help="Path to request log for keeper-bench replay; falls back to KEEPER_REPLAY_PATH",
    )


@pytest.fixture(scope="session")
def run_meta(request):
    return {
        "commit_sha": request.config.getoption("--commit-sha"),
    }


def _merge_env_opts(opts):
    """Merge environment-provided feature flags into opts."""
    opts = dict(opts or {})
    # Feature flags from env
    ff_env = (os.environ.get("KEEPER_FEATURE_FLAGS") or "").strip()
    if ff_env:
        flags = {}
        for part in ff_env.split(","):
            if not part.strip():
                continue
            if "=" in part:
                k, v = part.split("=", 1)
            else:
                raise ValueError(f"Invalid feature flag: {part}")
            flags[k.strip()] = 1 if v.strip() == "1" else 0
        opts["feature_flags"] = dict(opts.get("feature_flags", {})) | flags
    return opts


def _print_failure_diagnostics(topology, builder):
    """Print diagnostics from instance directory on failure.
    
    Args:
        topology: Number of nodes in the cluster
        builder: ClusterBuilder instance to get instances_dir from
    """
    try:
        if not builder or not builder.cluster:
            raise ValueError("ClusterBuilder instance is required")
        inst_dir = builder.cluster.instances_dir

        for name in keeper_node_names(topology):
            # Print config
            cfg = builder.conf_dir / f"keeper_config_{name}.xml"
            if cfg.exists():
                print(f"==== {name} config ====\n{cfg.read_text()[:800]}")

            # Print logs
            for log_type, suffix in [("err", "clickhouse-server.err.log"), ("log", "clickhouse-server.log")]:
                log_path = inst_dir / name / "logs" / suffix
                if log_path.exists():
                    try:
                        txt = log_path.read_text()
                        print(f"==== {name} {log_type} ====\n" + "\n".join(txt.splitlines()[-200:]))
                    except Exception:
                        pass
    except Exception:
        pass


def _print_ready_failure_diagnostics(nodes):
    """Print diagnostics when cluster ready check fails."""
    for n in nodes:
        # Run probes: stat, srvr, mntr
        for probe_name, probe_fn in (("stat", four), ("srvr", four)):
            try:
                result = probe_fn(n, probe_name)
                print(f"==== {n.name} {probe_name} ====\n{str(result)[:2000]}")
            except Exception:
                continue
        try:
            m = mntr(n) or {}
            print(f"==== {n.name} mntr zk_server_state ====\n{m.get('zk_server_state', '')}")
        except Exception:
            continue


@pytest.fixture(scope="function")
def cluster_factory(request):
    def _make(cname, topology, backend, opts):
        builder = ClusterBuilder(cname, __file__)
        opts = _merge_env_opts(opts)
        try:
            cluster, nodes = builder.build(topology=topology, backend=backend, opts=opts)
        except Exception as e:
            _print_failure_diagnostics(topology, builder)
            raise e
        # Wait for cluster ready
        timeout = float(env_int("KEEPER_READY_TIMEOUT", DEFAULT_READY_TIMEOUT))
        try:
            wait_until(lambda: count_leaders(nodes) == 1, timeout_s=timeout, interval=0.5, desc="cluster ready")
        except Exception as e:
            _print_ready_failure_diagnostics(nodes)
            raise e
        return cluster, nodes
    return _make


def pytest_collection_modifyitems(config, items):
    return
