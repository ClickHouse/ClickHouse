import os
import pathlib

import pytest
from keeper.framework.core.cluster import ClusterBuilder
from keeper.framework.core.settings import parse_bool
from keeper.framework.core.util import env_int, wait_until
from keeper.framework.io.probes import count_leaders

pytest_plugins = ["keeper.pytest_plugins.scenario_loader"]


def pytest_addoption(parser):
    pa = parser.addoption
    pa(
        "--keeper-backend",
        action="store",
        default=os.environ.get("KEEPER_BACKEND", "default"),
    )
    pa("--commit-sha", action="store", default=os.environ.get("COMMIT_SHA", "local"))
    # Sink URL is resolved via CI ClickHouse helper; no explicit option needed
    pa("--duration", type=int, default=int(env_int("KEEPER_DURATION", 120)))
    pa(
        "--total-shards",
        type=int,
        default=int(os.environ.get("KEEPER_TOTAL_SHARDS", "1") or "1"),
    )
    pa(
        "--shard-index",
        type=int,
        default=int(os.environ.get("KEEPER_SHARD_INDEX", "0") or "0"),
    )
    pa(
        "--matrix-backends",
        action="store",
        default=os.environ.get("KEEPER_MATRIX_BACKENDS", ""),
    )
    pa(
        "--matrix-topologies",
        action="store",
        default=os.environ.get("KEEPER_MATRIX_TOPOLOGIES", ""),
    )
    pa("--seed", type=int, default=int(os.environ.get("KEEPER_SEED", "0")))
    pa(
        "--keep-containers-on-fail",
        action="store_true",
        default=parse_bool(os.environ.get("KEEPER_KEEP_ON_FAIL")),
    )
    pa(
        "--faults",
        choices=("on", "off", "random"),
        default=os.environ.get("KEEPER_FAULTS", "on"),
    )
    pa(
        "--random-faults-count",
        type=int,
        default=int(os.environ.get("KEEPER_RANDOM_FAULTS_COUNT", "1")),
    )
    pa(
        "--random-faults-include",
        action="store",
        default=os.environ.get("KEEPER_RANDOM_FAULTS_INCLUDE", ""),
    )
    pa(
        "--random-faults-exclude",
        action="store",
        default=os.environ.get("KEEPER_RANDOM_FAULTS_EXCLUDE", ""),
    )
    pa(
        "--keeper-include-ids",
        action="store",
        default=os.environ.get("KEEPER_INCLUDE_IDS", ""),
    )
    pa(
        "--seed-list",
        action="store",
        default=os.environ.get("KEEPER_SEEDS", ""),
    )


@pytest.fixture(scope="session")
def run_meta(request):
    return {
        "commit_sha": request.config.getoption("--commit-sha"),
        "backend": request.config.getoption("--keeper-backend"),
    }


@pytest.fixture(scope="function")
def cluster_factory(request):
    def _make(topology, backend, opts):
        anchor = __file__  # stable anchor in tests dir
        builder = ClusterBuilder(anchor)
        # Merge environment-provided feature flags / coordination overrides into scenario opts
        try:
            ff_env = os.environ.get("KEEPER_FEATURE_FLAGS", "").strip()
            if ff_env:
                flags = {}
                for part in ff_env.split(","):
                    if not part.strip():
                        continue
                    if "=" in part:
                        k, v = part.split("=", 1)
                    else:
                        k, v = part, "1"
                    k = k.strip()
                    v = v.strip()
                    flags[k] = 1 if v == "1" else 0
                base_opts = dict(opts or {})
                cur_ff = dict((base_opts.get("feature_flags") or {}))
                cur_ff.update(flags)
                base_opts["feature_flags"] = cur_ff
                opts = base_opts
        except Exception:
            pass
        try:
            coord_xml = os.environ.get("KEEPER_COORD_OVERRIDES_XML", "")
            if coord_xml:
                base_opts = dict(opts or {})
                base_opts["coord_overrides_xml"] = coord_xml
                opts = base_opts
        except Exception:
            pass
        try:
            cluster, nodes = builder.build(
                topology=topology, backend=backend, opts=opts
            )
        except Exception as e:
            try:
                base = pathlib.Path(__file__).parent
                inst_dirs = sorted(
                    base.glob("_instances-*"),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True,
                )
                if inst_dirs:
                    inst = inst_dirs[0]
                    try:
                        cfg_root = next((inst / "configs").glob("*/"))
                    except StopIteration:
                        cfg_root = None
                    for i in range(1, 4):
                        if cfg_root:
                            cfg = cfg_root / f"keeper_config_keeper{i}.xml"
                            if cfg.exists():
                                print(
                                    f"==== keeper{i} config ====\n{cfg.read_text()[:800]}"
                                )
                        err = inst / f"keeper{i}" / "logs" / "clickhouse-server.err.log"
                        if err.exists():
                            try:
                                txt = err.read_text()
                                print(
                                    "==== keeper"
                                    + str(i)
                                    + " err ====\n"
                                    + "\n".join(txt.splitlines()[-200:])
                                )
                            except Exception:
                                pass
                        lg = inst / f"keeper{i}" / "logs" / "clickhouse-server.log"
                        if lg.exists():
                            try:
                                txt = lg.read_text()
                                print(
                                    "==== keeper"
                                    + str(i)
                                    + " log ====\n"
                                    + "\n".join(txt.splitlines()[-200:])
                                )
                            except Exception:
                                pass
            except Exception:
                pass
            raise e
        try:
            if parse_bool(os.environ.get("KEEPER_PRINT_KEEPER_CONFIG")):
                cname = os.environ.get("KEEPER_CLUSTER_NAME", "")
                conf_root = (
                    pathlib.Path(getattr(cluster, "instances_dir", ""))
                    / "configs"
                    / (cname or "")
                )
                for i in range(1, topology + 1):
                    p = conf_root / f"keeper_config_keeper{i}.xml"
                    if p.exists():
                        try:
                            print(f"==== keeper{i} config ====\n" + p.read_text()[:800])
                        except Exception:
                            pass
        except Exception:
            pass
        to = float(env_int("KEEPER_READY_TIMEOUT", 120))
        wait_until(
            lambda: count_leaders(nodes) == 1,
            timeout_s=to,
            interval=0.5,
            desc="cluster ready",
        )
        return cluster, nodes

    return _make


def pytest_collection_modifyitems(config, items):
    return
