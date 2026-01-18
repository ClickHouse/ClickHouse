import os
import pathlib
import shutil

from keeper.framework.core.settings import (
    CLIENT_PORT,
    ID_BASE,
    RAFT_PORT,
)
from tests.integration.helpers.cluster import ClickHouseCluster


def _feature_flags_xml(flags):
    """Generate XML for feature flags."""
    if not flags:
        return ""
    parts = []
    for k, v in flags.items():
        val = (
            int(v)
            if isinstance(v, (bool, int)) or (isinstance(v, float) and v.is_integer())
            else v
        )
        parts.append(f"<{k}>{val}</{k}>")
    return "<feature_flags>" + "".join(parts) + "</feature_flags>"


def _coord_settings_xml(rocks_backend):
    """Generate XML for coordination settings matching cloud production (RFC #41415).

    Args:
        rocks_backend: If True, enable RocksDB backend
    """
    rocks = (
        "<experimental_use_rocksdb>1</experimental_use_rocksdb>"
        if rocks_backend
        else ""
    )

    # Cloud production settings from RFC #41415 - these should match cloud config
    cloud_settings = (
        "<async_replication>1</async_replication>"
        "<compress_logs>false</compress_logs>"
        "<max_log_file_size>209715200</max_log_file_size>"
        "<max_requests_append_size>300</max_requests_append_size>"
        "<max_requests_batch_bytes_size>307200</max_requests_batch_bytes_size>"
        "<max_requests_batch_size>300</max_requests_batch_size>"
        "<reserved_log_items>500000</reserved_log_items>"
    )

    return (
        "<coordination_settings>"
        "<operation_timeout_ms>10000</operation_timeout_ms>"
        "<session_timeout_ms>30000</session_timeout_ms>"
        "<heart_beat_interval_ms>500</heart_beat_interval_ms>"
        "<shutdown_timeout>5000</shutdown_timeout>"
        f"{cloud_settings}"
        f"{rocks}</coordination_settings>"
    )


def _listen_hosts_xml():
    return (
        '<listen_host remove="1">::</listen_host>'
        '<listen_host remove="1">::1</listen_host>'
        "<listen_host>0.0.0.0</listen_host>"
        "<listen_try>1</listen_try>"
    )


def _normalize_backend(backend):
    return (backend or "default").strip().lower()


def _sanitize_cluster_name(name):
    # Docker-compose project/network names and DNS safe-ish
    safe = "".join((ch.lower() if ch.isalnum() or ch in "-_" else "_") for ch in name)
    return safe.strip("_") or "keeper"


def _cluster_name_from_env():
    worker = os.environ.get("PYTEST_XDIST_WORKER", "").strip()
    cbase = os.environ.get("KEEPER_CLUSTER_NAME", "keeper").strip() or "keeper"
    cname = f"{cbase}_{worker}" if worker else cbase
    return _sanitize_cluster_name(cname)


def _build_feature_flags(feature_flags):
    ff = dict(feature_flags or {})
    # Do not allow experimental_use_rocksdb under <feature_flags>; it belongs to coordination_settings
    ff.pop("experimental_use_rocksdb", None)
    # Cloud feature flags from RFC #41415 - should match cloud config
    ff.setdefault("check_not_exists", "1")
    ff.setdefault("create_if_not_exists", "1")
    ff.setdefault("remove_recursive", "1")
    return ff




def _build_disks_block():
    return ""


def _build_peers_xml(names, start_sid):
    return "\n".join(
        [
            f"        <server><id>{j}</id><hostname>{n}</hostname><port>{RAFT_PORT}</port></server>"
            for j, n in enumerate(names, start=start_sid)
        ]
    )


def _configure_startup_wait():
    ready_timeout = os.environ.get("KEEPER_READY_TIMEOUT", "300")
    os.environ.setdefault("KEEPER_START_TIMEOUT_SEC", ready_timeout)
    os.environ.setdefault(
        "KEEPER_CONNECT_TIMEOUT_SEC", str(int(ready_timeout or 300) + 180)
    )
    # Only wait for Keeper client port (and control port if enabled)
    os.environ.setdefault("CH_WAIT_START_PORTS", f"{CLIENT_PORT}")


def _keeper_server_xml(
    server_id,
    peers_xml,
    path_block,
    http_ctrl,
    coord_settings,
    feature_flags_xml,
    disk_select,
):
    return (
        "<keeper_server>"
        f"<tcp_port>{CLIENT_PORT}</tcp_port>"
        f"<server_id>{server_id}</server_id>"
        + path_block
        + http_ctrl
        + coord_settings
        + feature_flags_xml
        + "<raft_configuration>\n"
        + peers_xml
        + "\n    </raft_configuration>"
        + disk_select
        + "</keeper_server>"
    )


def _write_keeper_config(conf_dir, base_dir, cname, name, full_xml):
    cfg_path = (conf_dir / f"keeper_config_{name}.xml").resolve()
    cfg_path.write_text(full_xml)
    try:
        legacy_dir = base_dir / "configs" / cname
        legacy_dir.mkdir(parents=True, exist_ok=True)
        (legacy_dir / f"keeper_config_{name}.xml").write_text(full_xml)
    except Exception:
        pass
    return cfg_path


class ClusterBuilder:
    def __init__(self, file_anchor):
        self.file_anchor = file_anchor

    def build(self, topology, backend, opts):
        opts = opts or {}
        feature_flags = dict(opts.get("feature_flags", {}) or {})
        rocks_backend = _normalize_backend(backend) == "rocks"
        coord_overrides_xml = opts.get("coord_overrides_xml", "")
        cname = _cluster_name_from_env()

        cluster = ClickHouseCluster(self.file_anchor, name=cname)
        base_dir = pathlib.Path(cluster.base_dir)
        conf_dir = base_dir / "_keeper_configs" / cname
        if conf_dir.exists():
            shutil.rmtree(conf_dir, ignore_errors=True)
        conf_dir.mkdir(parents=True, exist_ok=True)

        _configure_startup_wait()

        # Precompute names and server ids
        names = [f"keeper{i}" for i in range(1, topology + 1)]

        # Build shared fragments in-memory (no more separate files)
        # Feature flags and coordination settings
        # Only include explicit feature_flags from scenarios/backends
        ff = _build_feature_flags(feature_flags)
        # Coordination settings matching cloud production (RFC #41415)
        coord_settings = _coord_settings_xml(rocks_backend)
        # If scenario provides a full coordination_settings block, use it as-is
        if coord_overrides_xml:
            coord_settings = coord_overrides_xml
        feature_flags_xml = _feature_flags_xml(ff)
        disk_select = ""

        # Per-instance configs and add instances
        nodes = []
        # Use 1-based server ids by default for raft members
        start_sid = 1 if ID_BASE <= 0 else ID_BASE
        peers_xml = _build_peers_xml(names, start_sid)
        disks_block = _build_disks_block()
        # Avoid emitting additional top-level sections that may already exist in base config
        # Do not inject a Prometheus block; base configs may already enable it
        prom_block = ""
        # Do not inject <zookeeper> for keeper_server tests to avoid collisions with base configs
        zk_block = ""

        for i, name in enumerate(names, start=start_sid):
            # Build a single per-node config file with all required sections (minimal to avoid collisions)
            path_block = (
                "<log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>"
                "<snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>"
            )
            keeper_server = _keeper_server_xml(
                i,
                peers_xml,
                path_block,
                "",
                coord_settings,
                feature_flags_xml,
                disk_select,
            )
            # Ensure server listens on container IPs; do not duplicate tcp_port
            net_block = _listen_hosts_xml()
            full_xml = (
                "<clickhouse>"
                + keeper_server
                + net_block
                + prom_block
                + disks_block
                + "</clickhouse>"
            )
            cfg_path = _write_keeper_config(conf_dir, base_dir, cname, name, full_xml)
            cfgs = [str(cfg_path)]
            inst = cluster.add_instance(
                name,
                main_configs=cfgs,
                with_zookeeper=False,
                stay_alive=True,
                hostname=name,
            )
            nodes.append(inst)

        cluster.start()
        return cluster, nodes
