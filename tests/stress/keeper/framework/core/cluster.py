import os
import pathlib
import shutil

from keeper.framework.core.settings import (
    CLIENT_PORT,
    CONTROL_PORT,
    ID_BASE,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    RAFT_PORT,
    S3_LOG_ENDPOINT,
    S3_REGION,
    S3_SNAPSHOT_ENDPOINT,
    parse_bool,
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


def _resolve_use_s3():
    use_minio = bool(MINIO_ENDPOINT)
    if parse_bool(os.environ.get("KEEPER_DISABLE_S3")):
        use_minio = False
    if parse_bool(os.environ.get("KEEPER_USE_S3")):
        use_minio = True
    use_s3 = bool(use_minio or S3_LOG_ENDPOINT or S3_SNAPSHOT_ENDPOINT)
    return use_s3, use_minio


def _build_feature_flags(feature_flags):
    ff = dict(feature_flags or {})
    # Do not allow experimental_use_rocksdb under <feature_flags>; it belongs to coordination_settings
    ff.pop("experimental_use_rocksdb", None)
    # Cloud feature flags from RFC #41415 - should match cloud config
    ff.setdefault("check_not_exists", "1")
    ff.setdefault("create_if_not_exists", "1")
    ff.setdefault("remove_recursive", "1")
    return ff


def _build_disk_select(use_s3):
    if not use_s3:
        return ""
    return (
        "<latest_log_storage_disk>local_log_disk</latest_log_storage_disk>"
        "<latest_snapshot_storage_disk>local_snapshot_disk</latest_snapshot_storage_disk>"
        "<old_log_storage_disk>local_log_disk</old_log_storage_disk>"
        "<old_snapshot_storage_disk>local_snapshot_disk</old_snapshot_storage_disk>"
        "<log_storage_disk>s3_keeper_log_disk</log_storage_disk>"
        "<snapshot_storage_disk>s3_keeper_snapshot_disk</snapshot_storage_disk>"
        "<storage_path>/var/lib/clickhouse/coordination</storage_path>"
    )


def _build_disks_xml(use_s3, use_minio):
    if not use_s3:
        return []
    disks_xml = [
        "<local_log_disk><type>local</type><path>/var/lib/clickhouse/coordination/log/</path></local_log_disk>",
        "<local_snapshot_disk><type>local</type><path>/var/lib/clickhouse/coordination/snapshots/</path></local_snapshot_disk>",
    ]
    ep = (MINIO_ENDPOINT or "").rstrip("/") if use_minio else None
    ak = MINIO_ACCESS_KEY
    sk = MINIO_SECRET_KEY
    s3_log = S3_LOG_ENDPOINT
    s3_snap = S3_SNAPSHOT_ENDPOINT
    s3_region = S3_REGION
    if s3_log or s3_snap:
        region_tag = f"<region>{s3_region}</region>" if s3_region else ""
        disks_xml += [
            f"<s3_keeper_log_disk><type>s3_plain</type><endpoint>{s3_log or ''}</endpoint>{region_tag}<use_environment_credentials>true</use_environment_credentials></s3_keeper_log_disk>",
            f"<s3_keeper_snapshot_disk><type>s3_plain</type><endpoint>{s3_snap or ''}</endpoint>{region_tag}<use_environment_credentials>true</use_environment_credentials></s3_keeper_snapshot_disk>",
        ]
    else:
        disks_xml += [
            f"<s3_keeper_log_disk><type>s3_plain</type><endpoint>{ep}/logs/</endpoint><access_key_id>{ak}</access_key_id><secret_access_key>{sk}</secret_access_key></s3_keeper_log_disk>",
            f"<s3_keeper_snapshot_disk><type>s3_plain</type><endpoint>{ep}/snapshots/</endpoint><access_key_id>{ak}</access_key_id><secret_access_key>{sk}</secret_access_key></s3_keeper_snapshot_disk>",
        ]
    return disks_xml


def _build_disks_block(disks_xml):
    if not disks_xml:
        return ""
    return (
        "<storage_configuration><disks>"
        + "\n".join(disks_xml)
        + "</disks></storage_configuration>"
    )


def _build_peers_xml(names, start_sid):
    return "\n".join(
        [
            f"        <server><id>{j}</id><hostname>{n}</hostname><port>{RAFT_PORT}</port></server>"
            for j, n in enumerate(names, start=start_sid)
        ]
    )


def _configure_startup_wait(enable_ctrl):
    ready_timeout = os.environ.get("KEEPER_READY_TIMEOUT", "300")
    os.environ.setdefault("KEEPER_START_TIMEOUT_SEC", ready_timeout)
    os.environ.setdefault(
        "KEEPER_CONNECT_TIMEOUT_SEC", str(int(ready_timeout or 300) + 180)
    )
    # Only wait for Keeper client port (and control port if enabled)
    os.environ.setdefault("CH_WAIT_START_PORTS", f"{CLIENT_PORT}")
    if enable_ctrl:
        try:
            os.environ["CH_WAIT_START_PORTS"] = f"{CONTROL_PORT},{CLIENT_PORT}"
        except Exception:
            pass


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
        use_s3, use_minio = _resolve_use_s3()
        cname = _cluster_name_from_env()

        cluster = ClickHouseCluster(self.file_anchor, name=cname)
        base_dir = pathlib.Path(cluster.base_dir)
        conf_dir = pathlib.Path(cluster.instances_dir) / "configs" / cname
        if conf_dir.exists():
            shutil.rmtree(conf_dir, ignore_errors=True)
        conf_dir.mkdir(parents=True, exist_ok=True)

        enable_ctrl = bool(CONTROL_PORT) and parse_bool(
            os.environ.get("KEEPER_ENABLE_CONTROL", "0")
        )
        _configure_startup_wait(enable_ctrl)

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
        http_ctrl = (
            f"<http_control><port>{CONTROL_PORT}</port><readiness><endpoint>/ready</endpoint></readiness></http_control>"
            if enable_ctrl
            else ""
        )
        # Keeper disk selection tags
        disk_select = _build_disk_select(use_s3)
        disks_xml = _build_disks_xml(use_s3, use_minio)

        # Per-instance configs and add instances
        nodes = []
        # Use 1-based server ids by default for raft members
        start_sid = 1 if ID_BASE <= 0 else ID_BASE
        peers_xml = _build_peers_xml(names, start_sid)
        disks_block = _build_disks_block(disks_xml)
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
                if not use_s3
                else ""
            )
            keeper_server = _keeper_server_xml(
                i,
                peers_xml,
                path_block,
                http_ctrl,
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
