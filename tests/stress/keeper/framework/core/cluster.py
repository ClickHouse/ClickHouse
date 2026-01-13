import os
import pathlib
import shutil

from tests.integration.helpers.cluster import ClickHouseCluster

from .settings import (
    CLIENT_PORT,
    CONTROL_PORT,
    ID_BASE,
    MINIO_ACCESS_KEY,
    MINIO_ENDPOINT,
    MINIO_SECRET_KEY,
    PROM_PORT,
    RAFT_PORT,
    S3_LOG_ENDPOINT,
    S3_REGION,
    S3_SNAPSHOT_ENDPOINT,
    parse_bool,
)


def _feature_flags_xml(flags):
    if not flags:
        return ""
    try:
        parts = []
        for k, v in (flags or {}).items():
            try:
                if isinstance(v, (bool, int)):
                    val = int(v)
                elif isinstance(v, float) and v.is_integer():
                    val = int(v)
                else:
                    val = v
            except Exception:
                val = v
            parts.append(f"<{k}>{val}</{k}>")
        return "<feature_flags>" + "".join(parts) + "</feature_flags>"
    except Exception:
        return ""


def _coord_settings_xml(rocks_backend):
    try:
        return (
            "<coordination_settings>"
            "<operation_timeout_ms>10000</operation_timeout_ms>"
            "<session_timeout_ms>30000</session_timeout_ms>"
            "<heart_beat_interval_ms>500</heart_beat_interval_ms>"
            "<shutdown_timeout>5000</shutdown_timeout>"
            + ("<experimental_use_rocksdb>1</experimental_use_rocksdb>" if rocks_backend else "")
            + "</coordination_settings>"
        )
    except Exception:
        return ""


def _prometheus_block_xml():
    try:
        return (
            "<prometheus>"
            "<endpoint>/metrics</endpoint>"
            f"<port>{PROM_PORT}</port>"
            "<metrics>true</metrics>"
            "<events>true</events>"
            "<asynchronous_metrics>true</asynchronous_metrics>"
            "</prometheus>"
        )
    except Exception:
        return ""


def _listen_hosts_xml():
    return (
        '<listen_host remove="1">::</listen_host>'
        '<listen_host remove="1">::1</listen_host>'
        "<listen_host>0.0.0.0</listen_host>"
        "<listen_try>1</listen_try>"
    )


class ClusterBuilder:
    def __init__(self, file_anchor):
        self.file_anchor = file_anchor

    def build(self, topology, backend, opts):
        feature_flags = dict(opts.get("feature_flags", {}) or {})
        # Backend-specific defaults
        try:
            b = (backend or "default").strip().lower()
        except Exception:
            b = "default"
        rocks_backend = b == "rocks"
        coord_overrides_xml = opts.get("coord_overrides_xml", "")
        use_minio = bool(MINIO_ENDPOINT)
        # Allow explicit override via env: KEEPER_DISABLE_S3=1 to force local disks
        # or KEEPER_USE_S3=1 to force S3 if endpoints/minio are configured
        if parse_bool(os.environ.get("KEEPER_DISABLE_S3")):
            use_minio = False
        if parse_bool(os.environ.get("KEEPER_USE_S3")):
            use_minio = True

        # Derive unique cluster name per xdist worker to avoid docker-compose collisions
        worker = (os.environ.get("PYTEST_XDIST_WORKER", "") or "").strip()
        cbase = os.environ.get("KEEPER_CLUSTER_NAME", "keeper").strip() or "keeper"
        cname = f"{cbase}_{worker}" if worker else cbase
        # Sanitize name to characters safe for docker-compose project/network names and DNS
        try:
            cname = (
                "".join(
                    (ch.lower() if (ch.isalnum() or ch in "-_") else "_")
                    for ch in cname
                ).strip("_")
                or "keeper"
            )
        except Exception:
            pass

        cluster = ClickHouseCluster(self.file_anchor, name=cname)
        base_dir = pathlib.Path(cluster.base_dir)
        conf_dir = pathlib.Path(cluster.instances_dir) / "configs" / cname
        try:
            if conf_dir.exists():
                shutil.rmtree(conf_dir, ignore_errors=True)
        except Exception:
            pass
        conf_dir.mkdir(parents=True, exist_ok=True)

        # Ensure startup wait knobs are aligned with job env
        try:
            # Default keeper start timeout aligns to READY if not set explicitly
            if not os.environ.get("KEEPER_START_TIMEOUT_SEC"):
                os.environ["KEEPER_START_TIMEOUT_SEC"] = os.environ.get(
                    "KEEPER_READY_TIMEOUT", "300"
                )
            # Allow longer connection window when logs are progressing
            if not os.environ.get("KEEPER_CONNECT_TIMEOUT_SEC"):
                rt = int(os.environ.get("KEEPER_READY_TIMEOUT", "300") or "300")
                os.environ["KEEPER_CONNECT_TIMEOUT_SEC"] = str(rt + 180)
            # Prefer probing keeper client port first, then native/http
            if not os.environ.get("CH_WAIT_START_PORTS"):
                os.environ["CH_WAIT_START_PORTS"] = f"{CLIENT_PORT},9000,8123"
        except Exception:
            pass

        # Precompute names and server ids
        names = [f"keeper{i}" for i in range(1, topology + 1)]

        # Build shared fragments in-memory (no more separate files)
        # Feature flags and extra coordination settings
        # Only include explicit feature_flags from scenarios/backends
        ff = dict(feature_flags or {})
        # Do not allow experimental_use_rocksdb under <feature_flags>; it belongs to coordination_settings
        try:
            ff.pop("experimental_use_rocksdb", None)
        except Exception:
            pass
        extra_coord = (
            "<async_replication>1</async_replication>"
            "<compress_logs>false</compress_logs>"
            "<max_log_file_size>209715200</max_log_file_size>"
            "<max_requests_append_size>300</max_requests_append_size>"
            "<max_requests_batch_bytes_size>307200</max_requests_batch_bytes_size>"
            "<max_requests_batch_size>300</max_requests_batch_size>"
            "<raft_limits_reconnect_limit>10</raft_limits_reconnect_limit>"
            "<raft_logs_level>trace</raft_logs_level>"
            "<reserved_log_items>500000</reserved_log_items>"
        )
        if coord_overrides_xml:
            extra_coord += coord_overrides_xml
        # Minimal, broadly-supported coordination settings
        coord_settings = _coord_settings_xml(rocks_backend)
        feature_flags_xml = _feature_flags_xml(ff)
        enable_ctrl = bool(CONTROL_PORT) and parse_bool(
            os.environ.get("KEEPER_ENABLE_CONTROL", "0")
        )
        http_ctrl = (
            f"<http_control><port>{CONTROL_PORT}</port><readiness><endpoint>/ready</endpoint></readiness></http_control>"
            if enable_ctrl
            else ""
        )
        # Keeper disk selection tags
        use_s3 = bool(use_minio or S3_LOG_ENDPOINT or S3_SNAPSHOT_ENDPOINT)
        if use_s3:
            disk_select = (
                "<latest_log_storage_disk>local_log_disk</latest_log_storage_disk>"
                "<latest_snapshot_storage_disk>local_snapshot_disk</latest_snapshot_storage_disk>"
                "<old_log_storage_disk>local_log_disk</old_log_storage_disk>"
                "<old_snapshot_storage_disk>local_snapshot_disk</old_snapshot_storage_disk>"
                "<log_storage_disk>s3_keeper_log_disk</log_storage_disk>"
                "<snapshot_storage_disk>s3_keeper_snapshot_disk</snapshot_storage_disk>"
                "<storage_path>/var/lib/clickhouse/coordination</storage_path>"
            )
        else:
            # Use server defaults for local disks to avoid collisions with base configs
            disk_select = ""
        # Zookeeper client nodes xml
        zk_nodes = "".join(
            f"<node><host>{h}</host><port>{CLIENT_PORT}</port></node>" for h in names
        )
        # Disks definition xml (local + optional S3)
        disks_xml = []
        if use_s3:
            # Define only when using S3 to keep local case minimal
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
                region_tag = (lambda r: f"<region>{r}</region>" if r else "")(s3_region)
                disks_xml += [
                    f"<s3_keeper_log_disk><type>s3_plain</type><endpoint>{s3_log or ''}</endpoint>{region_tag}<use_environment_credentials>true</use_environment_credentials></s3_keeper_log_disk>",
                    f"<s3_keeper_snapshot_disk><type>s3_plain</type><endpoint>{s3_snap or ''}</endpoint>{region_tag}<use_environment_credentials>true</use_environment_credentials></s3_keeper_snapshot_disk>",
                ]
            else:
                disks_xml += [
                    f"<s3_keeper_log_disk><type>s3_plain</type><endpoint>{ep}/logs/</endpoint><access_key_id>{ak}</access_key_id><secret_access_key>{sk}</secret_access_key></s3_keeper_log_disk>",
                    f"<s3_keeper_snapshot_disk><type>s3_plain</type><endpoint>{ep}/snapshots/</endpoint><access_key_id>{ak}</access_key_id><secret_access_key>{sk}</secret_access_key></s3_keeper_snapshot_disk>",
                ]

        # Per-instance configs and add instances
        nodes = []
        # Use 1-based server ids by default for raft members
        start_sid = 1 if ID_BASE <= 0 else ID_BASE
        peers_xml = "\n".join(
            [
                f"        <server><id>{j}</id><hostname>{n}</hostname><port>{RAFT_PORT}</port></server>"
                for j, n in enumerate(names, start=start_sid)
            ]
        )
        disks_block = (
            (
                "<storage_configuration><disks>"
                + "\n".join(disks_xml)
                + "</disks></storage_configuration>"
            )
            if disks_xml
            else ""
        )
        # Avoid emitting additional top-level sections that may already exist in base config
        # Explicitly enable Prometheus endpoint for Keeper metrics collection
        prom_block = _prometheus_block_xml()
        # Do not inject <zookeeper> for keeper_server tests to avoid collisions with base configs
        zk_block = ""
        # If control is enabled, prefer probing the control port first.
        if enable_ctrl:
            try:
                cur = (os.environ.get("CH_WAIT_START_PORTS", "") or "").strip()
                head = str(CONTROL_PORT)
                os.environ["CH_WAIT_START_PORTS"] = head if not cur else f"{head},{cur}"
            except Exception:
                pass

        for i, name in enumerate(names, start=start_sid):
            # Build a single per-node config file with all required sections (minimal to avoid collisions)
            path_block = (
                "<log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>"
                "<snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>"
                if not use_s3
                else ""
            )
            keeper_server = (
                "<keeper_server>"
                f"<tcp_port>{CLIENT_PORT}</tcp_port>"
                f"<server_id>{i}</server_id>"
                + path_block
                + http_ctrl
                + extra_coord
                + coord_settings
                + feature_flags_xml
                + "<raft_configuration>\n"
                + peers_xml
                + "\n    </raft_configuration>"
                + disk_select
                + "</keeper_server>"
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
            cfg_path = (conf_dir / f"keeper_config_{name}.xml").resolve()
            cfg_path.write_text(full_xml)
            try:
                legacy_dir = base_dir / "configs" / cname
                legacy_dir.mkdir(parents=True, exist_ok=True)
                (legacy_dir / f"keeper_config_{name}.xml").write_text(full_xml)
            except Exception:
                pass
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
