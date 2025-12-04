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


class ClusterBuilder:
    def __init__(self, file_anchor):
        self.file_anchor = file_anchor

    def _render_embedded_xml(self, names, sid, start_sid=ID_BASE):
        peers = "\n".join(
            [
                f"        <server><id>{i}</id><hostname>{n}</hostname><port>{RAFT_PORT}</port></server>"
                for i, n in enumerate(names, start=start_sid)
            ]
        )
        # Only inject control endpoint when explicitly enabled to avoid failures on older images.
        enable_ctrl = bool(CONTROL_PORT) and parse_bool(
            os.environ.get("KEEPER_ENABLE_CONTROL", "0")
        )
        http_ctrl = (
            f"<http_control><port>{CONTROL_PORT}</port><readiness><endpoint>/ready</endpoint></readiness></http_control>"
            if enable_ctrl
            else ""
        )
        return f"""<clickhouse>
  <keeper_server>
    <tcp_port>{CLIENT_PORT}</tcp_port>
    <server_id>{sid}</server_id>
    <log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>
    <snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>
    {http_ctrl}
    <coordination_settings>
      <operation_timeout_ms>10000</operation_timeout_ms>
      <session_timeout_ms>30000</session_timeout_ms>
      <heart_beat_interval_ms>500</heart_beat_interval_ms>
      <election_timeout_lower_bound_ms>1000</election_timeout_lower_bound_ms>
      <election_timeout_upper_bound_ms>2000</election_timeout_upper_bound_ms>
      <force_sync>true</force_sync>
      <quorum_reads>false</quorum_reads>
      <shutdown_timeout>5000</shutdown_timeout>
      <startup_timeout>30000</startup_timeout>
    </coordination_settings>
    <raft_configuration>
{peers}
    </raft_configuration>
  </keeper_server>
</clickhouse>"""

    def build(self, topology, backend, opts):
        feature_flags = opts.get("feature_flags", {})
        coord_overrides_xml = opts.get("coord_overrides_xml", "")
        use_minio = bool(MINIO_ENDPOINT)
        # Allow explicit override via env: KEEPER_DISABLE_S3=1 to force local disks
        # or KEEPER_USE_S3=1 to force S3 if endpoints/minio are configured
        if parse_bool(os.environ.get("KEEPER_DISABLE_S3")):
            use_minio = False
        if parse_bool(os.environ.get("KEEPER_USE_S3")):
            use_minio = True

        cluster = ClickHouseCluster(
            self.file_anchor, name=os.environ.get("KEEPER_CLUSTER_NAME", "keeper")
        )
        base_dir = pathlib.Path(cluster.base_dir)
        # Use a unique configs subdir per cluster name to avoid xdist collisions
        cname = os.environ.get("KEEPER_CLUSTER_NAME", "keeper").strip() or "keeper"
        conf_dir = base_dir / "configs" / cname
        try:
            if conf_dir.exists():
                shutil.rmtree(conf_dir, ignore_errors=True)
        except Exception:
            pass
        conf_dir.mkdir(parents=True, exist_ok=True)

        # Precompute names and server ids
        names = [f"keeper{i}" for i in range(1, topology + 1)]

        # Build shared fragments in-memory (no more separate files)
        # Feature flags and extra coordination settings
        ff = {"check_not_exists": 1, "create_if_not_exists": 1, "remove_recursive": 1}
        ff.update(feature_flags or {})
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
            disk_select = (
                "<latest_log_storage_disk>local_log_disk</latest_log_storage_disk>"
                "<latest_snapshot_storage_disk>local_snapshot_disk</latest_snapshot_storage_disk>"
                "<old_log_storage_disk>local_log_disk</old_log_storage_disk>"
                "<old_snapshot_storage_disk>local_snapshot_disk</old_snapshot_storage_disk>"
                "<log_storage_disk>local_log_disk</log_storage_disk>"
                "<snapshot_storage_disk>local_snapshot_disk</snapshot_storage_disk>"
                "<storage_path>/var/lib/clickhouse/coordination</storage_path>"
            )
        # Zookeeper client nodes xml
        zk_nodes = "".join(
            f"<node><host>{h}</host><port>{CLIENT_PORT}</port></node>" for h in names
        )
        # Disks definition xml (local + optional S3)
        disks_xml = [
            "<local_log_disk><type>local</type><path>/var/lib/clickhouse/coordination/log/</path></local_log_disk>",
            "<local_snapshot_disk><type>local</type><path>/var/lib/clickhouse/coordination/snapshots/</path></local_snapshot_disk>",
        ]
        if use_s3:
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
        for i, name in enumerate(names, start=start_sid):
            # Build a single per-node config file with all required sections
            peers = "\n".join(
                [
                    f"        <server><id>{j}</id><hostname>{n}</hostname><port>{RAFT_PORT}</port></server>"
                    for j, n in enumerate(names, start=start_sid)
                ]
            )
            enable_ctrl = bool(CONTROL_PORT) and parse_bool(
                os.environ.get("KEEPER_ENABLE_CONTROL", "0")
            )
            http_ctrl = (
                f"<http_control><port>{CONTROL_PORT}</port><readiness><endpoint>/ready</endpoint></readiness></http_control>"
                if enable_ctrl
                else ""
            )
            # Merge baseline and extra coordination settings
            coord_settings = (
                "<coordination_settings>"
                "<operation_timeout_ms>10000</operation_timeout_ms>"
                "<session_timeout_ms>30000</session_timeout_ms>"
                "<heart_beat_interval_ms>500</heart_beat_interval_ms>"
                "<election_timeout_lower_bound_ms>1000</election_timeout_lower_bound_ms>"
                "<election_timeout_upper_bound_ms>2000</election_timeout_upper_bound_ms>"
                "<force_sync>true</force_sync>"
                "<quorum_reads>false</quorum_reads>"
                "<shutdown_timeout>5000</shutdown_timeout>"
                "<startup_timeout>30000</startup_timeout>"
                + (
                    "<experimental_use_rocksdb>1</experimental_use_rocksdb>"
                    if backend == "rocksdb"
                    else ""
                )
                + extra_coord
                + "</coordination_settings>"
            )
            feature_flags_xml = (
                "<feature_flags>"
                + "".join(f"<{k}>{1 if v else 0}</{k}>" for k, v in ff.items())
                + "</feature_flags>"
            )
            keeper_server = (
                "<keeper_server>"
                f"<tcp_port>{CLIENT_PORT}</tcp_port>"
                f"<server_id>{i}</server_id>"
                "<log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>"
                "<snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>"
                + http_ctrl
                + coord_settings
                + "<digest_enabled>true</digest_enabled>"
                + feature_flags_xml
                + "<raft_configuration>\n"
                + peers
                + "\n    </raft_configuration>"
                + disk_select
                + "</keeper_server>"
            )
            disks_block = (
                "<storage_configuration><disks>"
                + "\n".join(disks_xml)
                + "</disks></storage_configuration>"
            )
            prom_block = f"<prometheus><endpoint>/metrics</endpoint><port>{PROM_PORT}</port><metrics>true</metrics><events>true</events><asynchronous_metrics>true</asynchronous_metrics></prometheus>"
            zk_block = f"<zookeeper>{zk_nodes}</zookeeper>"
            macros_block = f"<macros><replica>{name}</replica><shard>1</shard></macros>"
            full_xml = (
                "<clickhouse>"
                + keeper_server
                + disks_block
                + prom_block
                + zk_block
                + macros_block
                + "</clickhouse>"
            )
            (conf_dir / f"keeper_config_{name}.xml").write_text(full_xml)
            cfgs = [f"configs/{cname}/keeper_config_{name}.xml"]
            inst = cluster.add_instance(
                name, main_configs=cfgs, with_zookeeper=False, stay_alive=True
            )
            nodes.append(inst)

        # Ensure a clean instances dir to avoid create_dir collisions
        inst_dir = pathlib.Path(cluster.instances_dir)
        if inst_dir.exists():
            try:
                shutil.rmtree(inst_dir, ignore_errors=True)
            except Exception:
                pass

        cluster.start()
        return cluster, nodes
