import os
import pathlib
import shutil

from keeper.framework.core.settings import (
    CLIENT_PORT,
    CONTROL_PORT,
    DEFAULT_READY_TIMEOUT,
    ID_BASE,
    PROM_PORT,
    RAFT_PORT,
    keeper_node_names,
)
from tests.integration.helpers.cluster import ClickHouseCluster

from ci.jobs.keeper_stress_job import env_bool, env_int


def _feature_flags_xml(flags):
    """Generate XML for feature flags."""
    if not flags:
        return ""
    def xml_val(v):
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, int):
            return v
        if isinstance(v, float) and v.is_integer():
            return int(v)
        return v
    return (
        "<feature_flags>"
        + "".join(f"<{k}>{xml_val(v)}</{k}>" for k, v in flags.items())
        + "</feature_flags>"
    )


def _coord_settings_xml(rocks_backend, overrides_xml=None):
    """Generate XML for coordination settings.

    Args:
        rocks_backend: If True, enable RocksDB backend
        overrides_xml: Optional XML fragment or full <coordination_settings> block to merge
    """
    rocks = (
        "<experimental_use_rocksdb>1</experimental_use_rocksdb>"
        if rocks_backend
        else ""
    )

    settings = (
        "<async_replication>1</async_replication>"
        "<compress_logs>false</compress_logs>"
        "<max_log_file_size>209715200</max_log_file_size>"
        "<max_requests_append_size>300</max_requests_append_size>"
        "<max_requests_batch_bytes_size>307200</max_requests_batch_bytes_size>"
        "<max_requests_batch_size>300</max_requests_batch_size>"
        "<reserved_log_items>500000</reserved_log_items>"
    )

    # Extract inner content from overrides if it's a full <coordination_settings> block
    overrides_content = ""
    if overrides_xml:
        overrides_xml = overrides_xml.strip()
        if overrides_xml.startswith("<coordination_settings>"):
            # Extract content between tags
            end_tag = "</coordination_settings>"
            if overrides_xml.endswith(end_tag):
                overrides_content = overrides_xml[
                    len("<coordination_settings>") : -len(end_tag)
                ]
            else:
                raise ValueError(f"Invalid coordination settings XML: {overrides_xml}")
        else:
            # use as-is
            overrides_content = overrides_xml

    return (
        "<coordination_settings>"
        "<operation_timeout_ms>10000</operation_timeout_ms>"
        "<session_timeout_ms>30000</session_timeout_ms>"
        "<heart_beat_interval_ms>500</heart_beat_interval_ms>"
        "<shutdown_timeout>5000</shutdown_timeout>"
        f"{settings}"
        f"{overrides_content}"
        f"{rocks}</coordination_settings>"
    )


def _listen_hosts_xml():
    return (
        '<listen_host remove="1">::</listen_host>'
        '<listen_host remove="1">::1</listen_host>'
        "<listen_host>0.0.0.0</listen_host>"
        "<listen_try>1</listen_try>"
    )


def _prometheus_xml():
    return (
        "<prometheus>"
        "<endpoint>/metrics</endpoint>"
        f"<port>{PROM_PORT}</port>"
        "<keeper_metrics_only>true</keeper_metrics_only>"
        "<metrics>true</metrics>"
        "<asynchronous_metrics>true</asynchronous_metrics>"
        "</prometheus>"
    )


def _http_control_xml():
    """Generate XML for HTTP control port."""
    if env_bool("KEEPER_ENABLE_HTTP_CONTROL", False):
        return f"<http_control><port>{CONTROL_PORT}</port></http_control>"
    return ""


def _normalize_backend(backend):
    return (backend or "default").strip().lower()


def _build_feature_flags(feature_flags):
    ff = feature_flags
    # Do not allow experimental_use_rocksdb under <feature_flags>; it belongs to coordination_settings
    ff.pop("experimental_use_rocksdb", None)
    
    ff.setdefault("check_not_exists", "1")
    ff.setdefault("create_if_not_exists", "1")
    ff.setdefault("remove_recursive", "1")
    return ff


def _build_peers_xml(names, start_sid):
    return "\n".join(
        [
            f"        <server><id>{j}</id><hostname>{n}</hostname><port>{RAFT_PORT}</port></server>"
            for j, n in enumerate(names, start=start_sid)
        ]
    )


def _configure_startup_timeouts():
    """Configure start and connection timeouts from KEEPER_READY_TIMEOUT."""
    ready_timeout = env_int("KEEPER_READY_TIMEOUT", DEFAULT_READY_TIMEOUT)
    os.environ.setdefault("KEEPER_START_TIMEOUT_SEC", str(ready_timeout))
    os.environ.setdefault("KEEPER_CONNECT_TIMEOUT_SEC", str(ready_timeout + 180))
    os.environ.setdefault("CH_WAIT_START_PORTS", str(CLIENT_PORT))


def _keeper_server_xml(
    server_id,
    peers_xml,
    path_block,
    http_ctrl,
    coord_settings,
    feature_flags_xml,
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
        + "</keeper_server>"
    )


def _write_keeper_config(conf_dir, name, full_xml):
    cfg_path = (conf_dir / f"keeper_config_{name}.xml").resolve()
    cfg_path.write_text(full_xml)
    return cfg_path


def _build_node_config_xml(server_id, peers_xml, coord_settings, feature_flags_xml):
    """Build complete XML config for a single Keeper node."""
    path_block = (
        "<log_storage_path>/var/lib/clickhouse/coordination/log</log_storage_path>"
        "<snapshot_storage_path>/var/lib/clickhouse/coordination/snapshots</snapshot_storage_path>"
    )
    keeper_server = _keeper_server_xml(
        server_id, peers_xml, path_block, _http_control_xml(), coord_settings, feature_flags_xml
    )
    return (
        "<clickhouse>"
        + keeper_server
        + _prometheus_xml()
        + _listen_hosts_xml()
        + "</clickhouse>"
    )


class ClusterBuilder:
    def __init__(self, cname, file_anchor):
        self.cname = cname
        self.file_anchor = file_anchor
        self.cluster = None
        self.conf_dir = None
        self.base_dir = None

    def build(self, topology, backend, opts):
        opts = opts or {}
        self.cluster = ClickHouseCluster(self.file_anchor, name=self.cname)
        self.base_dir = pathlib.Path(self.cluster.base_dir)
        self.conf_dir = self.base_dir / "_keeper_configs" / self.cname
        if self.conf_dir.exists():
            shutil.rmtree(self.conf_dir, ignore_errors=True)
        self.conf_dir.mkdir(parents=True, exist_ok=True)

        _configure_startup_timeouts()

        # Build shared XML fragments
        feature_flags = dict(opts.get("feature_flags", {}))
        _build_feature_flags(feature_flags)
        feature_flags_xml = _feature_flags_xml(feature_flags)
        # Always build base settings with backend support, merge overrides if provided
        coord_settings = _coord_settings_xml(
            _normalize_backend(backend) == "rocks",
            overrides_xml=opts.get("coord_overrides_xml"),
        )

        # Create nodes
        names = keeper_node_names(topology)
        start_sid = 1 if ID_BASE <= 0 else ID_BASE
        peers_xml = _build_peers_xml(names, start_sid)

        nodes = []
        for server_id, name in enumerate(names, start=start_sid):
            full_xml = _build_node_config_xml(
                server_id, peers_xml, coord_settings, feature_flags_xml
            )
            cfg_path = _write_keeper_config(self.conf_dir, name, full_xml)
            nodes.append(
                self.cluster.add_instance(
                    name,
                    main_configs=[str(cfg_path)],
                    with_zookeeper=False,
                    stay_alive=True,
                    hostname=name,
                )
            )

        self.cluster.start()
        return self.cluster, nodes

    def cleanup(self, clean_artifacts=True):
        """Clean up cluster and associated files.
        
        Args:
            clean_artifacts: If True, also remove instance directories and config directories
        """
        if not self.cluster:
            return
        
        try:
            # Shutdown cluster (stops containers, removes networks, etc.)
            self.cluster.shutdown()
        except Exception:
            pass
        
        if clean_artifacts:
            try:
                # Remove config directory
                if self.conf_dir and self.conf_dir.exists():
                    shutil.rmtree(self.conf_dir, ignore_errors=True)
                # Remove instance directory
                if self.cluster:
                    if self.cluster.instances_dir.exists():
                        shutil.rmtree(self.cluster.instances_dir, ignore_errors=True)
            except Exception:
                pass
