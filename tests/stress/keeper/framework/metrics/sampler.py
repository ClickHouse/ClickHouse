import json
import os
import threading
import time
from typing import Dict

from keeper.framework.core.util import env_int
from keeper.framework.core.settings import (
    SAMPLER_FLUSH_EVERY,
    SAMPLER_ROW_FLUSH_THRESHOLD,
)
from keeper.framework.core.util import ts_ms
from keeper.framework.io.probes import (
    ch_async_metrics,
    ch_metrics,
    container_stats,
    dirs,
    lgif,
    mntr,
    prom_metrics,
    srvr_kv,
)
from keeper.framework.io.prom_parse import parse_prometheus_text
from keeper.framework.io.sink import sink_clickhouse


def _is_zookeeper_node(n):
    return getattr(n, "is_zookeeper", False)


def _node_has_ch_metrics(n):
    """True if node can serve prom/ch_metrics (has ClickHouse). ZKBackedNode and Keeper nodes do."""
    return callable(getattr(n, "query", None))


def compute_prom_config(default_interval: int = 10) -> Dict:
    """Compute Prometheus sampling configuration.
    
    If KEEPER_METRICS_INTERVAL_S is set: sample and push metrics every N seconds (e.g. 5).
    If not set: interval_s=10, flush every 3 snapshots (push every 30s).
    """
    metrics_interval_env = os.environ.get("KEEPER_METRICS_INTERVAL_S", "").strip()
    if metrics_interval_env:
        try:
            interval = max(1, int(metrics_interval_env))
            flush_every = 1
        except ValueError:
            interval = env_int("KEEPER_MONITOR_INTERVAL_S", default_interval)
            flush_every = SAMPLER_FLUSH_EVERY
    else:
        interval = env_int("KEEPER_MONITOR_INTERVAL_S", default_interval)
        flush_every = SAMPLER_FLUSH_EVERY
    prom_every_n = env_int("KEEPER_PROM_EVERY_N", 3)
    return {
        "interval_s": interval,
        "prom_every_n": prom_every_n,
        "flush_every": flush_every,
    }


def _make_metric_row(run_id, run_meta, scenario_id, topo, node, stage, source, name, value, labels_json="{}", ts=None):
    """Build a standardized metric row dict."""
    try:
        val = float(value)
    except (ValueError, TypeError):
        val = 0.0
    return {
        "ts": ts or ts_ms(),
        "run_id": run_id,
        "commit_sha": run_meta.get("commit_sha", "local"),
        "branch": run_meta.get("branch"),
        "backend": run_meta.get("backend", "default"),
        "job_type": run_meta.get("job_type") or "unknown",
        "pull_request_number": int(run_meta.get("pull_request_number") or 0),
        "scenario": scenario_id,
        "topology": topo,
        "node": node,
        "stage": stage,
        "source": source,
        "name": str(name),
        "value": val,
        "labels_json": labels_json or "{}",
    }


class MetricsSampler:
    def __init__(
        self,
        nodes,
        run_meta,
        scenario_id,
        topology,
        run_id,
        interval_s=10,
        prom_every_n=3,
        ctx=None,
        flush_every=None,
    ):
        self.nodes = nodes
        self.run_meta = run_meta or {}
        self.scenario_id = scenario_id or ""
        self.topology = topology or 0
        self.run_id = run_id
        self.interval_s = interval_s
        self._prom_every_n = prom_every_n
        # Defaults to reduce volume safely across all scenarios
        DEFAULT_EXCL_LABEL_KEYS = ("le", "quantile")
        self.prom_exclude_label_keys = DEFAULT_EXCL_LABEL_KEYS
        self._ctx = ctx
        self._stop = False
        self._th = None
        self._metrics_ts_rows = []
        self._flush_lock = threading.Lock()
        self._snap_count = 0
        self._flush_every = int(flush_every if flush_every is not None else SAMPLER_FLUSH_EVERY)
        self._row_flush_threshold = int(SAMPLER_ROW_FLUSH_THRESHOLD)

    def _parse_prom(self, text):
        return parse_prometheus_text(
            text,
            exclude_label_keys=self.prom_exclude_label_keys,
        )

    def snapshot_stage(self, stage, nodes):
        """Take a one-time snapshot for a specific stage (pre/post/fail).
        
        Returns list of metric rows. Useful for capturing baseline (pre) and
        final state (post) metrics that are compared for derived metrics.
        """
        if not nodes:
            nodes = self.nodes
        rows = []
        ts = ts_ms()
        for n in nodes:
            # Store baseline lgif for pre stage (used by gates for monotonic checks).
            # Skip for ZooKeeper: lgif is Keeper Raft 4LW; ZK has no equivalent.
            if stage == "pre" and self._ctx and not _is_zookeeper_node(n):
                base = self._ctx.setdefault("_metrics_cache_baseline", {})
                if n.name not in base:
                    try:
                        base[n.name] = {"lgif": lgif(n)}
                    except Exception as e:
                        print(f"[keeper][snapshot_stage] error getting lgif baseline for node {n.name}: {e}")
            
            def _append_kv(source, kv):
                for k, v in (kv or {}).items():
                    rows.append(_make_metric_row(
                        self.run_id, self.run_meta, self.scenario_id, self.topology,
                        n.name, stage, source, k, v, ts=ts
                    ))
            
            def _append_rows(source, items, name_key="name", value_key="value", labels_key=None):
                for r in items or []:
                    name = r.get(name_key)
                    value = r.get(value_key, 0)
                    labels = r.get(labels_key, "{}") if labels_key else "{}"
                    if name:
                        rows.append(_make_metric_row(
                            self.run_id, self.run_meta, self.scenario_id, self.topology,
                            n.name, stage, source, name, value, labels, ts=ts
                        ))
            
            try:
                _append_kv("mntr", mntr(n))
            except Exception as e:
                print(f"[keeper][snapshot_stage] error getting mntr for node {n.name}: {e}")
            
            try:
                d_txt = dirs(n) or ""
                rows.append(_make_metric_row(
                    self.run_id, self.run_meta, self.scenario_id, self.topology,
                    n.name, stage, "dirs", "lines", len(d_txt.splitlines()), ts=ts
                ))
                rows.append(_make_metric_row(
                    self.run_id, self.run_meta, self.scenario_id, self.topology,
                    n.name, stage, "dirs", "bytes", len(d_txt.encode("utf-8")), ts=ts
                ))
            except Exception as e:
                print(f"[keeper][snapshot_stage] error getting dirs for node {n.name}: {e}")
            
            try:
                _append_kv("srvr", srvr_kv(n))
            except Exception as e:
                print(f"[keeper][snapshot_stage] error getting srvr for node {n.name}: {e}")
            
            try:
                _append_kv("container", container_stats(n))
            except Exception as e:
                print(f"[keeper][snapshot_stage] error getting container_stats for node {n.name}: {e}")
            
            # prom/ch_metrics only from nodes that have ClickHouse (.query)
            if _node_has_ch_metrics(n):
                try:
                    parsed = self._parse_prom(prom_metrics(n))
                    _append_rows("prom", parsed, "name", "value", "labels_json")
                except Exception as e:
                    print(f"[keeper][snapshot_stage] error getting prom metrics for node {n.name}: {e}")
                try:
                    _append_rows("ch_metrics", ch_metrics(n), "name", "value")
                except Exception as e:
                    print(f"[keeper][snapshot_stage] error getting ch_metrics for node {n.name}: {e}")
                try:
                    _append_rows("ch_async_metrics", ch_async_metrics(n), "name", "value")
                except Exception as e:
                    print(f"[keeper][snapshot_stage] error getting ch_async_metrics for node {n.name}: {e}")

        sink_clickhouse(rows)
        return rows

    def _append_row_at(self, ts, node_name, source, name, value, labels_json="{}"):
        """Append a metric row with timestamp."""
        row = _make_metric_row(
            self.run_id, self.run_meta, self.scenario_id, self.topology,
            node_name, "run", source, name, value, labels_json, ts=ts
        )
        with self._flush_lock:
            self._metrics_ts_rows.append(row)

    def _append_kv_rows(self, ts, node_name, source, kv):
        """Append rows for key-value mapping."""
        for k, v in (kv or {}).items():
            self._append_row_at(ts, node_name, source, k, v)

    def _snapshot_node(self, ts, n):
        """Sample all metrics from a single node (for continuous sampling).
        ts: single timestamp for this snapshot round (same for all nodes).
        """
        def _sample_kv(source, fn):
            try:
                kv = fn()
                self._append_kv_rows(ts, n.name, source, kv)
            except Exception as e:
                print(f"[keeper][_snapshot_node] error getting {source} for node {n.name}: {e}")

        def _sample_dirs():
            try:
                d_txt = dirs(n)
                d_lines = len(d_txt.splitlines())
                d_bytes = len(d_txt.encode("utf-8"))
                self._append_row_at(ts, n.name, "dirs", "lines", d_lines)
                self._append_row_at(ts, n.name, "dirs", "bytes", d_bytes)
            except Exception as e:
                print(f"[keeper][_snapshot_node] error getting dirs for node {n.name}: {e}")

        def _sample_prom():
            # Prometheus only from nodes that have ClickHouse (.query); same as snapshot_stage.
            if not _node_has_ch_metrics(n):
                return
            # Sampling frequency: only every prom_every_n intervals (e.g. every 30s) to limit volume.
            if (self._snap_count % self._prom_every_n) != 0:
                return
            try:
                p = prom_metrics(n)
                parsed = self._parse_prom(p)
                for r in parsed:
                    self._append_row_at(ts, n.name, "prom", r.get("name", ""), r.get("value", 0.0), r.get("labels_json", "{}"))
            except Exception as e:
                print(f"[keeper][_snapshot_node] error getting prom metrics for node {n.name}: {e}")

        _sample_kv("mntr", lambda: mntr(n))
        _sample_dirs()
        _sample_kv("srvr", lambda: srvr_kv(n))
        # lgif: Keeper Raft 4LW only; ZK has no equivalent.
        if not _is_zookeeper_node(n):
            _sample_kv("lgif", lambda: lgif(n))
        _sample_kv("container", lambda: container_stats(n))
        _sample_prom()
        # Note: ch_metrics and ch_async_metrics are NOT collected during continuous sampling
        # They're expensive SQL queries and are only collected in snapshot_stage("post") at the end
        # Time-series data for these metrics is not needed - final values are sufficient

    def _snapshot_once(self):
        ts = ts_ms()
        for n in self.nodes:
            self._snapshot_node(ts, n)

    def _loop(self):
        while not self._stop:
            self._snapshot_once()
            self._snap_count += 1
            if (
                self._snap_count % self._flush_every == 0
                or len(self._metrics_ts_rows) > self._row_flush_threshold
            ):
                try:
                    self.flush()
                except Exception as e:
                    print(f"Metrics flush failed, some data points may be lost: {e}")
            time.sleep(self.interval_s)

    def start(self):
        """Start continuous metrics sampling in background thread."""
        if self._th:
            return
        self._stop = False
        self._th = threading.Thread(target=self._loop, daemon=True)
        self._th.start()

    def stop(self):
        """Stop continuous metrics sampling."""
        self._stop = True
        if self._th:
            self._th.join(timeout=max(15, self.interval_s * 2))
            self._th = None

    def flush(self):
        """Flush accumulated metrics to sink."""
        with self._flush_lock:
            rows = self._metrics_ts_rows
            if not rows:
                return
            self._metrics_ts_rows = []
        print(f"[keeper][push-metrics] flushing {len(rows)} rows")
        if os.environ.get("KEEPER_PRINT_METRICS"):
            for r in rows:
                print(json.dumps(r, ensure_ascii=False))
        sink_clickhouse(rows)

