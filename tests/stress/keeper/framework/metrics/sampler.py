import threading
import json
import time

from ..core.settings import SAMPLER_FLUSH_EVERY, SAMPLER_ROW_FLUSH_THRESHOLD
from ..io.probes import lgif, mntr, prom_metrics, dirs, srvr_kv
from ..io.prom_parse import parse_prometheus_text
from ..io.sink import sink_clickhouse


def _ts_ms_str():
    t = time.time()
    lt = time.gmtime(t)
    return time.strftime("%Y-%m-%d %H:%M:%S", lt) + f".{int((t - int(t))*1000):03d}"


class MetricsSampler:
    def __init__(
        self,
        nodes,
        run_meta,
        scenario_id,
        topology,
        sink_url=None,
        interval_s=5,
        stage_prefix="run",
        run_id="",
        prom_allow_prefixes=None,
        prom_name_allowlist=None,
        prom_exclude_label_keys=None,
        prom_every_n=1,
        ctx=None,
    ):
        self.nodes = nodes
        self.run_meta = run_meta or {}
        self.scenario_id = scenario_id or ""
        self.topology = topology or 0
        self.sink_url = sink_url
        self.interval_s = max(1, int(interval_s))
        self.stage_prefix = stage_prefix
        self.run_id = run_id or ""
        # Defaults to reduce volume safely across all scenarios
        DEFAULT_NAME_ALLOW = (
            "keeper_znode_count",
            "keeper_ephemerals_count",
            "keeper_total_watches_count",
            "keeper_session_count",
            "keeper_packets_received_total",
            "keeper_packets_sent_total",
            "keeper_outstanding_requests",
            "keeper_approximate_data_size",
            "keeper_connections",
            "raft_current_term",
            "raft_commit_index",
            "raft_snapshot_index",
            "raft_is_leader",
        )
        DEFAULT_EXCL_LABEL_KEYS = ("le", "quantile")

        self.prom_allow_prefixes = (
            tuple(prom_allow_prefixes) if prom_allow_prefixes else None
        )
        # None -> use default allowlist; empty tuple/list -> disable
        if prom_name_allowlist is None:
            self.prom_name_allowlist = DEFAULT_NAME_ALLOW
        else:
            self.prom_name_allowlist = tuple(prom_name_allowlist)
            if len(self.prom_name_allowlist) == 0:
                self.prom_name_allowlist = None
        self.prom_exclude_label_keys = (
            tuple(prom_exclude_label_keys)
            if prom_exclude_label_keys
            else DEFAULT_EXCL_LABEL_KEYS
        )
        try:
            self._prom_every_n = max(1, int(prom_every_n))
        except Exception:
            self._prom_every_n = 1
        self._stop = False
        self._th = None
        self._metrics_ts_rows = []
        self._snap_count = 0
        self._flush_every = int(SAMPLER_FLUSH_EVERY)
        self._row_flush_threshold = int(SAMPLER_ROW_FLUSH_THRESHOLD)
        self._ctx = ctx

    def _append_row(self, node_name, source, name, value, labels_json="{}"):
        try:
            val = float(value)
        except Exception:
            val = 0.0
        self._metrics_ts_rows.append(
            {
                "ts": _ts_ms_str(),
                "run_id": self.run_id,
                "commit_sha": self.run_meta.get("commit_sha", "local"),
                "backend": self.run_meta.get("backend", "default"),
                "scenario": self.scenario_id,
                "topology": self.topology,
                "node": node_name,
                "stage": self.stage_prefix,
                "source": source,
                "name": str(name),
                "value": val,
                "labels_json": labels_json or "{}",
            }
        )

    def _cache_entry(self, node_name):
        if self._ctx is None:
            return None, None
        cache = self._ctx.setdefault("_metrics_cache", {})
        entry = dict(cache.get(node_name) or {})
        base = self._ctx.setdefault("_metrics_cache_baseline", {})
        return entry, base

    def _store_entry(self, node_name, entry):
        if self._ctx is None:
            return
        cache = self._ctx.setdefault("_metrics_cache", {})
        cache[node_name] = entry

    def _parse_prom(self, text):
        if self.prom_allow_prefixes is None:
            return parse_prometheus_text(
                text,
                name_allowlist=self.prom_name_allowlist,
                exclude_label_keys=self.prom_exclude_label_keys,
            )
        return parse_prometheus_text(
            text,
            allow_prefixes=self.prom_allow_prefixes,
            name_allowlist=self.prom_name_allowlist,
            exclude_label_keys=self.prom_exclude_label_keys,
        )

    def _snapshot_once(self):
        for n in self.nodes:
            try:
                m = mntr(n)
                l = lgif(n)
                try:
                    for k, v in (m or {}).items():
                        self._append_row(n.name, "mntr", k, v)
                except Exception:
                    pass
                entry, base = self._cache_entry(n.name)
                if entry is not None:
                    entry["mntr"] = m
                    entry["lgif"] = l
                    self._store_entry(n.name, entry)
                    if n.name not in (base or {}):
                        base[n.name] = {"lgif": l}
            except Exception:
                pass
            # Sample 4LW 'dirs' in a lightweight way: count lines and bytes
            try:
                d_txt = dirs(n)
                d_lines = len(d_txt.splitlines()) if d_txt else 0
                d_bytes = len(d_txt.encode("utf-8")) if d_txt else 0
                self._append_row(n.name, "dirs", "lines", d_lines)
                self._append_row(n.name, "dirs", "bytes", d_bytes)
                entry, _ = self._cache_entry(n.name)
                if entry is not None:
                    entry["dirs_lines"] = d_lines
                    entry["dirs_bytes"] = d_bytes
                    self._store_entry(n.name, entry)
            except Exception:
                pass
            # Sample 4LW 'srvr' counters
            try:
                sk = srvr_kv(n) or {}
                for k, v in sk.items():
                    self._append_row(n.name, "srvr", k, v)
                entry, _ = self._cache_entry(n.name)
                if entry is not None:
                    entry["srvr"] = sk
                    self._store_entry(n.name, entry)
            except Exception:
                pass
            try:
                if (self._snap_count % self._prom_every_n) == 0:
                    p = prom_metrics(n)
                    parsed = self._parse_prom(p)
                    for r in parsed:
                        name = r.get("name", "")
                        self._append_row(n.name, "prom", name, r.get("value", 0.0), r.get("labels_json", "{}"))
                    entry, _ = self._cache_entry(n.name)
                    if entry is not None:
                        entry["prom_rows"] = parsed
                        self._store_entry(n.name, entry)
            except Exception:
                pass

    def _loop(self):
        while not self._stop:
            self._snapshot_once()
            self._snap_count += 1
            if self.sink_url and (
                self._snap_count % self._flush_every == 0
                or len(self._metrics_ts_rows) > self._row_flush_threshold
            ):
                try:
                    self.flush()
                except Exception:
                    pass
            time.sleep(self.interval_s)

    def start(self):
        if self._th:
            return
        self._stop = False
        self._th = threading.Thread(target=self._loop, daemon=True)
        self._th.start()

    def stop(self):
        self._stop = True
        if self._th:
            self._th.join(timeout=5)
            self._th = None

    def flush(self):
        if not self._metrics_ts_rows:
            return
        try:
            print("[keeper][push-metrics] begin")
            for r in self._metrics_ts_rows:
                try:
                    print(json.dumps(r, ensure_ascii=False))
                except Exception:
                    pass
            print("[keeper][push-metrics] end")
        except Exception:
            pass
        if self.sink_url:
            sink_clickhouse(self.sink_url, "keeper_metrics_ts", self._metrics_ts_rows)
        self._metrics_ts_rows = []
