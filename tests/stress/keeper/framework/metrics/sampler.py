import json
import os
import threading
import time

from keeper.framework.core.settings import (
    SAMPLER_FLUSH_EVERY,
    SAMPLER_ROW_FLUSH_THRESHOLD,
)
from keeper.framework.core.util import ts_ms
from keeper.framework.io.probes import dirs, lgif, mntr, prom_metrics, srvr_kv
from keeper.framework.io.prom_parse import parse_prometheus_text
from keeper.framework.io.sink import sink_clickhouse


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
        DEFAULT_EXCL_LABEL_KEYS = ("le", "quantile")

        self.prom_allow_prefixes = (
            tuple(prom_allow_prefixes) if prom_allow_prefixes else None
        )
        # None -> no name allowlist (allow all matching prefixes);
        # empty tuple/list -> no name allowlist as well.
        if prom_name_allowlist is None:
            self.prom_name_allowlist = None
        else:
            self.prom_name_allowlist = tuple(prom_name_allowlist) or None
        self.prom_exclude_label_keys = (
            tuple(prom_exclude_label_keys)
            if prom_exclude_label_keys
            else DEFAULT_EXCL_LABEL_KEYS
        )
        self._prom_every_n = max(1, int(prom_every_n or 1))
        self._stop = False
        self._th = None
        self._metrics_ts_rows = []
        self._snap_count = 0
        self._flush_every = int(SAMPLER_FLUSH_EVERY)
        self._row_flush_threshold = int(SAMPLER_ROW_FLUSH_THRESHOLD)
        self._ctx = ctx

    def _append_row(self, node_name, source, name, value, labels_json="{}"):
        return self._append_row_at(ts_ms(), node_name, source, name, value, labels_json)

    def _append_row_at(self, ts, node_name, source, name, value, labels_json="{}"):
        try:
            val = float(value)
        except Exception:
            val = 0.0
        self._metrics_ts_rows.append(
            {
                "ts": ts,
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

    def _append_kv_rows(self, ts, node_name, source, kv):
        for k, v in (kv or {}).items():
            self._append_row_at(ts, node_name, source, k, v)

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

    def _update_cache(self, node_name, entry, **updates):
        if entry is None:
            return
        entry.update(updates)
        self._store_entry(node_name, entry)

    def _safe(self, fn):
        try:
            fn()
        except Exception:
            pass

    def _parse_prom(self, text):
        if self.prom_allow_prefixes is None:
            return parse_prometheus_text(
                text,
                allow_prefixes=None,
                name_allowlist=self.prom_name_allowlist,
                exclude_label_keys=self.prom_exclude_label_keys,
            )
        return parse_prometheus_text(
            text,
            allow_prefixes=self.prom_allow_prefixes,
            name_allowlist=self.prom_name_allowlist,
            exclude_label_keys=self.prom_exclude_label_keys,
        )

    def _snapshot_node(self, n):
        ts = ts_ms()
        entry, base = self._cache_entry(n.name)

        def sample_mntr():
            m = mntr(n)
            l = lgif(n)
            self._append_kv_rows(ts, n.name, "mntr", m)
            if base is not None and n.name not in (base or {}):
                base[n.name] = {"lgif": l}
            self._update_cache(n.name, entry, mntr=m, lgif=l)

        def sample_dirs():
            # Lightweight 4LW 'dirs': count lines and bytes
            d_txt = dirs(n)
            d_lines = len(d_txt.splitlines()) if d_txt else 0
            d_bytes = len(d_txt.encode("utf-8")) if d_txt else 0
            self._append_row_at(ts, n.name, "dirs", "lines", d_lines)
            self._append_row_at(ts, n.name, "dirs", "bytes", d_bytes)
            self._update_cache(n.name, entry, dirs_lines=d_lines, dirs_bytes=d_bytes)

        def sample_srvr():
            sk = srvr_kv(n) or {}
            self._append_kv_rows(ts, n.name, "srvr", sk)
            self._update_cache(n.name, entry, srvr=sk)

        def sample_prom():
            if (self._snap_count % self._prom_every_n) != 0:
                return
            p = prom_metrics(n)
            parsed = self._parse_prom(p)
            for r in parsed:
                self._append_row_at(
                    ts,
                    n.name,
                    "prom",
                    r.get("name", ""),
                    r.get("value", 0.0),
                    r.get("labels_json", "{}"),
                )
            self._update_cache(n.name, entry, prom_rows=parsed)

        self._safe(sample_mntr)
        self._safe(sample_dirs)
        self._safe(sample_srvr)
        self._safe(sample_prom)

    def _snapshot_once(self):
        for n in self.nodes:
            self._snapshot_node(n)

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
        rows = self._metrics_ts_rows
        if not rows:
            return
        self._metrics_ts_rows = []
        print(f"[keeper][push-metrics] flushing {len(rows)} rows")
        if os.environ.get("KEEPER_PRINT_METRICS"):
            for r in rows:
                print(json.dumps(r, ensure_ascii=False))
        sink_clickhouse(self.sink_url or "ci", "keeper_metrics_ts", rows)
