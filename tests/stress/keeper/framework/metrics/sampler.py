import threading
import time

from ..core.settings import SAMPLER_FLUSH_EVERY, SAMPLER_ROW_FLUSH_THRESHOLD
from ..io.probes import lgif, mntr, prom_metrics
from ..io.prom_parse import parse_prometheus_text
from ..io.sink import sink_clickhouse


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
        self.prom_allow_prefixes = (
            tuple(prom_allow_prefixes) if prom_allow_prefixes else None
        )
        self._stop = False
        self._th = None
        self._metrics_ts_rows = []
        self._snap_count = 0
        self._flush_every = int(SAMPLER_FLUSH_EVERY)
        self._row_flush_threshold = int(SAMPLER_ROW_FLUSH_THRESHOLD)
        self._ctx = ctx

    def _snapshot_once(self):
        for n in self.nodes:
            try:
                m = mntr(n)
                l = lgif(n)
                try:
                    for k, v in (m or {}).items():
                        try:
                            val = float(v)
                        except Exception:
                            continue
                        self._metrics_ts_rows.append(
                            {
                                "run_id": self.run_id,
                                "commit_sha": self.run_meta.get("commit_sha", "local"),
                                "backend": self.run_meta.get("backend", "default"),
                                "scenario": self.scenario_id,
                                "topology": self.topology,
                                "node": n.name,
                                "stage": self.stage_prefix,
                                "source": "mntr",
                                "name": str(k),
                                "value": val,
                                "labels_json": "{}",
                            }
                        )
                except Exception:
                    pass
                if self._ctx is not None:
                    cache = self._ctx.setdefault("_metrics_cache", {})
                    entry = dict(cache.get(n.name) or {})
                    entry["mntr"] = m
                    entry["lgif"] = l
                    cache[n.name] = entry
            except Exception:
                pass
            try:
                p = prom_metrics(n)
                parsed = parse_prometheus_text(
                    p, allow_prefixes=self.prom_allow_prefixes
                )
                for r in parsed:
                    name = r.get("name", "")
                    try:
                        val = float(r.get("value", 0.0))
                    except Exception:
                        val = 0.0
                    lj = r.get("labels_json", "{}")
                    self._metrics_ts_rows.append(
                        {
                            "run_id": self.run_id,
                            "commit_sha": self.run_meta.get("commit_sha", "local"),
                            "backend": self.run_meta.get("backend", "default"),
                            "scenario": self.scenario_id,
                            "topology": self.topology,
                            "node": n.name,
                            "stage": self.stage_prefix,
                            "source": "prom",
                            "name": name,
                            "value": val,
                            "labels_json": lj,
                        }
                    )
                if self._ctx is not None:
                    cache = self._ctx.setdefault("_metrics_cache", {})
                    entry = dict(cache.get(n.name) or {})
                    entry["prom_rows"] = parsed
                    cache[n.name] = entry
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
        if not self.sink_url:
            return
        if self._metrics_ts_rows:
            sink_clickhouse(self.sink_url, "keeper_metrics_ts", self._metrics_ts_rows)
            self._metrics_ts_rows = []
