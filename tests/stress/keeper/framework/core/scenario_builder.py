class ScenarioBuilder:
    def __init__(self, sid, name, topology=3, backend="default"):
        self._sid = sid
        self._name = name
        self._topology = int(topology)
        self._backend = backend
        self._workload = {}
        self._duration = None
        self._pre = []
        self._faults = []
        self._gates = []

    def _add_step(self, collection, step):
        if step:
            collection.append(step)

    def set_workload_config(self, path):
        self._workload = {"config": path}

    def set_duration(self, duration_s):
        self._duration = int(duration_s)

    def pre(self, step):
        self._add_step(self._pre, step)

    def fault(self, step):
        self._add_step(self._faults, step)

    def during(self, kind, on, steps):
        self._faults.append({"kind": str(kind), "on": on, "steps": list(steps or [])})

    def gate(self, gate):
        self._add_step(self._gates, gate)

    def build(self):
        out = {
            "id": self._sid,
            "name": self._name,
            "topology": self._topology,
            "backend": self._backend,
        }
        if self._duration is not None:
            out["duration"] = int(self._duration)
        if self._workload:
            out["workload"] = dict(self._workload)
        if self._pre:
            out["pre"] = list(self._pre)
        if self._faults:
            out["faults"] = list(self._faults)
        if self._gates:
            out["gates"] = list(self._gates)
        return out


# Helpers for presets


def with_jitter(
    sb, delay_ms=10, jitter_ms=5, loss_pct=0, duration_s=120, target_parallel=True
):
    step = {"kind": "netem", "on": "all", "delay_ms": int(delay_ms)}
    if jitter_ms:
        step["jitter_ms"] = int(jitter_ms)
    if loss_pct:
        step["loss_pct"] = int(loss_pct)
    step["duration_s"] = int(duration_s)
    if target_parallel:
        step["target_parallel"] = True
    sb.fault(step)


def with_gp3_disk(sb, ms=3, duration_s=120, target_parallel=True):
    step = {
        "kind": "dm_delay",
        "on": "all",
        "ms": int(ms),
        "duration_s": int(duration_s),
    }
    if target_parallel:
        step["target_parallel"] = True
    sb.fault(step)
