import os

from keeper.framework.core.util import parse_bool


def _worker_index():
    """Return pytest-xdist worker index (0-based)."""
    w = (os.environ.get("PYTEST_XDIST_WORKER") or "").strip()
    if w.startswith("gw"):
        try:
            return int(w[2:])
        except ValueError:
            return 0
    return 0


def _port_offset():
    """Compute port offset for parallel test isolation."""
    step = 100
    return _worker_index() * step


_OFF = _port_offset()

# Ports with per-worker offset for xdist parallelism
RAFT_PORT = 9234 + _OFF
CLIENT_PORT = 9181 + _OFF
CONTROL_PORT = 0
PROM_PORT = 9363 + _OFF
ID_BASE = 1

DEFAULT_FAULT_DURATION_S = 30
SAMPLER_FLUSH_EVERY = 3
SAMPLER_ROW_FLUSH_THRESHOLD = 5000
DEFAULT_ERROR_RATE = 0.1
DEFAULT_P99_MS = 10000
