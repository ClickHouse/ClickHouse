import os

from keeper.framework.core.util import env_float, env_int, env_str, parse_bool


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
    base = env_int("KEEPER_PORT_OFFSET", 0)
    step = env_int("KEEPER_XDIST_PORT_STEP", 100)
    return base + _worker_index() * step


_OFF = _port_offset()

# Ports with per-worker offset for xdist parallelism
RAFT_PORT = env_int("KEEPER_RAFT_PORT", 9234) + _OFF
CLIENT_PORT = env_int("KEEPER_CLIENT_PORT", 9181) + _OFF
CONTROL_PORT = env_int("KEEPER_CONTROL_PORT", 0)
PROM_PORT = env_int("KEEPER_PROM_PORT", 9363) + _OFF
ID_BASE = env_int("KEEPER_ID_BASE", 1)

# S3 storage config
S3_LOG_ENDPOINT = env_str("KEEPER_S3_LOG_ENDPOINT", "")
S3_SNAPSHOT_ENDPOINT = env_str("KEEPER_S3_SNAPSHOT_ENDPOINT", "")
S3_REGION = env_str("KEEPER_S3_REGION", "")

# MinIO config
MINIO_ENDPOINT = env_str("KEEPER_MINIO_ENDPOINT", "")
MINIO_ACCESS_KEY = env_str("KEEPER_MINIO_ACCESS_KEY", "")
MINIO_SECRET_KEY = env_str("KEEPER_MINIO_SECRET_KEY", "")

# Defaults
DEFAULT_FAULT_DURATION_S = env_int("KEEPER_DEFAULT_FAULT_DURATION", 30)
SAMPLER_FLUSH_EVERY = env_int("KEEPER_SAMPLER_FLUSH_EVERY", 3)
SAMPLER_ROW_FLUSH_THRESHOLD = env_int("KEEPER_SAMPLER_ROW_FLUSH_THRESHOLD", 5000)
DEFAULT_ERROR_RATE = env_float("KEEPER_DEFAULT_ERROR_RATE", 0.1)
DEFAULT_P99_MS = env_int("KEEPER_DEFAULT_P99_MS", 10000)
