import os

def parse_bool(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip() == "1"

RAFT_PORT = int(os.environ.get("KEEPER_RAFT_PORT", "9234") or "9234")
CLIENT_PORT = int(os.environ.get("KEEPER_CLIENT_PORT", "9181") or "9181")
CONTROL_PORT = int(os.environ.get("KEEPER_CONTROL_PORT", "0") or "0")
PROM_PORT = int(os.environ.get("KEEPER_PROM_PORT", "9363") or "9363")
ID_BASE = int(os.environ.get("KEEPER_ID_BASE", "1") or "1")

S3_LOG_ENDPOINT = os.environ.get("KEEPER_S3_LOG_ENDPOINT", "").strip()
S3_SNAPSHOT_ENDPOINT = os.environ.get("KEEPER_S3_SNAPSHOT_ENDPOINT", "").strip()
S3_REGION = os.environ.get("KEEPER_S3_REGION", "").strip()

MINIO_ENDPOINT = os.environ.get("KEEPER_MINIO_ENDPOINT", "").strip()
MINIO_ACCESS_KEY = os.environ.get("KEEPER_MINIO_ACCESS_KEY", "").strip()
MINIO_SECRET_KEY = os.environ.get("KEEPER_MINIO_SECRET_KEY", "").strip()

DEFAULT_FAULT_DURATION_S = int(os.environ.get("KEEPER_DEFAULT_FAULT_DURATION", "30") or "30")

SAMPLER_FLUSH_EVERY = int(os.environ.get("KEEPER_SAMPLER_FLUSH_EVERY", "3") or "3")
SAMPLER_ROW_FLUSH_THRESHOLD = int(os.environ.get("KEEPER_SAMPLER_ROW_FLUSH_THRESHOLD", "5000") or "5000")

DEFAULT_ERROR_RATE = float(os.environ.get("KEEPER_DEFAULT_ERROR_RATE", "0.1") or "0.1")
DEFAULT_P99_MS = int(os.environ.get("KEEPER_DEFAULT_P99_MS", "10000") or "10000")
