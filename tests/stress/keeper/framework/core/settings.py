# Keeper service ports
RAFT_PORT = 9234
CLIENT_PORT = 9181
CONTROL_PORT = 9182
PROM_PORT = 9363
ID_BASE = 1

ZK_CLIENT_PORT = 2181

# RaftKeeper (JDRaftKeeper) default client port; host-mapped ports 18101, 18102, 18103
RAFTKEEPER_CLIENT_PORT = 8101
RAFTKEEPER_HOST_PORTS = (18101, 18102, 18103)

DEFAULT_FAULT_DURATION_S = 30
MIN_FAULT_DURATION = 10
SAMPLER_FLUSH_EVERY = 3
SAMPLER_ROW_FLUSH_THRESHOLD = 5000
DEFAULT_ERROR_RATE = 0.1
DEFAULT_P99_MS = 10000
DEFAULT_P95_MS = 5000  # Typically p95 is ~50% of p99

KEEPER_CH_QUERY_TIMEOUT = 60

# Timeout defaults (in seconds)
DEFAULT_READY_TIMEOUT = 300

DEFAULT_OPERATION_TIMEOUT_MS = 30000
DEFAULT_CONNECTION_TIMEOUT_MS = 400000
# 300s: dm_delay kills all 3 nodes simultaneously for ~60-120s during setup/teardown;
# session timeout must exceed the 120s teardown window.  300s gives plenty of
# margin for the cluster to recover before sessions expire.
DEFAULT_SESSION_TIMEOUT_MS = 300000

DEFAULT_WORKLOAD_CONFIG = "workloads/prod_mix.yaml"
DEFAULT_CONCURRENCY = 64

def keeper_node_names(topology):
    """Generate keeper node names for a given topology. ZooKeeper uses ZOOKEEPER_CONTAINERS in cluster."""
    return [f"keeper{i}" for i in range(1, topology + 1)]
