# Keeper service ports
RAFT_PORT = 9234
CLIENT_PORT = 9181
CONTROL_PORT = 9182
PROM_PORT = 9363
ID_BASE = 1

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

DEFAULT_OPERATION_TIMEOUT_MS = 3000
DEFAULT_CONNECTION_TIMEOUT_MS = 40000

DEFAULT_WORKLOAD_CONFIG = "workloads/prod_mix.yaml"
DEFAULT_CONCURRENCY = 64

def keeper_node_names(topology):
    """Generate list of keeper node names for a given topology.
    
    Args:
        topology: Number of nodes in the cluster
        
    Returns:
        List of node names, e.g., ['keeper1', 'keeper2', 'keeper3'] for topology=3
    """
    return [f"keeper{i}" for i in range(1, topology + 1)]
