from keeper.framework.core.settings import CLIENT_PORT


def servers_arg(nodes):
    """Build server address string for keeper-bench.
    
    Args:
        nodes: List of ClickHouseCluster node instances (ip_address is set after cluster.start()).
    
    Returns:
        Space-separated string of "host:port" addresses.
    """
    addrs = []
    for n in nodes or []:
        addrs.append(f"{n.ip_address}:{int(CLIENT_PORT)}")
    return " ".join(addrs)
