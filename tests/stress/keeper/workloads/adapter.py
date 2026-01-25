from keeper.framework.core.settings import CLIENT_PORT


def servers_arg(nodes):
    """Build server address string for keeper-bench.

    Uses keeper_client_host_port (localhost) when the cluster published the
    client port for host-side bench; otherwise ip_address (Docker IP).

    Args:
        nodes: List of ClickHouseCluster node instances.

    Returns:
        Space-separated string of "host:port" addresses.
    """
    addrs = []
    for n in nodes or []:
        hp = getattr(n, "keeper_client_host_port", None)
        if hp is not None:
            addrs.append(f"localhost:{int(hp)}")
        else:
            ip = getattr(n, "ip_address", None)
            if ip:
                addrs.append(f"{ip}:{int(CLIENT_PORT)}")
    return " ".join(addrs)
