from keeper.framework.core.settings import CLIENT_PORT


def servers_arg(nodes, in_container=False):
    """Build server address string for keeper-bench.

    Keeper-bench always runs on host (in_container=False): uses keeper_client_host_port
    when the cluster published the client port; otherwise ip_address and client_port.
    For ZooKeeper (ZooNodeWrapper), that yields host-reachable IP:2181 per node.

    Args:
        nodes: List of ClickHouseCluster node instances or ZooNodeWrapper.
        in_container: If True and ZooKeeper nodes, return Docker hostnames (zoo1:2181).
                     Kept for compatibility; callers typically use False.

    Returns:
        Space-separated string of "host:port" addresses.
    """
    if in_container and nodes and getattr(nodes[0], "is_zookeeper", False):
        # Use zk_name when set (ZKBackedNode) so we get zoo1:2181, not keeper1:2181
        return " ".join(
            f"{getattr(n, 'zk_name', n.name)}:{getattr(n, 'client_port', CLIENT_PORT)}" for n in nodes
        )
    addrs = []
    for n in nodes or []:
        hp = getattr(n, "keeper_client_host_port", None)
        if hp is not None:
            addrs.append(f"localhost:{int(hp)}")
        else:
            ip = getattr(n, "ip_address", None)
            if ip:
                port = getattr(n, "client_port", None) or CLIENT_PORT
                addrs.append(f"{ip}:{int(port)}")
    return " ".join(addrs)
