from ..framework.core.settings import CLIENT_PORT


def servers_arg(nodes):
    addrs = []
    for n in nodes or []:
        host = getattr(n, "ip_address", None) or getattr(n, "name", "localhost")
        addrs.append(f"{host}:{int(CLIENT_PORT)}")
    return " ".join(addrs)
