import time, shlex

def _exec(node, cmd, user=None, privileged=False):
    if isinstance(cmd, str):
        args = ["bash", "-lc", cmd]
    else:
        args = list(cmd)
    kwargs = {}
    if user:
        kwargs["user"] = user
    if privileged:
        kwargs["privileged"] = True
    out = node.exec_in_container(args, **kwargs)
    return out

def sh(node, cmd, user=None, privileged=False):
    try:
        out = _exec(node, cmd, user=user, privileged=privileged)
        return {"out": out}
    except Exception:
        return {"out": ""}

def sh_root(node, cmd):
    return sh(node, cmd, user="root", privileged=True)

def has_bin(node, name):
    r = sh(node, f"command -v {shlex.quote(name)} >/dev/null 2>&1; echo $?")
    return str(r.get("out", " ")).strip().endswith("0")

def resolve_targets(spec, nodes, leader):
    if not nodes:
        return []
    if isinstance(spec, list):
        names = set(str(x) for x in spec)
        return [n for n in nodes if n.name in names]
    s = str(spec or "leader").strip().lower()
    if s == "leader":
        return [leader]
    if s == "followers":
        return [n for n in nodes if n.name != leader.name]
    if s == "all":
        return list(nodes)
    if s == "one":
        return [leader]
    return [n for n in nodes if n.name == s] or [leader]

def wait_until(cond, timeout_s=60.0, interval=0.5, desc=""):
    start = time.time()
    while True:
        try:
            if cond():
                return True
        except Exception:
            pass
        if time.time() - start > float(timeout_s):
            raise AssertionError(desc or "timeout")
        time.sleep(float(interval))
