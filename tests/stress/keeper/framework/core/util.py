import os
import shlex
import shutil
import subprocess
import sys
import threading
import time
import traceback


def _exec(node, cmd, user=None, privileged=False, timeout=None, nothrow=False):
    """Execute command in container, return raw output."""
    args = ["bash", "-lc", cmd] if isinstance(cmd, str) else list(cmd)
    kwargs = {}
    if timeout is None:
        to_env = os.environ.get("KEEPER_DOCKER_EXEC_TIMEOUT") or os.environ.get(
            "KEEPER_PYTEST_TIMEOUT"
        )
        if to_env:
            try:
                timeout = max(10, min(int(to_env), 3600))
            except Exception:
                timeout = 300
        else:
            timeout = 300
    if user:
        kwargs["user"] = user
    if privileged:
        kwargs["privileged"] = True
    if timeout is not None:
        kwargs["timeout"] = timeout
    return node.exec_in_container(args, nothrow=bool(nothrow), **kwargs)


def sh(node, cmd, user=None, privileged=False, timeout=None):
    """Execute shell command, return {"out": ...}. Swallows exceptions."""
    try:
        out = _exec(
            node,
            cmd,
            user=user,
            privileged=privileged,
            timeout=timeout,
            nothrow=True,
        )
        return {"out": out}
    except Exception:
        return {"out": ""}


def sh_strict(node, cmd, user=None, privileged=False, timeout=None):
    """Execute shell command, return {"out": ...}. Raises exceptions."""
    out = _exec(node, cmd, user=user, privileged=privileged, timeout=timeout)
    return {"out": out}


def sh_root(node, cmd, timeout=60):
    """Execute shell command as root with privileges."""
    return sh(node, cmd, user="root", privileged=True, timeout=timeout)


def sh_root_strict(node, cmd, timeout=60):
    """Execute shell command as root with privileges. Raises exceptions."""
    return sh_strict(node, cmd, user="root", privileged=True, timeout=timeout)


def has_bin(node, name):
    """Check if binary exists in container PATH."""
    r = sh(node, f"command -v {shlex.quote(name)} >/dev/null 2>&1; echo $?", timeout=5)
    return str(r.get("out", "")).strip().endswith("0")


def host_sh(cmd, timeout=None):
    """Execute a command on the host (not inside container). Returns {"out": ...}."""
    try:
        if timeout is None:
            to_env = os.environ.get("KEEPER_HOST_EXEC_TIMEOUT") or os.environ.get(
                "KEEPER_PYTEST_TIMEOUT"
            )
            if to_env:
                try:
                    timeout = max(5, min(int(to_env), 3600))
                except Exception:
                    timeout = 300
            else:
                timeout = 300
        else:
            try:
                timeout = float(timeout)
            except Exception:
                timeout = 300

        pt = os.environ.get("KEEPER_PYTEST_TIMEOUT")
        if pt:
            try:
                pt_s = int(pt)
                max_host = max(5, pt_s - 60)
                timeout = min(float(timeout), float(max_host))
            except Exception:
                pass

        args = cmd if isinstance(cmd, list) else ["bash", "-lc", cmd]
        res = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout,
            check=False,
            text=True,
        )
        return {"out": res.stdout}
    except subprocess.TimeoutExpired as ex:
        print(f"[keeper][host_sh] timeout={timeout} cmd={cmd}")
        raise ex
    except Exception:
        return {"out": ""}


def host_has_bin(name):
    """Check if binary exists on host PATH."""
    try:
        return shutil.which(str(name)) is not None
    except Exception:
        return False


def _env_value(name):
    import os

    return os.environ.get(name)


def _env_coerce(name, default, caster):
    val = _env_value(name)
    if val in (None, ""):
        return caster(default)
    try:
        return caster(val)
    except (ValueError, TypeError):
        return caster(default)


def parse_bool(v):
    """Parse value as bool (truthy if '1' or True)."""
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    return str(v).strip() == "1"


def resolve_targets(spec, nodes, leader):
    """Resolve target spec to list of nodes.

    spec can be: "leader", "followers", "all", "one", a node name, or list of names.
    """
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
    """Block until cond() returns True or timeout."""
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


def ts_ms():
    """Return UTC timestamp string with ms precision."""
    t = time.time()
    return (
        time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(t))
        + f".{int((t % 1) * 1000):03d}"
    )


def leader_or_first(nodes):
    """Return leader node, or first node if leader detection fails."""
    if not nodes:
        return None
    # Import here to avoid circular dependency
    from ..io.probes import is_leader

    for n in nodes:
        try:
            if is_leader(n):
                return n
        except Exception:
            continue
    return nodes[0]


def for_each_target(step, nodes, leader, run_one):
    """Execute run_one(node) for each resolved target.

    If step["target_parallel"] is truthy, runs in parallel threads.
    """
    targets = resolve_targets(step.get("on", "leader"), nodes, leader)

    if step.get("target_parallel"):
        errors = []

        def _wrap(t):
            try:
                run_one(t)
            except Exception as e:
                errors.append(e)

        dur = step.get("duration_s")
        slack = float(step.get("target_parallel_slack_s", 60.0))
        if dur is not None:
            try:
                deadline = time.time() + float(dur) + slack
            except Exception:
                deadline = time.time() + 300.0
        else:
            deadline = time.time() + 300.0

        threads = []
        for i, t in enumerate(targets):
            tname = ""
            try:
                tname = str(getattr(t, "name", "") or "")
            except Exception:
                tname = ""
            th = threading.Thread(
                target=_wrap,
                args=(t,),
                daemon=True,
                name=(f"target[{i}] {tname}".strip() or f"target[{i}]"),
            )
            threads.append(th)

        for th in threads:
            th.start()
        for th in threads:
            th.join(timeout=max(0.0, deadline - time.time()))

        alive = [th for th in threads if th.is_alive()]
        if alive:
            frames = {}
            try:
                frames = sys._current_frames() or {}
            except Exception:
                frames = {}
            details = []
            for th in alive:
                stack_txt = ""
                try:
                    fr = frames.get(th.ident)
                    if fr is not None:
                        stack_txt = "".join(traceback.format_stack(fr)[-12:])
                except Exception:
                    stack_txt = ""
                entry = f"thread={th.name} ident={th.ident}"
                if stack_txt:
                    entry += "\n" + stack_txt
                details.append(entry)
            raise AssertionError(
                f"for_each_target: exceeded deadline; alive_threads={len(alive)}\n"
                + "\n---\n".join(details)
            )

        if errors:
            raise errors[0]
    else:
        for t in targets:
            run_one(t)


def env_int(name, default=0):
    """Get environment variable as int with default."""
    return _env_coerce(name, default, int)


def env_float(name, default=0.0):
    """Get environment variable as float with default."""
    return _env_coerce(name, default, float)


def env_str(name, default=""):
    """Get environment variable as stripped string with default."""
    val = _env_value(name)
    if val in (None, ""):
        return str(default)
    return str(val).strip()


def env_bool(name, default=False):
    """Get environment variable as bool (truthy if "1")."""
    val = _env_value(name)
    if val in (None, ""):
        return bool(default)
    return parse_bool(val)
