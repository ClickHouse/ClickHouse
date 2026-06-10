#!/usr/bin/env python3
"""
tmp/fuzz_lab — parallel fuzz harness for the ClickHouse AST-fuzzer oracle.

Single entrypoint. Subcommands:
    init                    Stop any existing single-instance fuzz, then
                            (re-)render configs and create per-instance data dirs.
    start [--inst N | --all]  Launch server + fuzzer-loop for one or all instances.
    stop  [--inst N | --all]  SIGTERM both; SIGKILL after 30 s grace.
    status                  Tab-separated table: pid/uptime/rss/crashes/mismatches/passes/last_exit
                            Also auto-snapshots any newly-crashed instance and auto-respawns
                            any DOWN-without-`.stopped` instance (typically OOM victims).
    crashes                 Per-instance count + top frame of each Fatal block.
    mismatches              Tail of state/mismatches.persistent.log grouped by oracle.
    tail N                  tail -n 50 of inst/NN/logs/server.log
    lldb N                  Print an `lldb -p <pid>` command + a breakpoint script.
    snapshot N              Snapshot inst/NN to state/crashes/inst-NN-<ts>/
    purge [--inst N|--all]  Stop + delete instance data; keeps state/.
    builds                  List which variants from fleet.toml are available vs missing.
    build VARIANT           Configure + ninja the variant; on success symlink bin/<variant>.
                            Run in the foreground so the agent sees progress.
    respawn N               Stop inst N, re-render its config so the latest bin/<variant>
                            symlink is picked up, then start it again.
    cycle init [--name X]   Create runs/<id>/ with frozen fleet.toml + bin/ symlinks +
                            manifest.toml + per-instance configs.
    cycle start|stop|status|list|purge [--run X]
                            Per-run-directory variants of the legacy commands.
    refresh [--merge-master]
                            Rebuild all variants then `cycle init` + `cycle start`.
                            --merge-master also runs `git merge origin/master` first.

Designed for agent use: stdlib only, deterministic output, exit codes match
intent (0 = ok, 1 = generic error, 2 = bad usage, 49 = found a mismatch).
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

# ----------------------------------------------------------------------
# Paths and constants
# ----------------------------------------------------------------------

LAB_DIR = Path(__file__).resolve().parent
REPO_ROOT = LAB_DIR.parent.parent          # /home/ubuntu/clickhouse
BIN_DIR = LAB_DIR / "bin"
INST_BASE = LAB_DIR / "inst"
STATE_DIR = LAB_DIR / "state"
CRASHES_DIR = STATE_DIR / "crashes"
TEMPLATES_DIR = LAB_DIR / "templates"
FLEET_TOML = LAB_DIR / "fleet.toml"

USERS_XML = REPO_ROOT / "tmp" / "ch_users_fuzz.xml"
CORPUS_SQL = REPO_ROOT / "tmp" / "oracle_merged_queries.sql"
STATELESS_DIR = REPO_ROOT / "tests" / "queries" / "0_stateless"
POPULATE_SQL = LAB_DIR / "templates" / "populate.sql"

PORT_TCP_BASE = 19200
PORT_HTTP_BASE = 18200
PORT_INTER_BASE = 19209
N_INSTANCES = 16

FLEET_STATUS_LOG = STATE_DIR / "fleet_status.log"
MISMATCHES_LOG = STATE_DIR / "mismatches.persistent.log"

# Crash regexes — match in server.log AND stderr.log.
# Narrow patterns that only fire on actual signal / sanitizer crash, NOT on
# the oracle's own `LOG_FATAL` for mismatches (which prints `<Fatal>` too).
CRASH_PATTERNS = [
    # Real-crash signals: SIGILL/4, SIGABRT/6, SIGBUS/7, SIGFPE/8, SIGSEGV/11.
    # Exclude SIGTERM/15 (the harness sends it during stop/snapshot) and
    # SIGINT/2 (Ctrl-C).
    r"BaseDaemon: Received signal (4|6|7|8|11)\b",
    r"BaseDaemon: ########## Short fault",
    r"==[0-9]+==ERROR: AddressSanitizer",
    # Match only the SUMMARY lines for sanitizer findings, not the
    # bare `==PID==WARNING:` line — TSan also emits that on harmless
    # startup warnings (e.g. `memory layout is incompatible, possibly due
    # to high-entropy ASLR`), which we don't want to treat as a crash.
    # Real findings always end in a SUMMARY block.
    r"SUMMARY: AddressSanitizer",
    r"SUMMARY: ThreadSanitizer",
    r"SUMMARY: MemorySanitizer",
    r"SUMMARY: UndefinedBehaviorSanitizer",
    # Logical errors are plain exceptions on release/sanitizer builds (only
    # debug builds abort on them), so without this pattern they were
    # invisible to the watchdog — CI greps for them, the lab didn't.
    # Baseline-checked at 0 occurrences fleet-wide before adding.
    r"DB::Exception: Logical error",
]
CRASH_RE = re.compile("|".join(CRASH_PATTERNS))


# ----------------------------------------------------------------------
# Minimal TOML reader (stdlib has it from 3.11; fall back for older)
# ----------------------------------------------------------------------

def load_fleet() -> dict:
    if sys.version_info >= (3, 11):
        import tomllib
        with open(FLEET_TOML, "rb") as f:
            return tomllib.load(f)
    out: dict = {"ram_ratio": {}, "client_max_memory_usage": {}, "env": {}, "instances": []}
    text = FLEET_TOML.read_text()
    m = re.search(r"^\s*instances\s*=\s*\[([^\]]+)\]", text, re.M | re.S)
    if m:
        out["instances"] = re.findall(r'"([^"]+)"', m.group(1))
    section = None
    for line in text.splitlines():
        s = line.split("#", 1)[0].strip()
        if not s:
            continue
        m = re.match(r"\[(\w+)\]", s)
        if m:
            section = m.group(1)
            if section not in out:
                out[section] = {}
            continue
        if section and "=" in s:
            k, v = s.split("=", 1)
            k = k.strip()
            v = v.strip()
            if v.startswith('"') and v.endswith('"'):
                out[section][k] = v[1:-1]
            else:
                try:
                    out[section][k] = float(v) if "." in v else int(v)
                except ValueError:
                    out[section][k] = v
    return out


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

@dataclass
class Instance:
    n: int
    build: str
    tcp: int
    http: int
    inter: int
    dir: Path
    bin: Path
    ram_ratio: float
    client_max_memory: int
    env: str

    @property
    def server_pid_file(self) -> Path:
        return self.dir / "server.pid"

    @property
    def fuzzer_pid_file(self) -> Path:
        return self.dir / "fuzzer.pid"

    @property
    def config_xml(self) -> Path:
        return self.dir / "config.xml"

    @property
    def data_dir(self) -> Path:
        return self.dir / "data"

    @property
    def log_dir(self) -> Path:
        return self.dir / "logs"

    @property
    def server_log(self) -> Path:
        return self.log_dir / "server.log"

    @property
    def stderr_log(self) -> Path:
        return self.log_dir / "stderr.log"


def build_instances() -> list[Instance]:
    fleet = load_fleet()
    builds = fleet["instances"]
    if len(builds) != N_INSTANCES:
        sys.exit(f"fleet.toml: expected {N_INSTANCES} instances, got {len(builds)}")

    insts: list[Instance] = []
    for n, build in enumerate(builds):
        bin_link = BIN_DIR / build
        if not bin_link.exists():
            print(f"WARN inst {n:02d}: build {build!r} missing — falling back to release",
                  file=sys.stderr)
            build = "release"
            bin_link = BIN_DIR / build
        bin_real = bin_link.resolve()
        if not bin_real.exists():
            sys.exit(f"inst {n:02d}: release binary {bin_real} not found — build first")
        ram_ratio = float(fleet["ram_ratio"].get(build, 0.05))
        client_max_memory = int(fleet["client_max_memory_usage"].get(build, 1024**3))
        env = fleet["env"].get(build, "")
        insts.append(Instance(
            n=n,
            build=build,
            tcp=PORT_TCP_BASE + n * 10,
            http=PORT_HTTP_BASE + n * 10,
            inter=PORT_INTER_BASE + n * 10,
            dir=INST_BASE / f"{n:02d}",
            bin=bin_link,
            ram_ratio=ram_ratio,
            client_max_memory=client_max_memory,
            env=env,
        ))
    return insts


def render_config(inst: Instance) -> None:
    tmpl = (TEMPLATES_DIR / "server.xml.tmpl").read_text()
    rendered = (tmpl
                .replace("{{tcp_port}}", str(inst.tcp))
                .replace("{{http_port}}", str(inst.http))
                .replace("{{interserver_port}}", str(inst.inter))
                .replace("{{data_dir}}", str(inst.data_dir))
                .replace("{{log_dir}}", str(inst.log_dir))
                .replace("{{ram_ratio}}", str(inst.ram_ratio))
                .replace("{{users_xml}}", str(USERS_XML)))
    inst.config_xml.write_text(rendered)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except (ProcessLookupError, PermissionError):
        return False
    except OSError:
        return False


def _read_pid(p: Path) -> int | None:
    try:
        s = p.read_text().strip()
        return int(s) if s else None
    except (FileNotFoundError, ValueError):
        return None


def _ps_rss_kb(pid: int) -> int:
    try:
        out = subprocess.check_output(["ps", "-p", str(pid), "-o", "rss="], text=True).strip()
        return int(out) if out else 0
    except (subprocess.CalledProcessError, ValueError):
        return 0


def _proc_start_ts(pid: int) -> float | None:
    try:
        with open(f"/proc/{pid}/stat") as f:
            fields = f.read().split()
        starttime_clock = int(fields[21])
        clk_tck = os.sysconf("SC_CLK_TCK")
        with open("/proc/uptime") as f:
            uptime = float(f.read().split()[0])
        boot_time = time.time() - uptime
        return boot_time + starttime_clock / clk_tck
    except (FileNotFoundError, ValueError, IndexError):
        return None


def _stop_prior_cycle_processes() -> None:
    # Walk every runs/*/inst/*/{server,fuzzer}.pid file and stop alive pids.
    # Without this, cycle init lays down a new run dir but leaves the previous
    # cycle's servers bound to TCP/interserver ports — the new cycle's binds
    # then fail with EADDRINUSE and the just-spawned servers exit, which the
    # harness re-respawns into the same conflict in a loop.
    if not RUNS_DIR.exists():
        return
    targets: list[tuple[int, str]] = []
    for run_dir in sorted(RUNS_DIR.iterdir()):
        if not run_dir.is_dir():
            continue
        inst_root = run_dir / "inst"
        if not inst_root.is_dir():
            continue
        for inst_dir in sorted(inst_root.iterdir()):
            for name in ("server.pid", "fuzzer.pid"):
                pid_file = inst_dir / name
                pid = _read_pid(pid_file)
                if pid and _pid_alive(pid):
                    targets.append((pid, str(pid_file)))
    if not targets:
        return
    print(f"cycle init: stopping {len(targets)} live process(es) from prior cycles...")
    for pid, _ in targets:
        try:
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, PermissionError):
            pass
    deadline = time.time() + 30
    while time.time() < deadline:
        alive = [(p, s) for p, s in targets if _pid_alive(p)]
        if not alive:
            targets = []
            break
        targets = alive
        time.sleep(1)
    for pid, src in targets:
        if _pid_alive(pid):
            print(f"  SIGKILL pid={pid}  ({src})")
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
    time.sleep(1)


# ASan/sanitizer findings that are fuzzer-noise, not memory-safety bugs: the
# fuzzer requests an absurd allocation (e.g. `toFixedString('', 1048576)` over
# many rows) and the sanitizer allocator cap trips before `max_memory_usage`.
# These lines match the broad `SUMMARY: AddressSanitizer` crash pattern, so we
# count and subtract them rather than treat them as crashes.
CRASH_NOISE_RE = "allocation-size-too-big|out-of-memory|requested allocation size"


def _grep_count(path: Path) -> int:
    if not path.exists():
        return 0

    def _count(pattern: str) -> int:
        try:
            out = subprocess.check_output(
                ["grep", "-aEc", pattern, str(path)],
                text=True, stderr=subprocess.DEVNULL,
            )
            return int(out.strip())
        except (subprocess.CalledProcessError, ValueError):
            return 0

    # Real crashes minus the sanitizer resource-limit noise.
    return max(0, _count("|".join(CRASH_PATTERNS)) - _count(CRASH_NOISE_RE))


# ----------------------------------------------------------------------
# Commands
# ----------------------------------------------------------------------

def cmd_init(_args) -> int:
    # Step 1: stop any existing single-instance fuzz on port 19000.
    for pat in ("queries-file.*oracle_merged_queries", "ch_data.*server"):
        try:
            pids = subprocess.check_output(["pgrep", "-f", pat], text=True).split()
        except subprocess.CalledProcessError:
            pids = []
        for p in pids:
            try:
                os.kill(int(p), signal.SIGTERM)
            except (ProcessLookupError, ValueError):
                pass
    time.sleep(5)
    for pat in ("build/programs/clickhouse server", "queries-file", "run_fuzzer_qcc.sh"):
        try:
            pids = subprocess.check_output(["pgrep", "-f", pat], text=True).split()
        except subprocess.CalledProcessError:
            pids = []
        for p in pids:
            try:
                os.kill(int(p), signal.SIGKILL)
            except (ProcessLookupError, ValueError):
                pass

    STATE_DIR.mkdir(parents=True, exist_ok=True)
    CRASHES_DIR.mkdir(parents=True, exist_ok=True)

    for inst in build_instances():
        inst.dir.mkdir(parents=True, exist_ok=True)
        inst.data_dir.mkdir(parents=True, exist_ok=True)
        (inst.data_dir / "tmp").mkdir(exist_ok=True)
        (inst.data_dir / "user_files").mkdir(exist_ok=True)
        (inst.data_dir / "format_schemas").mkdir(exist_ok=True)
        inst.log_dir.mkdir(parents=True, exist_ok=True)
        (inst.dir / "build").write_text(inst.build + "\n")
        render_config(inst)
        for p in (inst.dir / ".stopped", inst.server_pid_file, inst.fuzzer_pid_file):
            try:
                p.unlink()
            except FileNotFoundError:
                pass
        print(f"inst {inst.n:02d}  build={inst.build:11s}  port={inst.tcp}  ram_ratio={inst.ram_ratio:.2f}")

    print(f"\ninit done. mismatches log: {MISMATCHES_LOG}")
    return 0


def _start_one(inst: Instance) -> None:
    if (inst.dir / ".stopped").exists():
        print(f"inst {inst.n:02d}: marked .stopped — skipping (snapshot pending review)")
        return

    if (pid := _read_pid(inst.server_pid_file)) and _pid_alive(pid):
        print(f"inst {inst.n:02d}: server already running pid={pid}")
    else:
        env = os.environ.copy()
        for kv in inst.env.split():
            if "=" in kv:
                k, v = kv.split("=", 1)
                env[k] = v
        cmd = [str(inst.bin), "server", "--config", str(inst.config_xml.resolve())]
        with open(inst.log_dir / "stdout.log", "ab") as out, \
             open(inst.stderr_log, "ab") as err:
            proc = subprocess.Popen(
                cmd, stdout=out, stderr=err, env=env,
                start_new_session=True, cwd=str(inst.dir),
            )
        inst.server_pid_file.write_text(str(proc.pid) + "\n")
        ok = False
        for _ in range(30):
            time.sleep(1)
            if subprocess.run(
                [str(inst.bin), "client", "--port", str(inst.tcp), "-q", "SELECT 1"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            ).returncode == 0:
                ok = True
                break
        print(f"inst {inst.n:02d}: server pid={proc.pid} port={inst.tcp} "
              f"build={inst.build} {'UP' if ok else 'NOT-RESPONDING'}")
        if not ok:
            return

        marker = inst.dir / ".populated"
        if not marker.exists():
            res = subprocess.run(
                [str(inst.bin), "client", "--port", str(inst.tcp),
                 "--multiquery", "--queries-file", str(POPULATE_SQL)],
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            )
            if res.returncode == 0:
                marker.write_text(dt.datetime.now().isoformat() + "\n")
                print(f"inst {inst.n:02d}: schema + seed loaded ({POPULATE_SQL.name})")
            else:
                print(f"inst {inst.n:02d}: populate failed (rc={res.returncode}):\n"
                      f"{res.stdout.decode(errors='replace')[:400]}")
                return

    if (pid := _read_pid(inst.fuzzer_pid_file)) and _pid_alive(pid):
        print(f"inst {inst.n:02d}: fuzzer already running pid={pid}")
    else:
        env = os.environ.copy()
        env.update({
            "PORT": str(inst.tcp),
            "INST_DIR": str(inst.dir.resolve()),
            "BIN": str(inst.bin.resolve()),
            "CORPUS": str(CORPUS_SQL),
            "STATELESS_DIR": str(STATELESS_DIR),
            "FLEET_DIR": str(LAB_DIR),
            "RAM_CAP_BYTES": str(inst.client_max_memory),
            # Path info so the fuzzer loop can self-trigger a server restart
            # on corpus rotation. The loop calls back into harness.py to
            # respawn the server. For cycle-mode instances, RUN_ID is the
            # parent run dir's name; for legacy mode, it's empty.
            "HARNESS_PY": str(LAB_DIR / "harness.py"),
            "INST_N": f"{inst.n:02d}",
            "RUN_ID": inst.dir.parent.parent.name if inst.dir.parent.parent.parent.name == "runs" else "",
        })
        with open(inst.log_dir / "fuzzer_loop.log", "ab") as flog:
            fp = subprocess.Popen(
                ["bash", str(BIN_DIR / "fuzzer_loop.sh")],
                stdout=flog, stderr=subprocess.STDOUT, env=env,
                start_new_session=True, cwd=str(inst.dir),
            )
        inst.fuzzer_pid_file.write_text(str(fp.pid) + "\n")
        print(f"inst {inst.n:02d}: fuzzer pid={fp.pid}")


def cmd_start(args) -> int:
    insts = build_instances()
    if args.inst is not None:
        _start_one(insts[args.inst])
    else:
        for inst in insts:
            _start_one(inst)
    return 0


def _stop_one(inst: Instance) -> None:
    # Best-effort: cancel any in-flight queries first. Long-running fuzzed
    # queries (e.g. nested correlated EXISTS) can keep the server busy for
    # tens of seconds and block clean SIGTERM shutdown, so the harness used
    # to wait the full 30 s grace and then SIGKILL — losing the chance for
    # the server to flush logs / dump stack traces. Send `KILL QUERY` first
    # so the server can drain and exit gracefully.
    server_pid = _read_pid(inst.server_pid_file)
    if server_pid and _pid_alive(server_pid):
        try:
            subprocess.run(
                [str(inst.bin), "client", "--port", str(inst.tcp),
                 "--connect_timeout", "2", "--receive_timeout", "5",
                 "-q", "KILL QUERY WHERE 1 SYNC"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                timeout=8,
            )
        except (subprocess.TimeoutExpired, OSError):
            pass  # server may already be unresponsive; SIGTERM next

    pids_to_kill = []
    for pf in (inst.fuzzer_pid_file, inst.server_pid_file):
        pid = _read_pid(pf)
        if pid and _pid_alive(pid):
            pids_to_kill.append(pid)
    for pid in pids_to_kill:
        try:
            os.kill(pid, signal.SIGTERM)
        except (ProcessLookupError, ValueError):
            pass
    for _ in range(30):
        alive = [pid for pid in pids_to_kill if _pid_alive(pid)]
        if not alive:
            break
        time.sleep(1)
    for pid in pids_to_kill:
        if _pid_alive(pid):
            try:
                os.kill(pid, signal.SIGKILL)
            except (ProcessLookupError, ValueError):
                pass
    print(f"inst {inst.n:02d}: stopped (pids={pids_to_kill})")


def cmd_stop(args) -> int:
    insts = build_instances()
    if args.inst is not None:
        _stop_one(insts[args.inst])
    else:
        for inst in insts:
            _stop_one(inst)
    return 0


def _snapshot_one(inst: Instance, dest_root: Path | None = None) -> Path:
    ts = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    root = dest_root if dest_root is not None else CRASHES_DIR
    root.mkdir(parents=True, exist_ok=True)
    dst = root / f"inst-{inst.n:02d}-{ts}"
    dst.mkdir(parents=True, exist_ok=True)

    # ClickHouse leaves `data/store/<uuid>/` at mode 700 while the server is
    # alive, and the watchdog runs the snapshot from the same process that
    # decides whether to stop the instance — so the snapshot races the
    # still-running daemon and `copytree` used to raise `shutil.Error` with a
    # batch of `PermissionError` entries, aborting the entire cycle-status
    # walk. Swallow `EACCES` here so the rest of the data + the log dir
    # still get captured and the watchdog can move on to the next instance.
    def _copy_skipping_perm(src: Path, sub_dst: Path) -> None:
        if not src.exists():
            return
        try:
            shutil.copytree(src, sub_dst, symlinks=True, dirs_exist_ok=True)
        except shutil.Error as e:
            non_perm = [err for err in e.args[0] if "Permission denied" not in str(err[2])]
            if non_perm:
                raise shutil.Error(non_perm) from e
            perm_count = len(e.args[0])
            print(f"  ! snapshot inst {inst.n:02d}: skipped {perm_count} unreadable "
                  f"path(s) under {src} (server still holds them)",
                  file=sys.stderr)

    _copy_skipping_perm(inst.data_dir, dst / "data")
    _copy_skipping_perm(inst.log_dir, dst / "logs")
    repro = dst / "repro_hint.sql"
    try:
        out_b = subprocess.check_output(
            ["grep", "-aE", "ASTFuzzer: Fuzzed query:", str(inst.server_log)],
            stderr=subprocess.DEVNULL,
        )
        out = out_b.decode("utf-8", errors="replace")
        lines = out.splitlines()[-200:]
        repro.write_text("\n".join(re.sub(r".*Fuzzed query: ", "", l) for l in lines))
    except subprocess.CalledProcessError:
        repro.write_text("-- no fuzzed-query log lines found\n")
    return dst


def cmd_snapshot(args) -> int:
    insts = build_instances()
    dst = _snapshot_one(insts[args.inst])
    print(f"snapshot: {dst}")
    return 0


def cmd_status(_args) -> int:
    insts = build_instances()
    now = time.time()
    header = "inst  build       port   srv_pid  uptime  rss(MB)  crash  mism  passes  last_exit"
    print(header)
    total_rss = 0
    found_new_crash = False
    for inst in insts:
        srv_pid = _read_pid(inst.server_pid_file)
        srv_alive = srv_pid is not None and _pid_alive(srv_pid)
        uptime_s = 0
        rss_mb = 0
        if srv_alive:
            start = _proc_start_ts(srv_pid)
            uptime_s = int(now - start) if start else 0
            rss_mb = _ps_rss_kb(srv_pid) // 1024
            total_rss += rss_mb

        crashes = _grep_count(inst.server_log) + _grep_count(inst.stderr_log)
        marker = inst.dir / ".last_crash_count"
        prev = int(marker.read_text().strip()) if marker.exists() else 0
        if crashes > prev:
            found_new_crash = True
            print(f"  ! NEW CRASH on inst {inst.n:02d} (was {prev}, now {crashes}) — snapshotting + stopping",
                  file=sys.stderr)
            dst = _snapshot_one(inst)
            (inst.dir / ".stopped").write_text(f"crashed @ {dt.datetime.now().isoformat()} -> {dst}\n")
            _stop_one(inst)
        marker.write_text(str(crashes))

        mism = 0
        if (inst.dir / "mismatches.log").exists():
            try:
                mism = sum(1 for line in open(inst.dir / "mismatches.log", errors="replace")
                           if line.startswith("=== MISMATCH"))
            except OSError:
                pass

        passes = 0
        if inst.server_log.exists():
            try:
                passes = int(subprocess.check_output(
                    ["grep", "-acE", "oracle passed", str(inst.server_log)],
                    text=True, stderr=subprocess.DEVNULL,
                ).strip())
            except (subprocess.CalledProcessError, ValueError):
                passes = 0

        last_exit = ""
        if (inst.dir / "status.log").exists():
            try:
                tail = subprocess.check_output(
                    ["tail", "-n", "1", str(inst.dir / "status.log")], text=True,
                ).strip()
                m = re.search(r"exit=(\S+)", tail)
                last_exit = m.group(1) if m else ""
            except subprocess.CalledProcessError:
                pass

        stopped = (inst.dir / ".stopped").exists()
        status_marker = "STOPPED" if stopped else ("up" if srv_alive else "DOWN")
        print(f"  {inst.n:02d}  {inst.build:10s}  {inst.tcp}  "
              f"{srv_pid if srv_pid else '-':>7}  "
              f"{uptime_s:>6}  {rss_mb:>7}  {crashes:>5}  {mism:>4}  {passes:>6}  "
              f"{last_exit:>9}  {status_marker}")

        if not srv_alive and not stopped and not found_new_crash:
            print(f"  ~ inst {inst.n:02d}: DOWN with no .stopped marker — auto-respawning",
                  file=sys.stderr)
            _start_one(inst)

    print(f"\nfleet total RSS: {total_rss} MB")
    with open(FLEET_STATUS_LOG, "a") as f:
        f.write(f"{dt.datetime.now().isoformat()} total_rss_mb={total_rss}\n")
    return 49 if found_new_crash else 0


def cmd_crashes(_args) -> int:
    insts = build_instances()
    for inst in insts:
        if not inst.server_log.exists():
            continue
        try:
            out = subprocess.check_output(
                ["grep", "-anE",
                 r"Received signal|<Fatal>|AddressSanitizer:|ThreadSanitizer:|MemorySanitizer:|UndefinedBehaviorSanitizer:|runtime error:",
                 str(inst.server_log)],
                text=True, stderr=subprocess.DEVNULL,
            )
            lines = out.splitlines()
            if lines:
                print(f"=== inst {inst.n:02d} ({len(lines)} fatal/sanitizer lines) ===")
                for line in lines[:5]:
                    print(f"  {line}")
        except subprocess.CalledProcessError:
            pass
    return 0


def cmd_mismatches(_args) -> int:
    if not MISMATCHES_LOG.exists():
        print("no mismatches yet")
        return 0
    subprocess.run(["tail", "-n", "120", str(MISMATCHES_LOG)])
    return 0


def cmd_tail(args) -> int:
    insts = build_instances()
    subprocess.run(["tail", "-n", "50", str(insts[args.inst].server_log)])
    return 0


def cmd_lldb(args) -> int:
    insts = build_instances()
    inst = insts[args.inst]
    pid = _read_pid(inst.server_pid_file)
    if not pid or not _pid_alive(pid):
        print(f"inst {inst.n:02d}: server not running", file=sys.stderr)
        return 1
    script_path = inst.dir / "lldb.script"
    script_path.write_text(
        "# Attach with: lldb -p <pid> -s tmp/fuzz_lab/inst/NN/lldb.script\n"
        "process handle SIGSEGV --stop true --pass false --notify true\n"
        "process handle SIGABRT --stop true --pass false --notify true\n"
        "breakpoint set --name eset_remove\n"
        "breakpoint set --name edata_list_inactive_remove\n"
        "breakpoint set --name abortOnFailedAssertion\n"
        "continue\n"
    )
    print(f"lldb -p {pid} -s {script_path.resolve()}")
    print(f"# server log:  {inst.server_log}")
    print(f"# pid file:    {inst.server_pid_file}")
    return 0


def cmd_purge(args) -> int:
    insts = build_instances()
    targets = [insts[args.inst]] if args.inst is not None else insts
    for inst in targets:
        _stop_one(inst)
        for p in (inst.data_dir, inst.log_dir):
            if p.exists():
                shutil.rmtree(p)
        for p in (inst.dir / "status.log",
                  inst.dir / "mismatches.log",
                  inst.dir / ".stopped",
                  inst.dir / ".last_crash_count",
                  inst.dir / ".populated"):
            try:
                p.unlink()
            except FileNotFoundError:
                pass
        inst.data_dir.mkdir(exist_ok=True)
        (inst.data_dir / "tmp").mkdir(exist_ok=True)
        (inst.data_dir / "user_files").mkdir(exist_ok=True)
        (inst.data_dir / "format_schemas").mkdir(exist_ok=True)
        inst.log_dir.mkdir(exist_ok=True)
        print(f"inst {inst.n:02d}: purged (state/ kept)")
    return 0


# ----------------------------------------------------------------------
# Build helpers
# ----------------------------------------------------------------------

KNOWN_VARIANTS = ("release", "debug", "asan", "asan_ubsan", "ubsan", "tsan", "msan")
BUILD_SCRIPT = BIN_DIR / "build_variant.sh"


def _variants_in_fleet() -> list[str]:
    fleet = load_fleet()
    seen: dict[str, None] = {}
    for v in fleet["instances"]:
        seen[v] = None
    return list(seen)


def cmd_builds(_args) -> int:
    print(f"{'variant':<12}  {'symlink':<6}  {'binary':<8}  target")
    for v in _variants_in_fleet():
        link = BIN_DIR / v
        has_link = link.is_symlink() or link.exists()
        target = link.resolve() if has_link else Path()
        ok = has_link and target.exists()
        print(f"{v:<12}  "
              f"{'yes' if has_link else 'no':<6}  "
              f"{'yes' if ok else 'no':<8}  "
              f"{target if has_link else '(missing)'}")
    print()
    print(f"Build a missing variant with:  python3 harness.py build <variant>")
    return 0


def cmd_build(args) -> int:
    if args.variant not in KNOWN_VARIANTS:
        print(f"unknown variant {args.variant!r}; known: {', '.join(KNOWN_VARIANTS)}",
              file=sys.stderr)
        return 2
    if not BUILD_SCRIPT.exists():
        print(f"missing build script {BUILD_SCRIPT}", file=sys.stderr)
        return 1
    print(f"==> building variant {args.variant} (this can take ~30 min on cold cache)")
    rc = subprocess.run(["bash", str(BUILD_SCRIPT), args.variant]).returncode
    if rc != 0:
        print(f"build failed (rc={rc})", file=sys.stderr)
        return rc
    print(f"==> bin/{args.variant} symlinked. Use `harness.py respawn N` to swap an instance.")
    return 0


def cmd_respawn(args) -> int:
    insts = build_instances()
    inst = insts[args.inst]
    _stop_one(inst)
    stopped = inst.dir / ".stopped"
    if stopped.exists():
        stopped.unlink()
    inst = build_instances()[args.inst]
    _start_one(inst)
    return 0


# ----------------------------------------------------------------------
# Cycle infrastructure: per-run directories
# ----------------------------------------------------------------------

RUNS_DIR = LAB_DIR / "runs"


@dataclass
class Run:
    id: str
    dir: Path
    base_sha: str = ""

    @property
    def manifest(self) -> Path:
        return self.dir / "manifest.toml"

    @property
    def fleet_toml(self) -> Path:
        return self.dir / "fleet.toml"

    @property
    def bin_dir(self) -> Path:
        return self.dir / "bin"

    @property
    def inst_base(self) -> Path:
        return self.dir / "inst"

    @property
    def crashes_dir(self) -> Path:
        return self.dir / "crashes"

    @property
    def status_log(self) -> Path:
        return self.dir / "status.log"


def _git_sha(short: bool = True) -> str:
    try:
        cmd = ["git", "rev-parse", "--short", "HEAD"] if short else ["git", "rev-parse", "HEAD"]
        return subprocess.check_output(cmd, cwd=REPO_ROOT, text=True).strip()
    except subprocess.CalledProcessError:
        return "unknown"


def _list_run_ids() -> list[str]:
    if not RUNS_DIR.exists():
        return []
    return sorted(d.name for d in RUNS_DIR.iterdir()
                  if d.is_dir() and (d / "manifest.toml").exists())


def _load_run(run_id: str) -> Run:
    d = RUNS_DIR / run_id
    if not (d / "manifest.toml").exists():
        sys.exit(f"run {run_id!r} not found at {d}")
    base_sha = ""
    try:
        for line in (d / "manifest.toml").read_text().splitlines():
            m = re.match(r'^base_sha\s*=\s*"([^"]+)"', line)
            if m:
                base_sha = m.group(1)
                break
    except OSError:
        pass
    return Run(id=run_id, dir=d, base_sha=base_sha)


def _latest_run() -> Run | None:
    ids = _list_run_ids()
    return _load_run(ids[-1]) if ids else None


def _resolve_run(args_run: str | None) -> Run:
    if args_run and args_run != "latest":
        return _load_run(args_run)
    r = _latest_run()
    if r is None:
        sys.exit("no runs exist; run `lab cycle init` first")
    return r


def _build_instances_for_run(run: Run) -> list[Instance]:
    if sys.version_info >= (3, 11):
        import tomllib
        with open(run.fleet_toml, "rb") as f:
            fleet = tomllib.load(f)
    else:
        global FLEET_TOML
        saved = FLEET_TOML
        try:
            FLEET_TOML = run.fleet_toml
            fleet = load_fleet()
        finally:
            FLEET_TOML = saved

    builds = fleet["instances"]
    if len(builds) != N_INSTANCES:
        sys.exit(f"{run.fleet_toml}: expected {N_INSTANCES} instances, got {len(builds)}")

    insts: list[Instance] = []
    for n, build in enumerate(builds):
        bin_link = run.bin_dir / build
        if not bin_link.exists():
            print(f"WARN inst {n:02d}: build {build!r} missing in run {run.id} — falling back to release",
                  file=sys.stderr)
            build = "release"
            bin_link = run.bin_dir / build
        bin_real = bin_link.resolve()
        if not bin_real.exists():
            sys.exit(f"inst {n:02d}: binary {bin_real} not found")
        ram_ratio = float(fleet["ram_ratio"].get(build, 0.05))
        client_max_memory = int(fleet["client_max_memory_usage"].get(build, 1024**3))
        env = fleet["env"].get(build, "")
        insts.append(Instance(
            n=n,
            build=build,
            tcp=PORT_TCP_BASE + n * 10,
            http=PORT_HTTP_BASE + n * 10,
            inter=PORT_INTER_BASE + n * 10,
            dir=run.inst_base / f"{n:02d}",
            bin=bin_link,
            ram_ratio=ram_ratio,
            client_max_memory=client_max_memory,
            env=env,
        ))
    return insts


def cmd_cycle_init(args) -> int:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    ts = dt.datetime.now().strftime("%Y%m%d-%H%M")
    sha = _git_sha(short=True)
    run_id = args.name if args.name else f"{ts}-{sha}"
    run = Run(id=run_id, dir=RUNS_DIR / run_id, base_sha=_git_sha(short=False))
    if run.dir.exists():
        sys.exit(f"run {run.id!r} already exists at {run.dir}")
    _stop_prior_cycle_processes()
    run.dir.mkdir(parents=True)

    shutil.copy(FLEET_TOML, run.fleet_toml)

    run.bin_dir.mkdir(parents=True)
    for child in BIN_DIR.iterdir():
        if child.is_symlink() or child.is_file():
            target = child.resolve() if child.is_symlink() else child
            dest = run.bin_dir / child.name
            if child.is_symlink():
                dest.symlink_to(target)
            else:
                shutil.copy(child, dest)
                dest.chmod(0o755)

    insts = _build_instances_for_run(run)
    for inst in insts:
        inst.dir.mkdir(parents=True, exist_ok=True)
        inst.data_dir.mkdir(parents=True, exist_ok=True)
        (inst.data_dir / "tmp").mkdir(exist_ok=True)
        (inst.data_dir / "user_files").mkdir(exist_ok=True)
        (inst.data_dir / "format_schemas").mkdir(exist_ok=True)
        inst.log_dir.mkdir(parents=True, exist_ok=True)
        (inst.dir / "build").write_text(inst.build + "\n")
        render_config(inst)

    build_ids = {}
    for variant_link in sorted(run.bin_dir.iterdir()):
        if variant_link.is_symlink() or variant_link.is_file():
            try:
                out = subprocess.check_output(
                    ["readelf", "-n", str(variant_link.resolve())],
                    text=True, stderr=subprocess.DEVNULL,
                )
                m = re.search(r"Build ID:\s*([0-9a-f]+)", out)
                if m:
                    build_ids[variant_link.name] = m.group(1)
            except (subprocess.CalledProcessError, FileNotFoundError):
                pass

    manifest_lines = [
        f'id = "{run.id}"',
        f'base_sha = "{run.base_sha}"',
        f'started_at = "{dt.datetime.now().isoformat()}"',
        '',
        '[build_ids]',
    ]
    for variant, bid in build_ids.items():
        manifest_lines.append(f'{variant} = "{bid}"')
    run.manifest.write_text("\n".join(manifest_lines) + "\n")

    run.crashes_dir.mkdir(exist_ok=True)
    print(f"cycle init: {run.dir}")
    print(f"  base_sha: {run.base_sha}")
    print(f"  builds:   {', '.join(sorted(build_ids))}")
    return 0


def cmd_cycle_start(args) -> int:
    run = _resolve_run(args.run)
    insts = _build_instances_for_run(run)
    targets = [insts[args.inst]] if args.inst is not None else insts
    for inst in targets:
        _start_one(inst)
    print(f"\ncycle start: {run.dir}")
    return 0


def cmd_cycle_stop(args) -> int:
    run = _resolve_run(args.run)
    insts = _build_instances_for_run(run)
    targets = [insts[args.inst]] if args.inst is not None else insts
    for inst in targets:
        _stop_one(inst)
    return 0


def cmd_cycle_status(args) -> int:
    run = _resolve_run(args.run)
    insts = _build_instances_for_run(run)
    now = time.time()
    header = "inst  build       port   srv_pid  uptime  rss(MB)  crash  mism  passes  last_exit"
    print(f"run: {run.id}  (base_sha {run.base_sha[:12]})")
    print(header)
    total_rss = 0
    found_new_crash = False
    for inst in insts:
        srv_pid = _read_pid(inst.server_pid_file)
        srv_alive = srv_pid is not None and _pid_alive(srv_pid)
        uptime_s = 0
        rss_mb = 0
        if srv_alive:
            start = _proc_start_ts(srv_pid)
            uptime_s = int(now - start) if start else 0
            rss_mb = _ps_rss_kb(srv_pid) // 1024
            total_rss += rss_mb

        crashes = _grep_count(inst.server_log) + _grep_count(inst.stderr_log)
        marker = inst.dir / ".last_crash_count"
        prev = int(marker.read_text().strip()) if marker.exists() else 0
        if crashes > prev:
            found_new_crash = True
            print(f"  ! NEW CRASH on inst {inst.n:02d} (was {prev}, now {crashes}) — snapshotting + stopping",
                  file=sys.stderr)
            dst = _snapshot_one(inst, dest_root=run.crashes_dir)
            (inst.dir / ".stopped").write_text(f"crashed @ {dt.datetime.now().isoformat()} -> {dst}\n")
            _stop_one(inst)
        marker.write_text(str(crashes))

        mism = 0
        if (inst.dir / "mismatches.log").exists():
            try:
                mism = sum(1 for line in open(inst.dir / "mismatches.log", errors="replace")
                           if line.startswith("=== MISMATCH"))
            except OSError:
                pass

        passes = 0
        if inst.server_log.exists():
            try:
                passes = int(subprocess.check_output(
                    ["grep", "-acE", "oracle passed", str(inst.server_log)],
                    text=True, stderr=subprocess.DEVNULL,
                ).strip())
            except (subprocess.CalledProcessError, ValueError):
                passes = 0

        last_exit = ""
        if (inst.dir / "status.log").exists():
            try:
                tail = subprocess.check_output(
                    ["tail", "-n", "1", str(inst.dir / "status.log")], text=True,
                ).strip()
                m = re.search(r"exit=(\S+)", tail)
                last_exit = m.group(1) if m else ""
            except subprocess.CalledProcessError:
                pass

        stopped = (inst.dir / ".stopped").exists()
        status_marker = "STOPPED" if stopped else ("up" if srv_alive else "DOWN")
        print(f"  {inst.n:02d}  {inst.build:10s}  {inst.tcp}  "
              f"{srv_pid if srv_pid else '-':>7}  "
              f"{uptime_s:>6}  {rss_mb:>7}  {crashes:>5}  {mism:>4}  {passes:>6}  "
              f"{last_exit:>9}  {status_marker}")

        if not srv_alive and not stopped and not found_new_crash:
            print(f"  ~ inst {inst.n:02d}: DOWN with no .stopped — auto-respawning",
                  file=sys.stderr)
            _start_one(inst)

    print(f"\nfleet total RSS: {total_rss} MB")
    with open(run.status_log, "a") as f:
        f.write(f"{dt.datetime.now().isoformat()} total_rss_mb={total_rss}\n")
    return 49 if found_new_crash else 0


def cmd_cycle_list(_args) -> int:
    ids = _list_run_ids()
    if not ids:
        print("(no runs yet — run `lab cycle init` to create one)")
        return 0
    print(f"{'id':<35} {'base_sha':<12} {'inst':<5} {'crashes':<8} {'state':<10}")
    for rid in ids:
        run = _load_run(rid)
        crashes = sum(1 for _ in run.crashes_dir.iterdir() if (run.crashes_dir / _.name).is_dir()) if run.crashes_dir.exists() else 0
        insts_alive = 0
        try:
            insts = _build_instances_for_run(run)
            for inst in insts:
                pid = _read_pid(inst.server_pid_file)
                if pid and _pid_alive(pid):
                    insts_alive += 1
        except SystemExit:
            insts_alive = -1
        state = "active" if insts_alive > 0 else "stopped"
        print(f"{rid:<35} {run.base_sha[:12]:<12} {insts_alive:<5} {crashes:<8} {state:<10}")
    return 0


def cmd_cycle_purge(args) -> int:
    run = _resolve_run(args.run)
    insts = _build_instances_for_run(run)
    for inst in insts:
        _stop_one(inst)
        for p in (inst.data_dir, inst.log_dir):
            if p.exists():
                shutil.rmtree(p)
        for p in (inst.dir / "status.log", inst.dir / "mismatches.log",
                  inst.dir / ".stopped", inst.dir / ".last_crash_count",
                  inst.dir / ".populated"):
            try: p.unlink()
            except FileNotFoundError: pass
    print(f"cycle purge: {run.dir} (crashes/ preserved)")
    return 0


def cmd_refresh(args) -> int:
    if args.merge_master:
        print("==> merging origin/master into current branch")
        rc = subprocess.run(["git", "fetch", "origin", "master"], cwd=REPO_ROOT).returncode
        if rc != 0:
            print("git fetch failed", file=sys.stderr); return rc
        out = subprocess.check_output(["git", "status", "--porcelain"], cwd=REPO_ROOT, text=True).strip()
        if out:
            print(f"working tree dirty, refusing to merge:\n{out}", file=sys.stderr)
            return 1
        rc = subprocess.run(["git", "merge", "--no-edit", "origin/master"], cwd=REPO_ROOT).returncode
        if rc != 0:
            print("git merge failed (resolve conflicts manually)", file=sys.stderr); return rc

    variants = _variants_in_fleet()
    failures = []
    for variant in variants:
        print(f"==> rebuilding {variant}")
        rc = subprocess.run(["bash", str(BUILD_SCRIPT), variant]).returncode
        if rc != 0:
            failures.append(variant)
            print(f"!!! {variant} build failed (rc={rc}); continuing", file=sys.stderr)
    if failures:
        print(f"!!! {len(failures)} variant build(s) failed: {failures}", file=sys.stderr)
        print("refusing to start new cycle with mixed-build state", file=sys.stderr)
        return 1

    print("==> all builds OK, creating new cycle")
    rc = cmd_cycle_init(argparse.Namespace(name=None))
    if rc != 0:
        return rc

    print("==> starting new cycle")
    rc = cmd_cycle_start(argparse.Namespace(run=None, inst=None))
    return rc


# ----------------------------------------------------------------------
# CLI plumbing
# ----------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init")
    s = sub.add_parser("start"); s.add_argument("--inst", type=int)
    s = sub.add_parser("stop");  s.add_argument("--inst", type=int)
    sub.add_parser("status")
    sub.add_parser("crashes")
    sub.add_parser("mismatches")
    s = sub.add_parser("tail");     s.add_argument("inst", type=int)
    s = sub.add_parser("lldb");     s.add_argument("inst", type=int)
    s = sub.add_parser("snapshot"); s.add_argument("inst", type=int)
    s = sub.add_parser("purge");    s.add_argument("--inst", type=int)
    sub.add_parser("builds")
    s = sub.add_parser("build");    s.add_argument("variant")
    s = sub.add_parser("respawn");  s.add_argument("inst", type=int)

    cyc = sub.add_parser("cycle").add_subparsers(dest="subcmd", required=True)
    c = cyc.add_parser("init");    c.add_argument("--name")
    c = cyc.add_parser("start");   c.add_argument("--run", default=None); c.add_argument("--inst", type=int)
    c = cyc.add_parser("stop");    c.add_argument("--run", default=None); c.add_argument("--inst", type=int)
    c = cyc.add_parser("status");  c.add_argument("--run", default=None)
    c = cyc.add_parser("list")
    c = cyc.add_parser("purge");   c.add_argument("--run", default=None)
    r = sub.add_parser("refresh")
    r.add_argument("--merge-master", action="store_true")

    args = p.parse_args(argv)
    if args.cmd == "cycle":
        cycle_handler = {
            "init":   cmd_cycle_init,
            "start":  cmd_cycle_start,
            "stop":   cmd_cycle_stop,
            "status": cmd_cycle_status,
            "list":   cmd_cycle_list,
            "purge":  cmd_cycle_purge,
        }[args.subcmd]
        return cycle_handler(args)

    handler = {
        "init":       cmd_init,
        "start":      cmd_start,
        "stop":       cmd_stop,
        "status":     cmd_status,
        "crashes":    cmd_crashes,
        "mismatches": cmd_mismatches,
        "tail":       cmd_tail,
        "lldb":       cmd_lldb,
        "snapshot":   cmd_snapshot,
        "purge":      cmd_purge,
        "builds":     cmd_builds,
        "build":      cmd_build,
        "respawn":    cmd_respawn,
        "refresh":    cmd_refresh,
    }[args.cmd]
    return handler(args)


if __name__ == "__main__":
    sys.exit(main())
