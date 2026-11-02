#!/usr/bin/env python3
"""Terminal renderer for utils/continue-all-prs.sh.

Owns the terminal as the single writer, so there is no cross-process cursor
race (the reason the bash-only status bar corrupted output). Reads log lines on
stdin and prints them scrolling above a two-line status bar pinned to the bottom
of the screen via a DECSTBM scroll region. The status bar is rebuilt from the
counters file the bash script maintains:

  line rows-1 : needs-attention PR list
  line rows   : elapsed | round | ok/fail | cost | in/out/cache-in/cache-out

Environment:
  CAP_STATS  path to the counters file: "rounds ok fail in out cachein cacheout cost"
  CAP_NA     path to the needs-attention file (one PR number per line)
  CAP_START  epoch seconds when the run started (for elapsed time)
"""

import os
import shutil
import sys
import threading
import time

STATS = os.environ.get("CAP_STATS", "")
NAFILE = os.environ.get("CAP_NA", "")
try:
    START = float(os.environ.get("CAP_START") or time.time())
except ValueError:
    START = time.time()

RESERVED = 2  # bottom lines reserved for the status bar
_lock = threading.Lock()
_stop = threading.Event()
_state = {"rows": 24, "cols": 100}


def _w(s):
    try:
        sys.stdout.write(s)
        sys.stdout.flush()
    except (BrokenPipeError, ValueError, OSError):
        pass


def _humanize(n):
    n = float(n)
    if n >= 1e9:
        return "%.2fG" % (n / 1e9)
    if n >= 1e6:
        return "%.2fM" % (n / 1e6)
    if n >= 1e3:
        return "%.1fk" % (n / 1e3)
    return "%d" % int(n)


def _fmt_elapsed(sec):
    sec = int(max(0, sec))
    return "%02d:%02d:%02d" % (sec // 3600, (sec % 3600) // 60, sec % 60)


def _stats_line():
    r = s = f = i = o = ci = co = 0
    c = 0.0
    try:
        parts = open(STATS).read().split()
        if len(parts) >= 8:
            r, s, f, i, o, ci, co = (int(x) for x in parts[:7])
            c = float(parts[7])
    except Exception:
        pass
    return ("continue-all-prs | %s | round %d | ok %d fail %d | $%.2f | "
            "in %s out %s cache-in %s cache-out %s"
            % (_fmt_elapsed(time.time() - START), r, s, f, c,
               _humanize(i), _humanize(o), _humanize(ci), _humanize(co)))


def _na_line():
    try:
        nums = open(NAFILE).read().split()
    except Exception:
        nums = []
    if not nums:
        return "needs attention: none"
    return "needs attention (%d): %s" % (len(nums), " ".join(nums))


def _apply_region():
    """(Re)set the scroll region to rows 1..(rows-RESERVED); park cursor at its bottom."""
    bot = max(1, _state["rows"] - RESERVED)
    _w("\033[1;%dr\033[%d;1H" % (bot, bot))


def _refresh_size():
    sz = shutil.get_terminal_size((100, 24))
    if sz.lines != _state["rows"] or sz.columns != _state["cols"]:
        _state["rows"], _state["cols"] = sz.lines, sz.columns
        _apply_region()


def _print_msg(line):
    bot = max(1, _state["rows"] - RESERVED)
    line = line.rstrip("\n")[: _state["cols"]]
    # Go to the region's bottom line, clear it, write the message, then CR+LF to
    # scroll the region up by one. The two reserved lines below are untouched.
    _w("\033[%d;1H\033[2K%s\r\n" % (bot, line))


def _draw_bar():
    rows, cols = _state["rows"], _state["cols"]
    na = _na_line()[:cols]
    st = _stats_line()[:cols]
    _w("\033[%d;1H\033[2K\033[7m%s\033[0m" % (rows - 1, na))
    _w("\033[%d;1H\033[2K\033[7m%s\033[0m" % (rows, st))
    _w("\033[%d;1H" % max(1, rows - RESERVED))  # park cursor back in the region


def _bar_loop():
    while not _stop.wait(1.0):
        with _lock:
            _refresh_size()
            _draw_bar()


def main():
    sz = shutil.get_terminal_size((100, 24))
    _state["rows"], _state["cols"] = sz.lines, sz.columns
    with _lock:
        _apply_region()
        _draw_bar()
    threading.Thread(target=_bar_loop, daemon=True).start()
    try:
        while True:
            line = sys.stdin.readline()
            if not line:
                break
            with _lock:
                _refresh_size()
                _print_msg(line)
                _draw_bar()
    except KeyboardInterrupt:
        pass
    finally:
        _stop.set()
        with _lock:
            rows = _state["rows"]
            # reset the scroll region and clear the two reserved lines
            _w("\033[r\033[%d;1H\033[2K\033[%d;1H\033[2K\033[%d;1H"
               % (rows - 1, rows, rows))


if __name__ == "__main__":
    main()
