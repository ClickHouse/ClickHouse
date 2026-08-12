"""Fault runner with start/stop interface (similar to MetricsSampler)."""
import copy
import random
import threading
import time

from keeper.faults.base import apply_step
from keeper.framework.core.settings import DEFAULT_FAULT_DURATION_S


class FaultRunner:
    """Runs faults one by one from a list with start/stop interface.

    Fault recovery is signalled via ctx["_fault_recovered"]: apply_step uses a
    ref-count (ctx["_fault_active"]) so that parallel faults (kind: parallel)
    only set _fault_recovered=True when the last concurrent sub-step finishes.
    When apply_step returns for a sequential fault, the fault has recovered. If
    you need to wait for recovery (e.g. before state gates or the next fault),
    use wait_until(lambda: ctx.get("_fault_recovered") is True, ...) instead of
    inferring from cluster state (e.g. count_leaders).
    """
    
    def __init__(self, nodes, leader, ctx, faults, duration_s, seed=None):
        self.nodes = nodes
        self.leader = leader
        self.ctx = ctx
        self.faults = faults or []
        self.duration_s = int(duration_s or 120)
        self.seed = seed
        
        self._th = None
        self._stop = False
    
    def _loop(self):
        """Process faults one by one until duration expires or stopped."""
        if not self.faults:
            return
        
        rnd = random.Random(self.seed) if self.seed is not None else random
        deadline = time.time() + self.duration_s
        
        while not self._stop and time.time() < deadline:
            remaining = deadline - time.time()

            fault = rnd.choice(self.faults)
            if not isinstance(fault, dict):
                continue

            budget = int(max(1, min(float(DEFAULT_FAULT_DURATION_S), float(remaining))))
            fault_step = copy.deepcopy(fault)
            if isinstance(fault_step, dict):
                fault_step.pop("duration_s", None)
                fault_step.pop("seconds", None)
                fault_step["duration_s"] = budget
                fault_step["seconds"] = budget
            
            # Execute fault; when apply_step returns, ctx["_fault_recovered"] is True
            t0 = time.time()
            try:
                apply_step(fault_step, self.nodes, self.leader, self.ctx)
            except Exception as e:
                print(f"[keeper][fault-runner] error executing fault {fault_step.get('kind', 'unknown')}: {e}")
            elapsed = time.time() - t0
            
            # If fault completed quickly (e.g. kill), sleep to fill the budget
            # This ensures total fault time ~= scenario duration
            slack = float(budget) - float(elapsed)
            if slack > 0 and not self._stop:
                time.sleep(min(slack, max(0.0, deadline - time.time())))
    
    def start(self):
        """Start fault execution in background thread."""
        if self._th:
            return
        self._stop = False
        self._th = threading.Thread(target=self._loop, daemon=True, name="faults")
        self._th.start()
    
    def stop(self):
        """Stop fault execution and wait for completion."""
        self._stop = True
        if self._th:
            # By the time stop() is called the fault loop should already be finishing.
            # Use a fixed ceiling rather than duration_s+30 to avoid silently blocking
            # for several extra minutes when a fault step hangs.
            self._th.join(timeout=90)
            if self._th.is_alive():
                # Log but do not raise: stop() is called during teardown, so turning a
                # slow-to-terminate fault thread into a hard AssertionError would mask
                # the actual test outcome.  The thread will exit on its own once the
                # in-flight fault step finishes (it checks _stop on every iteration).
                print("[keeper][fault-runner] WARNING: fault thread did not terminate within 90s after stop()")
            self._th = None
