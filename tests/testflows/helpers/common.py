import testflows.settings as settings

from testflows.core import *

from multiprocessing.dummy import Pool
from multiprocessing import TimeoutError as PoolTaskTimeoutError

@TestStep(Given)
def instrument_clickhouse_server_log(self, node=None, test=None,
        clickhouse_server_log="/var/log/clickhouse-server/clickhouse-server.log"):
    """Instrument clickhouse-server.log for the current test (default)
    by adding start and end messages that include test name to log 
    of the specified node. If we are in the debug mode and the test
    fails then dump the messages from the log for this test.
    """
    if test is None:
    	test = current()

    if node is None:
        node = self.context.node

    with By("getting current log size"):
        cmd =  node.command(f"stat --format=%s {clickhouse_server_log}")
        start_logsize = cmd.output.split(" ")[0].strip()

    try:
        with And("adding test name start message to the clickhouse-server.log"):
            node.command(f"echo -e \"\\n-- start: {test.name} --\\n\" >> {clickhouse_server_log}")
        yield

    finally:
        if top().terminating is True:
            return

        with Finally("adding test name end message to the clickhouse-server.log", flags=TE):
           node.command(f"echo -e \"\\n-- end: {test.name} --\\n\" >> {clickhouse_server_log}")

        with And("getting current log size at the end of the test"):
           cmd =  node.command(f"stat --format=%s {clickhouse_server_log}")
           end_logsize = cmd.output.split(" ")[0].strip()

        with And("checking if test has failing result"):
            if settings.debug and not self.parent.result:
                with Then("dumping clickhouse-server.log for this test"):
                    node.command(f"tail -c +{start_logsize} {clickhouse_server_log}"
                    	f" | head -c {int(end_logsize) - int(start_logsize)}")

def join(tasks, timeout=None, polling=5):
    """Join all parallel tests.
    """
    exc = None

    for task in tasks:
        task._join_timeout = timeout

    while tasks:
        try:
            try:
                tasks[0].get(timeout=polling)
                tasks.pop(0)

            except PoolTaskTimeoutError as e:
                task = tasks.pop(0)
                if task._join_timeout is not None:
                    task._join_timeout -= polling
                    if task._join_timeout <= 0:
                        raise
                tasks.append(task)
                continue

        except KeyboardInterrupt as e:
            top().terminating = True
            raise

        except Exception as e:
            tasks.pop(0)
            if exc is None:
                exc = e
            top().terminating = True

    if exc is not None:
        raise exc

def start(pool, tasks, scenario, kwargs=None):
    """Start parallel test.
    """
    if kwargs is None:
        kwargs = {}

    task = pool.apply_async(scenario, [], kwargs)
    tasks.append(task)

    return task

def run_scenario(pool, tasks, scenario, kwargs=None):
    if kwargs is None:
        kwargs = {}

    _top = top()
    def _scenario_wrapper(**kwargs):
        if _top.terminating:
            return
        return scenario(**kwargs)

    if current().context.parallel:
        start(pool, tasks, _scenario_wrapper, kwargs)
    else:
        scenario(**kwargs)
