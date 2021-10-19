import testflows.settings as settings
from testflows.core import *

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
        if test.terminating is True:
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
