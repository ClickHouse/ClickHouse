from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


@TestStep(When)
def kill_clickhouse_process(self, signal="SIGKILL", node=None):
    """Kill server using a signal."""
    if node is None:
        node = self.context.node

    with By("sending SIGKILL to clickhouse process"):
        pid = node.clickhouse_pid()
        node.command(f"kill -{signal} {pid}", exitcode=0, steps=False)

    with And("checking pid does not exist"):
        for attempt in retries(timeout=100, delay=1):
            with attempt:
                if node.command(f"ps {pid}", steps=False, no_checks=True).exitcode != 1:
                    fail("pid still alive")

    with And("deleting clickhouse server pid file"):
        node.command("rm -rf /tmp/clickhouse-server.pid", exitcode=0, steps=False)


@TestScenario
@Repeat(100, until="fail")
def hard_restart(self, signal="SIGKILL", node=None):
    """Check that clickhouse either completes DELETE operation or
    leaves data in the same state after a hard restart
    by using signal.
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_table(table_name=table_name)

    with When("I insert a lot of data into the table"):
        insert(
            table_name=table_name,
            partitions=100,
            parts_per_partition=1,
            block_size=10000,
        )

    with Then("I compute expected output"):
        output1 = node.query(
            f"SELECT count(*) FROM {table_name} WHERE NOT(id>0)"
        ).output
        output2 = node.query(f"SELECT count(*) FROM {table_name}").output

    with When("I delete table and kill clickhouse server process in parallel"):
        By(name="executing delete operation", test=delete, parallel=True)(
            table_name=table_name, condition="WHERE id > 0", no_checks=True
        )

        By(
            name="killing clickhouse process",
            test=kill_clickhouse_process,
            parallel=True,
        )(signal=signal)

    with And("I restart server"):
        node.start_clickhouse()

    with Then("I check that either rows are deleted completely or data is unchanged"):
        for attempt in retries(timeout=100, delay=1):
            with attempt:
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output in (output1, output2), error()


@TestFeature
@Name("hard restart")
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_HardRestarts("1.0"),
    RQ_SRS_023_ClickHouse_LightweightDelete_NonCorruptedServerState("1.0"),
)
def feature(self, node="clickhouse1"):
    """Check that clickhouse either finish the DELETE or return the system
    to before the DELETE started after a hard restart.
    """
    self.context.node = self.context.cluster.node(node)

    table_engine = "MergeTree"
    signal = "SIGKILL"

    with Feature(f"{signal}"):
        self.context.table_engine = table_engine
        for scenario in loads(current_module(), Scenario):
            scenario(signal=signal)
