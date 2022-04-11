from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


@TestScenario
@Requirements(
    RQ_SRS_023_ClickHouse_LightweightDelete_EfficientPhysicalDataRemoval("1.0")
)
def delete_and_check_size_of_the_table(self, node=None):
    """Check that ClickHouse support efficient removal of physical data from the tables that
    had rows deleted using the DELETE statement by deleting from the table and checking table
    size in storage and in system.parts table.
    """
    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I have a table"):
        create_partitioned_table(
            table_name=table_name, settings="SETTINGS old_parts_lifetime = 0"
        )

    with When(
        "I insert a lot of data into the table", description="10 parts random values"
    ):
        node.query(
            f"INSERT INTO {table_name} SELECT number % 10  AS id, rand64() AS x FROM numbers(1,10000000)"
        )

    with When("I force merges"):
        node.query(f"OPTIMIZE TABLE {table_name} FINAL")

    with And("I check size of the table in system.parts table"):
        size_in_system_parts_before_deletion = int(
            node.query(
                f"SELECT sum(bytes_on_disk) from system.parts "
                f"where table = '{table_name}' AND active=1"
            ).output
        )

    with And("I check size of the table on disk"):
        size_on_disk_before_deletion = int(
            node.command("du -s /var/lib/clickhouse/store | awk '{print $1}'").output
        )

    with When(f"I delete all rows from the table"):
        delete(table_name=table_name, condition="WHERE id > 0")

    with Then("I check that rows are deleted immediately"):
        for attempt in retries(timeout=100, delay=1):
            with attempt:
                size_on_disk_after_deletion = int(
                    node.command(
                        "du -s /var/lib/clickhouse/store | awk '{print $1}'"
                    ).output
                )
                size_in_system_parts_after_deletion = int(
                    node.query(
                        f"SELECT sum(bytes_on_disk) from system.parts "
                        f"where table = '{table_name}' AND active=1"
                    ).output
                )
                assert (
                    1.9 * size_on_disk_after_deletion < size_on_disk_before_deletion
                ), error()
                assert (
                    1.9 * size_in_system_parts_after_deletion
                    < size_in_system_parts_before_deletion
                ), error()
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output == "1000000", error()


@TestFeature
@Name("efficient physical data removal")
def feature(self, node="clickhouse1"):
    """Check that ClickHouse support efficient removal of physical data from the tables that
    had rows deleted using the DELETE statement.
    """
    self.context.node = self.context.cluster.node(node)
    self.context.table_engine = "MergeTree"

    for scenario in loads(current_module(), Scenario):
        scenario()
