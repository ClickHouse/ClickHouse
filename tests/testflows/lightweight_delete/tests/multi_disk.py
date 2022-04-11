from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


entries = {
    "storage_configuration": {
        "disks": [],
        "policies": {"local": {"volumes": {"volume1": []}}},
    }
}


@TestScenario
def multi_disk_volume(self, number_of_disks=2, node=None):
    """Check ClickHouse supports policies with different number of
    disks in one volume, and with different disks configurations.
    """

    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I create directories"):
        create_directories_multi_disk_volume(number_of_disks=number_of_disks)

    with Given("I add configuration file"):
        add_config_multi_disk_volume(number_of_disks=number_of_disks)

    with And("I create a table that uses encrypted disk"):
        create_table(
            table_name=table_name, settings=f"SETTINGS storage_policy = 'local'"
        )

    with When("I insert data into the table"):
        insert(
            table_name=table_name,
            partitions=100,
            parts_per_partition=1,
            block_size=10000,
        )

    with And("I check table takes up more than one disk"):
        r = node.query(
            f"select count(*) from (SELECT DISTINCT disk_name FROM system.parts "
            f"WHERE table = '{table_name}' group by disk_name)"
        )
        assert r.output == "2", error()

    with Then("I expect data is successfully inserted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == "1000000", error()

    with Then("I perform delete operation"):
        delete(table_name=table_name, condition="WHERE x >= 50")

    with Then("I expect data is successfully deleted"):
        r = node.query(f"select count(*) from {table_name}")
        assert r.output == "5000", error()
        r = node.query(f"SELECT count(*) FROM {table_name} WHERE x >=50")
        assert r.output == "0", error


@TestFeature
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_MultidiskConfigurations("1.0"))
@Name("multi disk")
def feature(self, node="clickhouse1"):
    """Check ClickHouse supports policies with different number of
    disks in one volume, and with different disks configurations.
    """
    self.context.node = self.context.cluster.node(node)
    self.context.table_engine = "MergeTree"
    for scenario in loads(current_module(), Scenario):
        scenario()
