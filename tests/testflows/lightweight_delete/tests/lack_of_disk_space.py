from lightweight_delete.requirements import *
from lightweight_delete.tests.steps import *


entries = {
    "storage_configuration": {
        "disks": [
            {"jbod1": {"path": "/jbod1/", "keep_free_space_bytes": "1024"}},
        ],
        "policies": {"small_disk": {"volumes": {"volume1": {"disk": "jbod1"}}}},
    }
}


entries_for_tiered_storage = {
    "storage_configuration": {
        "disks": [
            {"jbod1": {"path": "/jbod1/"}},
            {"jbod2": {"path": "/jbod2/"}},
        ],
        "policies": {
            "small_disk": {
                "volumes": {"volume1": {"disk": "jbod1"}, "volume2": {"disk": "jbod2"}}
            }
        },
    }
}


@TestScenario
def lack_of_disk_space(self, node=None):
    """Check that clickhouse reserve space to avoid breaking in the middle.
    I fill the disk and delete insert the data in loop until the error and
    check the state of the table in the end when table stored on the disk.
    """

    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I add configuration file"):
        add_disk_configuration(entries=entries, restart=True)

    with And("I create a table that uses small disk"):
        create_table(
            table_name=table_name, settings=f"SETTINGS storage_policy = 'small_disk'"
        )

    with When("I insert data into the table"):
        insert(
            table_name=table_name,
            partitions=5,
            parts_per_partition=1,
            block_size=1450000,
        )

    with And("I check table takes up more than one disk"):
        r = node.query(
            f"SELECT DISTINCT disk_name FROM system.parts "
            f"WHERE table = '{table_name}'"
        )

        assert r.output == "jbod1", error()

        r = node.query(
            f"SELECT sum(bytes_on_disk)/1024/1024 FROM system.parts "
            f"WHERE table = '{table_name}' and active = 1"
        )

    with Then("I expect data is successfully inserted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == "7250000", error()

    with Then("I perform delete insert operation in loop"):
        with Then("I perform delete operation"):
            i = 0
            while i < 100:
                r = delete(
                    table_name=table_name, condition=f"WHERE id = 1", no_checks=True
                )
                if r.exitcode != 0:
                    i = 100
                else:
                    r = insert(
                        table_name=table_name,
                        partitions=1,
                        parts_per_partition=1,
                        block_size=1450000,
                        no_checks=True,
                    )
                    if r.exitcode != 0:
                        i = 100
                i += 1
        assert r.exitcode != 0, error()

    with Then("I check table state"):
        for attempt in retries(timeout=100, delay=1):
            with attempt:
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output in ("7250000", "5800000"), error()
                r = node.query(f"SELECT count(*) FROM {table_name} WHERE id=0")
                assert r.output in ("0", "1450000"), error()


@TestScenario
def lack_of_disk_space_tiered_storage(self, node=None):
    """Check that clickhouse reserve space to avoid breaking in the middle.
    I fill the disk and delete insert the data in loop until the error and
    check the state of the table in the end when table stored on two disks.
    """

    if node is None:
        node = self.context.node

    table_name = f"table_{getuid()}"

    with Given("I add configuration file"):
        add_disk_configuration(entries=entries_for_tiered_storage, restart=True)

    with And("I create a table that uses small disk"):
        create_table(
            table_name=table_name, settings=f"SETTINGS storage_policy = 'small_disk'"
        )

    with When("I insert data into the table"):
        insert(
            table_name=table_name,
            partitions=10,
            parts_per_partition=1,
            block_size=1450000,
        )

    with And("I check table takes up more than one disk"):
        r = node.query(
            f"SELECT DISTINCT disk_name FROM system.parts "
            f"WHERE table = '{table_name}'"
        )

        assert r.output == "jbod1\njbod2", error()

        r = node.query(
            f"SELECT sum(bytes_on_disk)/1024/1024 FROM system.parts "
            f"WHERE table = '{table_name}' and active = 1"
        )

    with Then("I expect data is successfully inserted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == "14500000", error()

    with Then("I perform delete insert operations in loop"):
        with Then("I perform delete operation"):
            i = 0
            while i < 100:
                r = delete(
                    table_name=table_name, condition=f"WHERE id = 1", no_checks=True
                )
                if r.exitcode != 0:
                    i = 100
                else:
                    r = insert(
                        table_name=table_name,
                        partitions=1,
                        parts_per_partition=1,
                        block_size=1450000,
                        no_checks=True,
                        settings=[("insert_distributed_sync", "1")],
                    )
                    if r.exitcode != 0:
                        i = 100
                i += 1
        assert r.exitcode != 0, error()

    with Then("I check table state"):
        for attempt in retries(timeout=100, delay=1):
            with attempt:
                r = node.query(f"SELECT count(*) FROM {table_name}")
                assert r.output in ("14500000", "13050000"), error()
                r = node.query(f"SELECT count(*) FROM {table_name} WHERE id=0")
                assert r.output in ("0", "1450000"), error()


@TestFeature
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_LackOfDiskSpace("1.0"))
@Name("lack of disk space")
def feature(self, node="clickhouse1"):
    """Check that clickhouse reserve space to avoid breaking in the middle."""
    self.context.node = self.context.cluster.node(node)
    self.context.table_engine = "MergeTree"
    for scenario in loads(current_module(), Scenario):
        scenario()
