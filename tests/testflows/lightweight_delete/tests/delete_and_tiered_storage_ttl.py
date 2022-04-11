from lightweight_delete.tests.steps import *
from lightweight_delete.requirements import *
from disk_level_encryption.tests.steps import create_table as create_table_with_ttl
from disk_level_encryption.tests.steps import (
    create_directories_multi_volume_policy,
    add_config_multi_volume_policy,
    insert_into_table,
)


@TestScenario
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_TTL("1.0"))
def delete_with_multi_volume_policy_using_ttl(
    self,
    number_of_volumes=2,
    numbers_of_disks=[1, 1],
    disks_types=[["local"], ["local"]],
    node=None,
):
    """Check that delete in table that uses tiered storage ttl perform correctly.
    I create parts on 2 volumes with 1 disk in each and delete some values in each volume.
    Using the following policy:
        volume0: disk local00
        volume1: disk local10
    """

    if node is None:
        node = self.context.node

    with Given("I create directories"):
        create_directories_multi_volume_policy(
            number_of_volumes=number_of_volumes, numbers_of_disks=numbers_of_disks
        )

    with And("I add configuration file"):
        add_config_multi_volume_policy(
            number_of_volumes=number_of_volumes,
            numbers_of_disks=numbers_of_disks,
            disks_types=disks_types,
            keys=[[None], [None]],
        )

    with And(
        "I create a table that uses tiered storage ttl",
        description="""
      TTL Date TO VOLUME 'volume0',
      Date + INTERVAL 1 HOUR TO VOLUME 'volume1',
      Date + INTERVAL 2 HOUR DELETE""",
    ):
        table_name = create_table_with_ttl(
            policy="local_encrypted", ttl=True, ttl_timeout=10
        )

    with When("I insert data into the table"):
        now = time.time()
        wait_expire = 31 * 60
        date = now
        for i in range(6):
            values = f"({i}, toDateTime({date-i*wait_expire}), '{i}{i}')"
            insert_into_table(name=table_name, values=values)

    r = node.query(
        f"SELECT Id, Date as Seconds, Value FROM {table_name} ORDER BY Id FORMAT JSONEachRow"
    )
    with Then("I compute expected output"):
        expected_output = (
            '{"Id":0,"Seconds":' + f"{int(date)}" + ',"Value":"00"}\n'
            '{"Id":1,"Seconds":' + f"{int(date-wait_expire)}" + ',"Value":"11"}\n'
            '{"Id":2,"Seconds":' + f"{int(date-2*wait_expire)}" + ',"Value":"22"}\n'
            '{"Id":3,"Seconds":' + f"{int(date-3*wait_expire)}" + ',"Value":"33"}'
        )

    with Then("I expect data is successfully inserted"):
        for attempt in retries(timeout=30, delay=5):
            with attempt:
                r = node.query(
                    f"SELECT Id, toInt32(Date) as Seconds, Value FROM {table_name} ORDER BY Id FORMAT JSONEachRow",
                    steps=False,
                )
                assert r.output == expected_output, error()

    with Then("I check part are on different disks"):
        r = node.query(
            f"SELECT COUNT(*) FROM (SELECT DISTINCT disk_name FROM system.parts WHERE table = '{table_name}')"
        )
        assert r.output == "2", error()

    with Then("I delete odd rows and check rows are deleted"):
        delete(table_name=table_name, condition="WHERE Id % 2 = 0", check=True)

    with And("I check rows are deleted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == "2", error()


@TestFeature
@Name("delete and tiered storage ttl")
def feature(self, node="clickhouse1"):
    """Check that delete in table that uses tiered storage ttl perform correctly."""
    self.context.node = self.context.cluster.node(node)
    self.context.table_engine = "MergeTree"
    for scenario in loads(current_module(), Scenario):
        scenario()
