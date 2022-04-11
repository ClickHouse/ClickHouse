from lightweight_delete.tests.steps import *
from lightweight_delete.requirements import *
from disk_level_encryption.tests.steps import create_table as create_table_encrypted
from disk_level_encryption.tests.steps import (
    add_encrypted_disk_configuration,
    insert_into_table,
    check_if_all_files_are_encrypted,
)


entries = {
    "storage_configuration": {
        "disks": [
            {"local": {"path": "/disk_local/"}},
            {
                "encrypted_local": {
                    "type": "encrypted",
                    "disk": "local",
                    "path": "encrypted/",
                }
            },
        ],
        "policies": {
            "local_encrypted": {
                "volumes": {"encrypted_disk": {"disk": "encrypted_local"}}
            }
        },
    }
}

expected_output = '{"Id":1,"Value":"hello"}\n{"Id":2,"Value":"there"}'


@TestScenario
@Requirements(RQ_SRS_023_ClickHouse_LightweightDelete_EncryptedDisk("1.0"))
def encrypted_disk(self, node=None):
    """Check that lightweight delete in table that stored on encrypted disk perform correctly."""
    disk_local = "/disk_local"
    disk_path = disk_local

    if node is None:
        node = self.context.node

    with Given("I create local disk folder on the server"):
        create_directory(path=disk_local)

    with And("I set up parameters"):
        entries_in_this_test = deepcopy(entries)
        entries_in_this_test["storage_configuration"]["disks"][1]["encrypted_local"][
            "key"
        ] = "firstfirstfirstf"

    with And("I add storage configuration that uses encrypted disk"):
        add_encrypted_disk_configuration(entries=entries_in_this_test, restart=True)

    table_name = f"table_{getuid()}"

    with And("I create a table that uses encrypted disk"):
        table_name = create_table_encrypted(policy="local_encrypted")

    with When("I insert data into the table"):
        for i in range(100):
            values = "(1, 'hello'),(2, 'there')"
            insert_into_table(name=table_name, values=values)

    with Then("I delete half of the table and check rows are deletes"):
        delete(table_name=table_name, condition="WHERE Value = 'there'", check=True)

    with Then("I check that the rest of the rows are not deleted"):
        r = node.query(f"SELECT count(*) FROM {table_name}")
        assert r.output == "100"

    with Then("I expect all files has ENC header"):
        check_if_all_files_are_encrypted(disk_path=disk_path)


@TestFeature
@Name("encrypted disk")
def feature(self, node="clickhouse1"):
    """Check that lightweight delete in table that stored on encrypted disk perform correctly."""
    self.context.node = self.context.cluster.node(node)
    self.context.table_engine = "MergeTree"
    for scenario in loads(current_module(), Scenario):
        scenario()
