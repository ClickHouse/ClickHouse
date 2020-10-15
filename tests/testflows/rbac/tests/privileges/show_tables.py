from contextlib import contextmanager

from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Table_ShowTables("1.0"),
)
def show_tables(self, node=None):
    """Check that a user is able to see a table in `SHOW TABLES` if and only if the user has privilege on that table,
    either granted directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    if node is None:
        node = self.context.node

    with user(node, f"{user_name}"):
        Scenario(test=show_tables_general, flags=TE,
            name="create with create view and select privilege granted directly")(grant_target_name=user_name, user_name=user_name)

    with user(node, f"{user_name}"), role(node, f"{role_name}"):
        with When("I grant the role to the user"):
            node.query(f"GRANT {role_name} TO {user_name}")
        Scenario(test=show_tables_general, flags=TE,
            name="create with create view and select privilege granted through a role")(grant_target_name=role_name, user_name=user_name)

@TestScenario
def show_tables_general(self, grant_target_name, user_name, node=None):
    table0_name = f"table0_{getuid()}"
    if node is None:
        node = self.context.node
    try:
        with Given("I have a table"):
            node.query(f"DROP TABLE IF EXISTS {table0_name}")
            node.query(f"CREATE TABLE {table0_name} (a String, b Int8, d Date) Engine = Memory")

        with Then("I check user does not see any tables"):
            output = node.query("SHOW TABLES", settings = [("user", f"{user_name}")]).output
            assert output == '', error()

        with When("I grant select privilege on the table"):
            node.query(f"GRANT SELECT(a) ON {table0_name} TO {grant_target_name}")
        with Then("I check the user does see a table"):
            output = node.query("SHOW TABLES", settings = [("user", f"{user_name}")]).output
            assert output == f'{table0_name}', error()

    finally:
        with Finally("I drop the table"):
            node.query(f"DROP TABLE IF EXISTS {table0_name}")

@TestFeature
@Name("show tables")
def feature(self, node="clickhouse1"):
    self.context.node = self.context.cluster.node(node)

    Scenario(run=show_tables, flags=TE)