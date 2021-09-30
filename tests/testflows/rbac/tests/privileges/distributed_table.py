from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestStep(Given)
def user(self, name, node=None):
    """Create a user with a given name.
    """
    if node is None:
        node = self.context.node

    try:
        with Given(f"I create user {name}"):
            node.query(f"CREATE USER OR REPLACE {name} ON CLUSTER one_shard_cluster")
        yield
    finally:
        with Finally(f"I delete user {name}"):
            node.query(f"DROP USER IF EXISTS {name} ON CLUSTER one_shard_cluster")

@TestStep(Given)
def role(self, name, node=None):
    """Create a role with a given name.
    """
    if node is None:
        node = self.context.node

    try:
        with Given(f"I create role {name}"):
            node.query(f"CREATE ROLE OR REPLACE {name} ON CLUSTER one_shard_cluster")
        yield
    finally:
        with Finally(f"I delete role {name}"):
            node.query(f"DROP ROLE IF EXISTS {name} ON CLUSTER one_shard_cluster")

@TestStep(Given)
def table(self, name, cluster=None, node=None):
    """Create a table with given name and on specified cluster, if specified.
    """
    if node is None:
        node = self.context.node
    try:
        if cluster:
            with Given(f"I create table {name}"):
                node.query(f"DROP TABLE IF EXISTS {name}")
                node.query(f"CREATE TABLE {name} ON CLUSTER {cluster} (a UInt64) ENGINE = Memory")
        else:
            with Given(f"I create table {name}"):
                node.query(f"DROP TABLE IF EXISTS {name}")
                node.query(f"CREATE TABLE {name} (a UInt64) ENGINE = Memory")
        yield
    finally:
        if cluster:
            with Finally(f"I delete table {name}"):
                node.query(f"DROP TABLE IF EXISTS {name} ON CLUSTER {cluster}")
        else:
            with Finally(f"I delete role {name}"):
                node.query(f"DROP ROLE IF EXISTS {name}")

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_DistributedTable_Create("1.0"),
)
def create(self):
    """Check the RBAC functionality of distributed table with CREATE.
    """
    create_scenarios=[
    create_without_privilege,
    create_with_privilege_granted_directly_or_via_role,
    create_with_all_privilege_granted_directly_or_via_role,
    ]

    for scenario in create_scenarios:
        Scenario(run=scenario, setup=instrument_clickhouse_server_log)

@TestScenario
def create_without_privilege(self, node=None):
    """Check that user is unable to create a distributed table without privileges.
    """
    user_name = f"user_{getuid()}"

    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    cluster = self.context.cluster_name

    if node is None:
        node = self.context.node

    with Given("I have a user"):
        user(name=user_name)

    with And("I have a table on a cluster"):
        table(name=table0_name, cluster=cluster)

    with When("I grant the user NONE privilege"):
        node.query(f"GRANT NONE TO {user_name}")

    with And("I grant the user USAGE privilege"):
        node.query(f"GRANT USAGE ON *.* TO {user_name}")

    with Then("I attempt to create the distributed table without privilege"):
        node.query(f"CREATE TABLE {table1_name} (a UInt64) ENGINE = Distributed(sharded_cluster, default, {table0_name}, rand())", settings = [("user", f"{user_name}")],
            exitcode=exitcode, message=message)

@TestScenario
def create_with_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to create a distributed table if and only if
    they have CREATE and REMOTE privileges granted either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a user"):
        user(name=user_name)

    Scenario(test=create_with_privilege,
        name="create with privilege granted directly")(grant_target_name=user_name, user_name=user_name)

    with Given("I have a user"):
        user(name=user_name)

    with And("I have a role"):
        role(name=role_name)

    with When("I grant the role to the user"):
        node.query(f"GRANT {role_name} TO {user_name} ON CLUSTER one_shard_cluster")

    Scenario(test=create_with_privilege,
        name="create with privilege granted through a role")(grant_target_name=role_name, user_name=user_name)

@TestOutline
def create_with_privilege(self, user_name, grant_target_name, node=None):
    """Grant CREATE and REMOTE privileges seperately, check the user is unable to create the table,
    grant both privileges and check the user is able is create the table.
    """
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    cluster = self.context.cluster_name

    if node is None:
        node = self.context.node

    try:
        with Given("I have a table on a cluster"):
            table(name=table0_name, cluster=cluster)

        with When("I grant create table privilege"):
            node.query(f"GRANT CREATE ON {table1_name} TO {grant_target_name}")

        with Then("I attempt to create the distributed table as the user"):
            node.query(f"CREATE TABLE {table1_name} (a UInt64) ENGINE = Distributed({cluster}, default, {table0_name}, rand())", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I revoke the create table privilege"):
            node.query(f"REVOKE CREATE TABLE ON {table1_name} FROM {grant_target_name}")

        with And("I grant remote privilege"):
            node.query(f"GRANT REMOTE ON *.* to {grant_target_name}")

        with Then("I attempt to create the distributed table as the user"):
            node.query(f"CREATE TABLE {table1_name} (a UInt64) ENGINE = Distributed({cluster}, default, {table0_name}, rand())", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant create table privilege"):
            node.query(f"GRANT CREATE ON {table1_name} TO {grant_target_name}")

        with Then("I attempt to create the distributed table as the user"):
            node.query(f"CREATE TABLE {table1_name} (a UInt64) ENGINE = Distributed({cluster}, default, {table0_name}, rand())", settings = [("user", f"{user_name}")])

    finally:
        with Finally("I drop the distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table1_name}")

@TestScenario
def create_with_all_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to create a distributed table if and only if
    they have ALL privilege granted either directly or through a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a user"):
        user(name=user_name)

    Scenario(test=create_with_privilege,
        name="create with privilege granted directly")(grant_target_name=user_name, user_name=user_name)

    with Given("I have a user"):
        user(name=user_name)

    with And("I have a role"):
        role(name=role_name)

    with When("I grant the role to the user"):
        node.query(f"GRANT {role_name} TO {user_name} ON CLUSTER one_shard_cluster")

    Scenario(test=create_with_privilege,
        name="create with privilege granted through a role")(grant_target_name=role_name, user_name=user_name)

@TestOutline
def create_with_privilege(self, user_name, grant_target_name, node=None):
    """Grant ALL privilege and check the user is able is create the table.
    """
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    cluster = self.context.cluster_name

    if node is None:
        node = self.context.node

    try:
        with Given("I have a table on a cluster"):
            table(name=table0_name, cluster=cluster)

        with When("I grant ALL privilege"):
            node.query(f"GRANT ALL ON *.* TO {grant_target_name}")

        with Then("I create the distributed table as the user"):
            node.query(f"CREATE TABLE {table1_name} (a UInt64) ENGINE = Distributed({cluster}, default, {table0_name}, rand())", settings = [("user", f"{user_name}")])

    finally:
        with Finally("I drop the distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table1_name}")

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_DistributedTable_Select("1.0"),
)
def select(self):
    """Check the RBAC functionality of distributed table with SELECT.
    """
    select_scenarios = [
        select_without_privilege,
        select_with_privilege_granted_directly_or_via_role,
        select_with_all_privilege_granted_directly_or_via_role
    ]

    for scenario in select_scenarios:
        Scenario(run=scenario, setup=instrument_clickhouse_server_log)

@TestScenario
def select_without_privilege(self, node=None):
    """Check that user is unable to select from a distributed table without privileges.
    """
    user_name = f"user_{getuid()}"
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    cluster = self.context.cluster_name

    if node is None:
        node = self.context.node

    try:
        with Given("I have a user"):
            user(name=user_name)

        with And("I have a table on a cluster"):
            table(name=table0_name, cluster=cluster)

        with And("I have a distributed table"):
            node.query(f"CREATE TABLE {table1_name} (a UInt64) ENGINE = Distributed({cluster}, default, {table0_name}, rand())")

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {user_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {user_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)
    finally:
        with Finally("I drop the distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table1_name}")

@TestScenario
def select_with_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to select from a distributed table if and only if
    they have SELECT privilege for the distributed table and the table it is using.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a user"):
        user(name=user_name)

    Scenario(test=select_with_privilege,
        name="select with privilege granted directly")(grant_target_name=user_name, user_name=user_name)

    with Given("I have a user"):
        user(name=user_name)

    with And("I have a role"):
        role(name=role_name)

    with When("I grant the role to the user"):
        node.query(f"GRANT {role_name} TO {user_name} ON CLUSTER one_shard_cluster")

    Scenario(test=select_with_privilege,
        name="select with privilege granted through a role")(grant_target_name=role_name, user_name=user_name)

@TestOutline
def select_with_privilege(self, user_name, grant_target_name, node=None):
    """Grant SELECT privilege to the user on each table seperately, check that user is unable to select from the distribute table,
    grant privilege on both tables and check the user is able to select.
    """
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    cluster = self.context.cluster_name

    if node is None:
        node = self.context.node

    try:
        with Given("I have a table on a cluster"):
            table(name=table0_name, cluster=cluster)

        with And("I have a distributed table"):
            node.query(f"CREATE TABLE {table1_name} (a UInt64) ENGINE = Distributed({cluster}, default, {table0_name}, rand())")

        with When("I grant select privilege on the distributed table"):
            node.query(f"GRANT SELECT ON {table1_name} TO {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I revoke select privilege on the distributed table"):
            node.query(f"REVOKE SELECT ON {table1_name} FROM {grant_target_name}")

        with And("I grant select privilege on the table used by the distributed table"):
            node.query(f"GRANT SELECT ON {table0_name} to {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant the user select privilege on the distributed table"):
            node.query(f"GRANT SELECT ON {table1_name} TO {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")])

    finally:
        with Finally("I drop the distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table1_name}")

@TestScenario
def select_with_all_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to select from a distributed table if and only if
    they have ALL privilege.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a user"):
        user(name=user_name)

    Scenario(test=select_with_privilege,
        name="select with privilege granted directly")(grant_target_name=user_name, user_name=user_name)

    with Given("I have a user"):
        user(name=user_name)

    with And("I have a role"):
        role(name=role_name)

    with When("I grant the role to the user"):
        node.query(f"GRANT {role_name} TO {user_name} ON CLUSTER one_shard_cluster")

    Scenario(test=select_with_privilege,
        name="select with privilege granted through a role")(grant_target_name=role_name, user_name=user_name)

@TestOutline
def select_with_privilege(self, user_name, grant_target_name, node=None):
    """Grant ALL and check the user is able to select from the distributed table.
    """
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    cluster = self.context.cluster_name

    if node is None:
        node = self.context.node

    try:
        with Given("I have a table on a cluster"):
            table(name=table0_name, cluster=cluster)

        with And("I have a distributed table"):
            node.query(f"CREATE TABLE {table1_name} (a UInt64) ENGINE = Distributed({cluster}, default, {table0_name}, rand())")

        with When("I grant ALL privilege"):
            node.query(f"GRANT ALL ON *.* TO {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")])

    finally:
        with Finally("I drop the distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table1_name}")

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_DistributedTable_Insert("1.0"),
)
def insert(self):
    """Check the RBAC functionality of distributed table with INSERT.
    """
    insert_scenarios = [
        insert_without_privilege,
        insert_with_privilege_granted_directly_or_via_role,
    ]

    for scenario in insert_scenarios:
        Scenario(run=scenario, setup=instrument_clickhouse_server_log)

@TestScenario
def insert_without_privilege(self, node=None):
    """Check that user is unable to insert into a distributed table without privileges.
    """
    user_name = f"user_{getuid()}"

    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    cluster = self.context.cluster_name

    if node is None:
        node = self.context.node

    try:
        with Given("I have a user"):
            user(name=user_name)

        with And("I have a table on a cluster"):
            table(name=table0_name, cluster=cluster)

        with And("I have a distributed table"):
            node.query(f"CREATE TABLE {table1_name} (a UInt64) ENGINE = Distributed({cluster}, default, {table0_name}, rand())")

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {user_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {user_name}")

        with Then("I attempt to insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table1_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)
    finally:
        with Finally("I drop the distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table1_name}")

@TestScenario
def insert_with_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to select from a distributed table if and only if
    they have INSERT privilege for the distributed table and the table it is using.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a user"):
        user(name=user_name)

    Scenario(test=insert_with_privilege,
        name="insert with privilege granted directly")(grant_target_name=user_name, user_name=user_name)

    with Given("I have a user"):
        user(name=user_name)

    with And("I have a role"):
        role(name=role_name)

    with When("I grant the role to the user"):
        node.query(f"GRANT {role_name} TO {user_name} ON CLUSTER one_shard_cluster")

    Scenario(test=insert_with_privilege,
        name="insert with privilege granted through a role")(grant_target_name=role_name, user_name=user_name)

@TestOutline
def insert_with_privilege(self, user_name, grant_target_name, node=None):
    """Grant INSERT privilege on each table seperately, check that the user is unable to insert into the distributed table,
    grant privilege on both tables and check the user is able to insert.
    """
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    cluster = self.context.cluster_name

    if node is None:
        node = self.context.node

    try:
        with Given("I have a table on a cluster"):
            table(name=table0_name, cluster=cluster)

        with And("I have a distributed table"):
            node.query(f"CREATE TABLE {table1_name} (a UInt64) ENGINE = Distributed({cluster}, default, {table0_name}, rand())")

        with When("I grant insert privilege on the distributed table"):
            node.query(f"GRANT INSERT ON {table1_name} TO {grant_target_name}")

        with Then("I attempt to insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table1_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I revoke the insert privilege on the distributed table"):
            node.query(f"REVOKE INSERT ON {table1_name} FROM {grant_target_name}")

        with And("I grant insert privilege on the table used by the distributed table"):
            node.query(f"GRANT INSERT ON {table0_name} to {grant_target_name}")

        with Then("I attempt to insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table1_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant insert privilege on the distributed table"):
            node.query(f"GRANT INSERT ON {table1_name} TO {grant_target_name}")

        with Then("I attempt to insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table1_name} VALUES (8888)", settings = [("user", f"{user_name}")])

        with When("I revoke ALL privileges"):
            node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

        with Then("I attempt to insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table1_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant ALL privilege"):
            node.query(f"GRANT ALL ON *.* To {grant_target_name}")

        with Then("I attempt to insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table1_name} VALUES (8888)", settings = [("user", f"{user_name}")])

    finally:
        with Finally("I drop the distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table1_name}")

@TestSuite
@Requirements(
    RQ_SRS_006_RBAC_DistributedTable_SpecialTables("1.0"),
)
def special_cases(self):
    """Check that the user is able to successfully execute queries on distributed tables using special tables,
    such as materialized view or another distributed table, if and only if they have the required privileges.
    """
    special_case_scenarios = [
        select_with_table_on_materialized_view_privilege_granted_directly_or_via_role,
        insert_with_table_on_materialized_view_privilege_granted_directly_or_via_role,
        select_with_table_on_source_table_of_materialized_view_privilege_granted_directly_or_via_role,
        insert_with_table_on_source_table_of_materialized_view_privilege_granted_directly_or_via_role,
        select_with_table_on_distributed_table_privilege_granted_directly_or_via_role,
        insert_with_table_on_distributed_table_privilege_granted_directly_or_via_role,
    ]

    for scenario in special_case_scenarios:
        Scenario(run=scenario, setup=instrument_clickhouse_server_log)

@TestScenario
def select_with_table_on_materialized_view_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to SELECT from a distributed table that uses a materialized view if and only if
    they have SELECT privilege on the distributed table and the materialized view it is built on.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a user"):
        user(name=user_name)

    Scenario(test=select_with_table_on_source_table_of_materialized_view,
        name="select with table on source table of materialized view, privilege granted directly")(grant_target_name=user_name, user_name=user_name)

    with Given("I have a user"):
        user(name=user_name)

    with And("I have a role"):
        role(name=role_name)

    with When("I grant the role to the user"):
        node.query(f"GRANT {role_name} TO {user_name} ON CLUSTER one_shard_cluster")

    Scenario(test=select_with_table_on_source_table_of_materialized_view,
        name="select with table on source table of materialized view, privilege granted through a role")(grant_target_name=role_name, user_name=user_name)

@TestOutline
def select_with_table_on_materialized_view(self, user_name, grant_target_name, node=None):
    """Grant SELECT on the distributed table and the materialized view seperately, check that the user is unable to select from the distributed table,
    grant privilege on both and check the user is able to select.
    """
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    view_name = f"view_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    cluster = self.context.cluster_name

    if node is None:
        node = self.context.node

    try:
        with Given("I have a table on a cluster"):
            table(name=table0_name, cluster=cluster)

        with And("I have a materialized view on a cluster"):
            node.query(f"CREATE MATERIALIZED VIEW {view_name} ON CLUSTER {cluster} ENGINE = Memory() AS SELECT * FROM {table0_name}")

        with And("I have a distributed table on the materialized view"):
            node.query(f"CREATE TABLE {table1_name} (a UInt64) ENGINE = Distributed({cluster}, default, {view_name}, rand())")

        with When("I grant the user NONE privilege"):
            node.query(f"GRANT NONE TO {grant_target_name}")

        with And("I grant the user USAGE privilege"):
            node.query(f"GRANT USAGE ON *.* TO {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant select privilege on the distributed table"):
            node.query(f"GRANT SELECT ON {table1_name} TO {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I revoke the select privilege on the distributed table"):
            node.query(f"REVOKE SELECT ON {table1_name} FROM {grant_target_name}")

        with And("I grant select privilege on the materialized view"):
            node.query(f"GRANT SELECT ON {view_name} to {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant select privilege on the distributed table"):
            node.query(f"GRANT SELECT ON {table1_name} TO {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")])

        with When("I revoke ALL privileges"):
            node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant ALL privilege"):
            node.query(f"GRANT ALL ON *.* To {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")])

    finally:
        with Finally("I drop the distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table1_name}")

        with And("I drop the view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")

@TestScenario
def select_with_table_on_source_table_of_materialized_view_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to SELECT from a distributed table that uses the source table of a materialized view if and only if
    they have SELECT privilege on the distributed table and the table it is using.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a user"):
        user(name=user_name)

    Scenario(test=select_with_table_on_source_table_of_materialized_view,
        name="select with table on source table of materialized view, privilege granted directly")(grant_target_name=user_name, user_name=user_name)

    with Given("I have a user"):
        user(name=user_name)

    with And("I have a role"):
        role(name=role_name)

    with When("I grant the role to the user"):
        node.query(f"GRANT {role_name} TO {user_name} ON CLUSTER one_shard_cluster")

    Scenario(test=select_with_table_on_source_table_of_materialized_view,
        name="select with table on source table of materialized view, privilege granted through a role")(grant_target_name=role_name, user_name=user_name)

@TestOutline
def select_with_table_on_source_table_of_materialized_view(self, user_name, grant_target_name, node=None):
    """Grant SELECT on the distributed table and the source table seperately, check that the user is unable to select from the distributed table,
    grant privilege on both and check the user is able to select.
    """
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    view_name = f"view_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    cluster = self.context.cluster_name

    if node is None:
        node = self.context.node

    try:
        with Given("I have a table on a cluster"):
            table(name=table0_name, cluster=cluster)

        with And("I have a materialized view on a cluster"):
            node.query(f"CREATE MATERIALIZED VIEW {view_name} ON CLUSTER {cluster} ENGINE = Memory() AS SELECT * FROM {table0_name}")

        with And("I have a distributed table using the source table of the materialized view"):
            node.query(f"CREATE TABLE {table1_name} (a UInt64) ENGINE = Distributed({cluster}, default, {table0_name}, rand())")

        with When("I grant select privilege on the distributed table"):
            node.query(f"GRANT SELECT ON {table1_name} TO {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I revoke select privilege on the distributed table"):
            node.query(f"REVOKE SELECT ON {table1_name} FROM {grant_target_name}")

        with And("I grant select privilege on the source table"):
            node.query(f"GRANT SELECT ON {table0_name} to {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant select privilege on the distributed table"):
            node.query(f"GRANT SELECT ON {table1_name} TO {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")])

        with When("I revoke ALL privileges"):
            node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant ALL privilege"):
            node.query(f"GRANT ALL ON *.* To {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")])

    finally:
        with Finally("I drop the distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table1_name}")

        with And("I drop the view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")

@TestScenario
def select_with_table_on_distributed_table_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to SELECT from a distributed table that uses another distributed table if and only if
    they have SELECT privilege on the distributed table, the distributed table it is using and the table that the second distributed table is using.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a user"):
        user(name=user_name)

    Scenario(test=select_with_table_on_distributed_table,
        name="select with table on distributed table, privilege granted directly")(grant_target_name=user_name, user_name=user_name)

    with Given("I have a user"):
        user(name=user_name)

    with And("I have a role"):
        role(name=role_name)

    with When("I grant the role to the user"):
        node.query(f"GRANT {role_name} TO {user_name} ON CLUSTER one_shard_cluster")

    Scenario(test=select_with_table_on_distributed_table,
        name="select with table on distributed table, privilege granted through a role")(grant_target_name=role_name, user_name=user_name)

@TestScenario
def select_with_table_on_distributed_table(self, user_name, grant_target_name, node=None):
    """Grant SELECT privilege seperately on the distributed table, the distributed table it is using and the table that the second distributed table is using,
    check that user is unable to select from the distributed table, grant privilege on all three and check the user is able to select.
    """
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table1_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    cluster = self.context.cluster_name

    if node is None:
        node = self.context.node

    try:
        with Given("I have a table on a cluster"):
            table(name=table0_name, cluster=cluster)

        with And("I have a distributed table on a cluster"):
            node.query(f"CREATE TABLE {table1_name} ON CLUSTER {cluster} (a UInt64) ENGINE = Distributed({cluster}, default, {table0_name}, rand())")

        with And("I have a distributed table on that distributed table"):
            node.query(f"CREATE TABLE {table2_name} (a UInt64) ENGINE = Distributed({cluster}, default, {table1_name}, rand())")

        for permutation in permutations(table_count=3):

            with grant_select_on_table(node, permutation, grant_target_name, table0_name, table1_name, table2_name) as tables_granted:

                with When(f"permutation={permutation}, tables granted = {tables_granted}"):

                    with Then("I attempt to select from the distributed table as the user"):
                        node.query(f"SELECT * FROM {table2_name}", settings = [("user", f"{user_name}")],
                            exitcode=exitcode, message=message)

        with When("I grant select on all tables"):

            with grant_select_on_table(node, max(permutations(table_count=3))+1, grant_target_name, table0_name, table1_name, table2_name):

                with Then("I attempt to select from the distributed table as the user"):
                        node.query(f"SELECT * FROM {table2_name}", settings = [("user", f"{user_name}")])

        with When("I revoke ALL privileges"):
            node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
                node.query(f"SELECT * FROM {table2_name}", settings = [("user", f"{user_name}")],
                    exitcode=exitcode, message=message)

        with When("I grant ALL privilege"):
            node.query(f"GRANT ALL ON *.* To {grant_target_name}")

        with Then("I attempt to select from the distributed table as the user"):
                node.query(f"SELECT * FROM {table2_name}", settings = [("user", f"{user_name}")])

    finally:
        with Finally("I drop the first distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table1_name}")

        with And("I drop the other distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table2_name}")

@TestScenario
def insert_with_table_on_materialized_view_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to INSERT into a distributed table that uses a materialized view if and only if
    they have INSERT privilege on the distributed table and the materialized view it is built on.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a user"):
        user(name=user_name)

    Scenario(test=insert_with_table_on_materialized_view,
        name="insert with table on materialized view, privilege granted directly")(grant_target_name=user_name, user_name=user_name)

    with Given("I have a user"):
        user(name=user_name)

    with And("I have a role"):
        role(name=role_name)

    with When("I grant the role to the user"):
        node.query(f"GRANT {role_name} TO {user_name} ON CLUSTER one_shard_cluster")

    Scenario(test=insert_with_table_on_materialized_view,
        name="insert with table on materialized view, privilege granted through a role")(grant_target_name=role_name, user_name=user_name)

@TestOutline
def insert_with_table_on_materialized_view(self, user_name, grant_target_name, node=None):
    """Grant INSERT on the distributed table and the materialized view seperately, check that the user is unable to insert into the distributed table,
    grant privilege on both and check the user is able to insert.
    """
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table2_{getuid()}"

    view_name = f"view_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    cluster = self.context.cluster_name

    if node is None:
        node = self.context.node

    try:
        with Given(f"I have a table on cluster {cluster}"):
            table(name=table0_name, cluster=cluster)

        with And("I have another table on the same cluster"):
            table(name=table1_name, cluster=cluster)

        with And("I have a materialized view on a cluster"):
            node.query(f"CREATE MATERIALIZED VIEW {view_name} ON CLUSTER {cluster} TO {table0_name} AS SELECT * FROM {table1_name}")

        with And("I have a distributed table on the materialized view"):
            node.query(f"CREATE TABLE {table2_name} (a UInt64) ENGINE = Distributed({cluster}, default, {view_name}, rand())")

        with When("I grant insert privilege on the distributed table"):
            node.query(f"GRANT INSERT ON {table2_name} TO {grant_target_name}")

        with Then("I attempt to insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table2_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I revoke the insert privilege on the distributed table"):
            node.query(f"REVOKE INSERT ON {table2_name} FROM {grant_target_name}")

        with And("I grant insert privilege on the view"):
            node.query(f"GRANT INSERT ON {view_name} to {grant_target_name}")

        with Then("I attempt insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table2_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant insert privilege on the distributed table"):
            node.query(f"GRANT INSERT ON {table2_name} TO {grant_target_name}")

        with Then("I attempt to insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table2_name} VALUES (8888)", settings = [("user", f"{user_name}")])

        with When("I revoke ALL privileges"):
            node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

        with Then("I attempt insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table2_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant ALL privilege"):
            node.query(f"GRANT ALL ON *.* To {grant_target_name}")

        with Then("I attempt to insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table2_name} VALUES (8888)", settings = [("user", f"{user_name}")])

    finally:
        with Finally("I drop the distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table2_name}")

        with And("I drop the view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")

@TestScenario
def insert_with_table_on_source_table_of_materialized_view_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to INSERT into a distributed table that uses the source table of a materialized view if and only if
    they have INSERT privilege on the distributed table and the table it is using.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a user"):
        user(name=user_name)

    Scenario(test=insert_with_table_on_source_table_of_materialized_view,
        name="insert with table on source table of materialized view, privilege granted directly")(grant_target_name=user_name, user_name=user_name)

    with Given("I have a user"):
        user(name=user_name)

    with And("I have a role"):
        role(name=role_name)

    with When("I grant the role to the user"):
        node.query(f"GRANT {role_name} TO {user_name} ON CLUSTER one_shard_cluster")

    Scenario(test=insert_with_table_on_source_table_of_materialized_view,
        name="insert with table on source table of materialized view, privilege granted through a role")(grant_target_name=role_name, user_name=user_name)

@TestOutline
def insert_with_table_on_source_table_of_materialized_view(self, user_name, grant_target_name, node=None):
    """Grant INSERT on the distributed table and the source table seperately, check that the user is unable to insert into the distributed table,
    grant privilege on both and check the user is able to insert.
    """
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"

    view_name = f"view_{getuid()}"
    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    cluster = self.context.cluster_name

    if node is None:
        node = self.context.node

    try:
        with Given("I have a table on a cluster"):
            table(name=table0_name, cluster=cluster)

        with And("I have a materialized view on a cluster"):
            node.query(f"CREATE MATERIALIZED VIEW {view_name} ON CLUSTER {cluster} ENGINE = Memory() AS SELECT * FROM {table0_name}")

        with And("I have a distributed table on the materialized view"):
            node.query(f"CREATE TABLE {table1_name} (a UInt64) ENGINE = Distributed({cluster}, default, {table0_name}, rand())")

        with When("I grant insert privilege on the distributed table"):
            node.query(f"GRANT INSERT ON {table1_name} TO {grant_target_name}")

        with Then("I attempt to insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table1_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I revoke insert privilege on the distributed table"):
            node.query(f"REVOKE INSERT ON {table1_name} FROM {grant_target_name}")

        with And("I grant insert privilege on the source table"):
            node.query(f"GRANT INSERT ON {table0_name} to {grant_target_name}")

        with Then("I attempt insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table1_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant insert privilege on the distributed table"):
            node.query(f"GRANT INSERT ON {table1_name} TO {grant_target_name}")

        with Then("I attempt to insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table1_name} VALUES (8888)", settings = [("user", f"{user_name}")])

        with When("I revoke ALL privileges"):
            node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

        with Then("I attempt insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table1_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant ALL privilege"):
            node.query(f"GRANT ALL ON *.* To {grant_target_name}")

        with Then("I attempt to insert into the distributed table as the user"):
            node.query(f"INSERT INTO {table1_name} VALUES (8888)", settings = [("user", f"{user_name}")])

    finally:
        with Finally("I drop the distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table1_name}")

        with And("I drop the view"):
            node.query(f"DROP VIEW IF EXISTS {view_name}")

@TestScenario
def insert_with_table_on_distributed_table_privilege_granted_directly_or_via_role(self, node=None):
    """Check that user is able to INSERT into a distributed table that uses another distributed table if and only if
    they have INSERT privilege on the distributed table, the distributed table it is using and the table that the second distributed table is using.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"

    if node is None:
        node = self.context.node

    with Given("I have a user"):
        user(name=user_name)

    Scenario(test=insert_with_table_on_distributed_table,
        name="insert with table on distributed table, privilege granted directly")(grant_target_name=user_name, user_name=user_name)

    with Given("I have a user"):
        user(name=user_name)

    with And("I have a role"):
        role(name=role_name)

    with When("I grant the role to the user"):
        node.query(f"GRANT {role_name} TO {user_name} ON CLUSTER one_shard_cluster")

    Scenario(test=insert_with_table_on_distributed_table,
        name="insert with table on distributed table, privilege granted through a role")(grant_target_name=role_name, user_name=user_name)

@TestOutline
def insert_with_table_on_distributed_table(self, user_name, grant_target_name, node=None):
    """Grant INSERT privilege seperately on the distributed table, the distributed table it is using and the table that the second distributed table is using,
    check that user is unable to insert into the distributed table, grant privilege on all three and check the user is able to insert.
    """
    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"
    table2_name = f"table1_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")
    cluster = self.context.cluster_name

    if node is None:
        node = self.context.node

    try:
        with Given("I have a table on a cluster"):
            table(name=table0_name, cluster=cluster)

        with And("I have a distributed table on a cluster"):
            node.query(f"CREATE TABLE {table1_name} ON CLUSTER {cluster} (a UInt64) ENGINE = Distributed({cluster}, default, {table0_name}, rand())")

        with And("I have a distributed table on that distributed table"):
            node.query(f"CREATE TABLE {table2_name} (a UInt64) ENGINE = Distributed({cluster}, default, {table1_name}, rand())")

        with When("I grant insert privilege on the outer distributed table"):
            node.query(f"GRANT INSERT ON {table2_name} TO {grant_target_name}")

        with Then("I attempt to insert into the outer distributed table as the user"):
            node.query(f"INSERT INTO {table2_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I revoke the insert privilege on the outer distributed table"):
            node.query(f"REVOKE INSERT ON {table2_name} FROM {grant_target_name}")

        with And("I grant insert privilege on the inner distributed table"):
            node.query(f"GRANT INSERT ON {table1_name} to {grant_target_name}")

        with Then("I attempt insert into the outer distributed table as the user"):
            node.query(f"INSERT INTO {table2_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I revoke the insert privilege on the inner distributed table"):
            node.query(f"REVOKE INSERT ON {table1_name} FROM {grant_target_name}")

        with And("I grant insert privilege on the innermost table"):
            node.query(f"GRANT INSERT ON {table0_name} to {grant_target_name}")

        with Then("I attempt insert into the outer distributed table as the user"):
            node.query(f"INSERT INTO {table2_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant insert privilege on the inner distributed table"):
            node.query(f"GRANT INSERT ON {table1_name} to {grant_target_name}")

        with Then("I attempt insert into the outer distributed table as the user"):
            node.query(f"INSERT INTO {table2_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant insert privilege on the outer distributed table"):
            node.query(f"GRANT INSERT ON {table2_name} to {grant_target_name}")

        with Then("I attempt insert into the outer distributed table as the user"):
            node.query(f"INSERT INTO {table2_name} VALUES (8888)", settings = [("user", f"{user_name}")])

        with When("I revoke ALL privileges"):
            node.query(f"REVOKE ALL ON *.* FROM {grant_target_name}")

        with Then("I attempt insert into the outer distributed table as the user"):
            node.query(f"INSERT INTO {table2_name} VALUES (8888)", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

        with When("I grant ALL privilege"):
            node.query(f"GRANT ALL ON *.* To {grant_target_name}")

        with Then("I attempt insert into the outer distributed table as the user"):
            node.query(f"INSERT INTO {table2_name} VALUES (8888)", settings = [("user", f"{user_name}")])

    finally:
        with Finally("I drop the outer distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table1_name}")

        with And("I drop the inner distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table2_name}")

@TestOutline(Scenario)
@Examples("cluster", [
    ("sharded_cluster12", Description("two node cluster with two shards where one shard is"
        " on clickhouse1 and another on clickhouse2 accessed from clickhouse1")),
    ("one_shard_cluster12", Description("two node cluster with only one shard and two replicas"
        " where one replica is on clickhouse1 and another on clickhouse2 accessed from clickhouse1")),
])
@Requirements(
    RQ_SRS_006_RBAC_DistributedTable_LocalUser("1.0")
)
def local_user(self, cluster, node=None):
    """Check that a user that exists locally and not present on the remote nodes
    is able to execute queries they have privileges to.
    """
    user_name = f"user_{getuid()}"

    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"

    if node is None:
        node = self.context.node

    try:
        with Given("I have a user on one node"):
            node.query(f"CREATE USER {user_name}")

        with And("I have a table on a cluster"):
            table(name=table0_name, cluster=cluster)

        with And("I have a distributed table"):
            node.query(f"CREATE TABLE {table1_name} (a UInt64) ENGINE = Distributed({cluster}, default, {table0_name}, rand())")

        with When("I grant select privilege on the distributed table"):
            node.query(f"GRANT SELECT ON {table1_name} TO {user_name}")

        with And("I grant select privilege on the other table"):
            node.query(f"GRANT SELECT ON {table0_name} TO {user_name}")

        with Then("I select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")])

        with When("I revoke ALL privileges"):
            node.query(f"REVOKE ALL ON *.* FROM {user_name}")

        with And("I grant ALL privilege"):
            node.query(f"GRANT ALL ON *.* To {user_name}")

        with Then("I select from the distributed table as the user"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")])

    finally:
        with Finally("I drop the user"):
            node.query(f"DROP USER IF EXISTS {user_name}")

        with And("I drop the distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table1_name}")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_DistributedTable_SameUserDifferentNodesDifferentPrivileges("1.0")
)
def multiple_node_user(self, node=None):
    """Check that a user that exists on multiple nodes with different privileges on each is able to execute queries
    if and only if they have the required privileges on the node they are executing the query from.
    """
    user_name = f"user_{getuid()}"

    table0_name = f"table0_{getuid()}"
    table1_name = f"table1_{getuid()}"

    exitcode, message = errors.not_enough_privileges(name=f"{user_name}")

    if node is None:
        node = self.context.node

    node2 = self.context.node2

    try:
        with Given("I have a user on a cluster with two nodes"):
            node.query(f"CREATE USER {user_name} ON CLUSTER sharded_cluster12")

        with And("I have a table on a cluster"):
            table(name=table0_name, cluster="sharded_cluster12")

        with And("I have a distributed table"):
            node.query(f"CREATE TABLE {table1_name} ON CLUSTER sharded_cluster12 (a UInt64) ENGINE = Distributed(sharded_cluster12, default, {table0_name}, rand())")

        with When("I grant select privilege on the distributed table on one node"):
            node.query(f"GRANT SELECT ON {table1_name} TO {user_name}")

        with And("I grant select privilege on the other table on one node"):
            node.query(f"GRANT SELECT ON {table0_name} TO {user_name}")

        with Then("I select from the distributed table on the node where the user has privileges"):
            node.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")])

        with And("I select from the distributed table on the node the user doesn't have privileges"):
            node2.query(f"SELECT * FROM {table1_name}", settings = [("user", f"{user_name}")],
                exitcode=exitcode, message=message)

    finally:
        with Finally("I drop the user"):
            node.query(f"DROP USER IF EXISTS {user_name}")

        with And("I drop the distributed table"):
            node.query(f"DROP TABLE IF EXISTS {table1_name}")

@TestOutline(Feature)
@Examples("cluster", [
    ("cluster1", Description("one node cluster with clickhouse1 accessed from clickhouse1")),
    ("sharded_cluster23", Description("two node cluster with two shards where one shard is"
        " on clickhouse2 and another on clickhouse3 accessed from clickhouse1")),
    ("sharded_cluster12", Description("two node cluster with two shards where one shard is"
        " on clickhouse1 and another on clickhouse2 accessed from clickhouse1")),
    ("one_shard_cluster12", Description("two node cluster with only one shard and two replicas"
        " where one replica is on clickhouse1 and another on clickhouse2 accessed from clickhouse1")),
])
def cluster_tests(self, cluster, node=None):
    """Scenarios to be run on different cluster configurations.
    """
    self.context.cluster_name = cluster

    tasks = []
    with Pool(3) as pool:
        try:
            for suite in loads(current_module(), Suite):
                run_scenario(pool, tasks, Suite(test=suite))
        finally:
            join(tasks)

@TestFeature
@Requirements(
    RQ_SRS_006_RBAC_Privileges_All("1.0"),
    RQ_SRS_006_RBAC_Privileges_None("1.0")
)
@Name("distributed table")
def feature(self, node="clickhouse1"):
    """Check the RBAC functionality of queries executed using distributed tables.
    """
    self.context.node = self.context.cluster.node(node)
    self.context.node2 = self.context.cluster.node("clickhouse2")
    self.context.node3 = self.context.cluster.node("clickhouse3")

    tasks = []
    with Pool(3) as pool:
        try:
            run_scenario(pool, tasks, Feature(test=cluster_tests))
            run_scenario(pool, tasks, Scenario(test=local_user))
            run_scenario(pool, tasks, Scenario(test=multiple_node_user))

        finally:
            join(tasks)

