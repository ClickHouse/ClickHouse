from contextlib import contextmanager
import json

from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
import rbac.tests.errors as errors

table_types = {
    "MergeTree": "CREATE TABLE {name} (d DATE, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = MergeTree(d, (a, b), 111)",
    "ReplacingMergeTree": "CREATE TABLE {name} (d DATE, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = ReplacingMergeTree(d, (a, b), 111)",
    "SummingMergeTree": "CREATE TABLE {name} (d DATE, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = SummingMergeTree(d, (a, b), 111)",
    "AggregatingMergeTree": "CREATE TABLE {name} (d DATE, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = AggregatingMergeTree(d, (a, b), 111)",
    "CollapsingMergeTree": "CREATE TABLE {name} (d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = CollapsingMergeTree(d, (a, b), 111, y);",
    "VersionedCollapsingMergeTree": "CREATE TABLE {name} (d Date, a String, b UInt8, x String, y Int8, z UInt32, version UInt64, sign Int8, INDEX a (b * y, d) TYPE minmax GRANULARITY 3) ENGINE = VersionedCollapsingMergeTree(sign, version) ORDER BY tuple()",
    "GraphiteMergeTree": "CREATE TABLE {name} (key UInt32, Path String, Time DateTime, d Date, a String, b UInt8, x String, y Int8, z UInt32, Value Float64, Version UInt32, col UInt64, INDEX a (key * Value, Time) TYPE minmax GRANULARITY 3) ENGINE = GraphiteMergeTree('graphite_rollup_example') ORDER BY tuple()"
}

table_requirements ={
    "MergeTree": RQ_SRS_006_RBAC_Privileges_Insert_MergeTree("1.0"),
    "ReplacingMergeTree": RQ_SRS_006_RBAC_Privileges_Insert_ReplacingMergeTree("1.0"),
    "SummingMergeTree": RQ_SRS_006_RBAC_Privileges_Insert_SummingMergeTree("1.0"),
    "AggregatingMergeTree": RQ_SRS_006_RBAC_Privileges_Insert_AggregatingMergeTree("1.0"),
    "CollapsingMergeTree": RQ_SRS_006_RBAC_Privileges_Insert_CollapsingMergeTree("1.0"),
    "VersionedCollapsingMergeTree": RQ_SRS_006_RBAC_Privileges_Insert_VersionedCollapsingMergeTree("1.0"),
    "GraphiteMergeTree": RQ_SRS_006_RBAC_Privileges_Insert_GraphiteMergeTree("1.0"),
}

@contextmanager
def table(node, name, table_type="MergeTree"):
    try:
        with Given(f"I have a {table_type} table"):
            node.query(table_types[table_type].format(name=name))
        yield
    finally:
        with Finally("I drop the table"):
            node.query(f"DROP TABLE IF EXISTS {name}")

@contextmanager
def user(node, name):
    try:
        names = name.split(",")
        for i in names:
            with Given("I have a user"):
                node.query(f"CREATE USER OR REPLACE {i}")
        yield
    finally:
        for i in names:
            with Finally("I drop the user"):
                node.query(f"DROP USER IF EXISTS {name}")

@contextmanager
def role(node, role):
    try:
        roles = role.split(",")
        for j in roles:
            with Given("I have a role"):
                node.query(f"CREATE ROLE OR REPLACE {j}")
        yield
    finally:
        for j in roles:
            with Finally("I drop the role"):
                node.query(f"DROP ROLE IF EXISTS {role}")

def input_output_equality_check(node, input_columns, input_data):
    data_list = [x.strip("'") for x in input_data.split(",")]
    input_dict = dict(zip(input_columns.split(","), data_list))
    output_dict = json.loads(node.query(f"select {input_columns} from merge_tree format JSONEachRow").output)
    output_dict = {k:str(v) for (k,v) in output_dict.items()}
    return input_dict == output_dict

@TestScenario
def without_privilege(self, table_type, node=None):
    """Check that user without insert privilege on a table is not able to insert on that table.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"):
            with When("I run INSERT without privilege"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')", settings = [("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_Grant("1.0"),
)
def user_with_privilege(self, table_type, node=None):
    """Check that user can insert into a table on which they have insert privilege and the inserted data is correct.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"):
            with When("I grant privilege"):
                node.query("GRANT INSERT ON merge_tree TO user0")
            with And("I use INSERT"):
                node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')", settings=[("user","user0")])
            with Then("I check the insert functioned"):
                output = node.query("SELECT d FROM merge_tree FORMAT JSONEachRow").output
                assert output == '{"d":"2020-01-01"}', error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_Revoke("1.0"),
)
def user_with_revoked_privilege(self, table_type, node=None):
    """Check that user is unable to insert into a table after insert privilege on that table has been revoked from user.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"):
            with When("I grant privilege"):
                node.query("GRANT INSERT ON merge_tree TO user0")
            with And("I revoke privilege"):
                node.query("REVOKE INSERT ON merge_tree FROM user0")
            with And("I use INSERT"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')",
                            settings=[("user","user0")], exitcode=exitcode, message=message)

@TestScenario
def user_with_privilege_on_columns(self, table_type):
    Scenario(run=user_column_privileges,
        examples=Examples("grant_columns revoke_columns insert_columns_fail insert_columns_pass data_fail data_pass table_type",
            [tuple(list(row)+[table_type]) for row in user_column_privileges.examples]))


@TestOutline(Scenario)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_Column("1.0"),
)
@Examples("grant_columns revoke_columns insert_columns_fail insert_columns_pass data_fail data_pass", [
    ("d", "d", "x", "d", '\'woo\'', '\'2020-01-01\''),
    ("d,a", "d", "x", "d", '\'woo\'', '\'2020-01-01\''),
    ("d,a,b", "d,a,b", "x", "d,b", '\'woo\'', '\'2020-01-01\',9'),
    ("d,a,b", "b", "y", "d,a,b", '9', '\'2020-01-01\',\'woo\',9')
])
def user_column_privileges(self, grant_columns, insert_columns_pass, data_fail, data_pass, table_type,
                                            revoke_columns=None, insert_columns_fail=None, node=None):
    """Check that user is able to insert on granted columns
    and unable to insert on not granted or revoked columns.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"):
            with When("I grant insert privilege"):
                node.query(f"GRANT INSERT({grant_columns}) ON merge_tree TO user0")
            if insert_columns_fail is not None:
                with And("I insert into not granted column"):
                    exitcode, message = errors.not_enough_privileges(name="user0")
                    node.query(f"INSERT INTO merge_tree ({insert_columns_fail}) VALUES ({data_fail})",
                                settings=[("user","user0")], exitcode=exitcode, message=message)
            with And("I insert into granted column"):
                node.query(f"INSERT INTO merge_tree ({insert_columns_pass}) VALUES ({data_pass})",
                            settings=[("user","user0")])
            with Then("I check the insert functioned"):
                input_equals_output = input_output_equality_check(node, insert_columns_pass, data_pass)
                assert input_equals_output, error()
            if revoke_columns is not None:
                with When("I revoke insert privilege from columns"):
                    node.query(f"REVOKE INSERT({revoke_columns}) ON merge_tree FROM user0")
                with And("I insert into revoked columns"):
                    exitcode, message = errors.not_enough_privileges(name="user0")
                    node.query(f"INSERT INTO merge_tree ({insert_columns_pass}) VALUES ({data_pass})",
                                settings=[("user","user0")], exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_Grant("1.0"),
)
def role_with_privilege(self, table_type, node=None):
    """Check that user can insert into a table after it is granted a role that
    has the insert privilege for that table.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), role(node, "role0"):
            with When("I grant insert privilege to a role"):
                node.query("GRANT INSERT ON merge_tree TO role0")
            with And("I grant role to the user"):
                node.query("GRANT role0 TO user0")
            with And("I insert into a table"):
                node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')", settings=[("user","user0")])
            with Then("I check that I can read inserted data"):
                output = node.query("SELECT d FROM merge_tree FORMAT JSONEachRow").output
                assert output == '{"d":"2020-01-01"}', error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_Revoke("1.0"),
)
def role_with_revoked_privilege(self, table_type, node=None):
    """Check that user with a role that has insert privilege on a table
    is unable to insert into that table after insert privilege
    has been revoked from the role.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), role(node, "role0"):
            with When("I grant privilege to a role"):
                node.query("GRANT INSERT ON merge_tree TO role0")
            with And("I grant the role to a user"):
                node.query("GRANT role0 TO user0")
            with And("I revoke privilege from the role"):
                node.query("REVOKE INSERT ON merge_tree FROM role0")
            with And("I insert into the table"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')",
                            settings=[("user","user0")], exitcode=exitcode, message=message)

@TestScenario
def user_with_revoked_role(self, table_type, node=None):
    """Check that user with a role that has insert privilege on a table
    is unable to insert into that table after the role with insert
    privilege has been revoked from the user.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), role(node, "role0"):
            with When("I grant privilege to a role"):
                node.query("GRANT INSERT ON merge_tree TO role0")
            with And("I grant the role to a user"):
                node.query("GRANT role0 TO user0")
            with And("I revoke the role from the user"):
                node.query("REVOKE role0 FROM user0")
            with And("I insert into the table"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')",
                            settings=[("user","user0")], exitcode=exitcode, message=message)

@TestScenario
def role_with_privilege_on_columns(self, table_type):
    Scenario(run=role_column_privileges,
        examples=Examples("grant_columns revoke_columns insert_columns_fail insert_columns_pass data_fail data_pass table_type",
            [tuple(list(row)+[table_type]) for row in role_column_privileges.examples]))

@TestOutline(Scenario)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_Column("1.0"),
)
@Examples("grant_columns revoke_columns insert_columns_fail insert_columns_pass data_fail data_pass", [
    ("d", "d", "x", "d", '\'woo\'', '\'2020-01-01\''),
    ("d,a", "d", "x", "d", '\'woo\'', '\'2020-01-01\''),
    ("d,a,b", "d,a,b", "x", "d,b", '\'woo\'', '\'2020-01-01\',9'),
    ("d,a,b", "b", "y", "d,a,b", '9', '\'2020-01-01\',\'woo\',9')
])
def role_column_privileges(self, grant_columns, insert_columns_pass, data_fail, data_pass,
                            table_type, revoke_columns=None, insert_columns_fail=None, node=None):
    """Check that user with a role is able to insert on granted columns and unable
    to insert on not granted or revoked columns.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), role(node, "role0"):
                with When("I grant insert privilege"):
                    node.query(f"GRANT INSERT({grant_columns}) ON merge_tree TO role0")
                with And("I grant the role to a user"):
                    node.query("GRANT role0 TO user0")
                if insert_columns_fail is not None:
                    with And("I insert into not granted column"):
                        exitcode, message = errors.not_enough_privileges(name="user0")
                        node.query(f"INSERT INTO merge_tree ({insert_columns_fail}) VALUES ({data_fail})",
                                    settings=[("user","user0")], exitcode=exitcode, message=message)
                with And("I insert into granted column"):
                    node.query(f"INSERT INTO merge_tree ({insert_columns_pass}) VALUES ({data_pass})",
                                settings=[("user","user0")])
                with Then("I check the insert functioned"):
                    input_equals_output = input_output_equality_check(node, insert_columns_pass, data_pass)
                    assert input_equals_output, error()
                if revoke_columns is not None:
                    with When("I revoke insert privilege from columns"):
                        node.query(f"REVOKE INSERT({revoke_columns}) ON merge_tree FROM role0")
                    with And("I insert into revoked columns"):
                        exitcode, message = errors.not_enough_privileges(name="user0")
                        node.query(f"INSERT INTO merge_tree ({insert_columns_pass}) VALUES ({data_pass})",
                                    settings=[("user","user0")], exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_Cluster("1.0"),
)
def user_with_privilege_on_cluster(self, table_type, node=None):
    """Check that user is able to insert on a table with
    privilege granted on a cluster.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        try:
            with Given("I have a user on a cluster"):
                node.query("CREATE USER OR REPLACE user0 ON CLUSTER sharded_cluster")
            with When("I grant insert privilege on a cluster without the node with the table"):
                node.query("GRANT ON CLUSTER cluster23 INSERT ON merge_tree TO user0")
            with And("I insert into the table expecting a fail"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')", settings=[("user","user0")],
                            exitcode=exitcode, message=message)
            with And("I grant insert privilege on cluster including all nodes"):
                node.query("GRANT ON CLUSTER sharded_cluster INSERT ON merge_tree TO user0")
            with And("I revoke insert privilege on cluster without the table node"):
                node.query("REVOKE ON CLUSTER cluster23 INSERT ON merge_tree FROM user0")
            with And("I insert into the table"):
                node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')", settings=[("user","user0")])
            with Then("I check that I can read inserted data"):
                output = node.query("SELECT d FROM merge_tree FORMAT JSONEachRow").output
                assert output == '{"d":"2020-01-01"}', error()
        finally:
            with Finally("I drop the user"):
                node.query("DROP USER user0 ON CLUSTER sharded_cluster")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_GrantOption_Grant("1.0"),
)
def user_with_privilege_from_user_with_grant_option(self, table_type, node=None):
    """Check that user is able to insert on a table when granted privilege
    from another user with grant option.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0,user1"):
            with When("I grant privilege with grant option to user"):
                node.query("GRANT INSERT(d) ON merge_tree TO user0 WITH GRANT OPTION")
            with And("I grant privilege on a column I dont have permission on"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("GRANT INSERT(b) ON merge_tree TO user1", settings=[("user","user0")],
                            exitcode=exitcode, message=message)
            with And("I grant privilege to another user via grant option"):
                node.query("GRANT INSERT(d) ON merge_tree TO user1", settings=[("user","user0")])
            with And("I insert into a table"):
                node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')", settings=[("user","user1")])
            with Then("I check that I can read inserted data"):
                output = node.query("SELECT d FROM merge_tree FORMAT JSONEachRow").output
                assert output == '{"d":"2020-01-01"}', error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_GrantOption_Grant("1.0"),
)
def role_with_privilege_from_user_with_grant_option(self, table_type, node=None):
    """Check that user is able to insert on a table when granted a role with
    insert privilege that was granted by another user with grant option.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0,user1"), role(node, "role0"):
            with When("I grant privilege with grant option to user"):
                node.query("GRANT INSERT(d) ON merge_tree TO user0 WITH GRANT OPTION")
            with And("I grant privilege on a column I dont have permission on"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("GRANT INSERT(b) ON merge_tree TO role0", settings=[("user","user0")],
                            exitcode=exitcode, message=message)
            with And("I grant privilege to a role via grant option"):
                node.query("GRANT INSERT(d) ON merge_tree TO role0", settings=[("user","user0")])
            with And("I grant the role to another user"):
                node.query("GRANT role0 TO user1")
            with And("I insert into a table"):
                node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')", settings=[("user","user1")])
            with Then("I check that I can read inserted data"):
                output = node.query("SELECT d FROM merge_tree FORMAT JSONEachRow").output
                assert output == '{"d":"2020-01-01"}', error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_GrantOption_Grant("1.0"),
)
def user_with_privilege_from_role_with_grant_option(self, table_type, node=None):
    """Check that user is able to insert on a table when granted privilege from a role with grant option
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0,user1"), role(node, "role0"):
            with When("I grant privilege with grant option to a role"):
                node.query("GRANT INSERT(d) ON merge_tree TO role0 WITH GRANT OPTION")
            with When("I grant role to a user"):
                node.query("GRANT role0 TO user0")
            with And("I grant privilege on a column I dont have permission on"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("GRANT INSERT(b) ON merge_tree TO user1", settings=[("user","user0")],
                            exitcode=exitcode, message=message)
            with And("I grant privilege to a user via grant option"):
                node.query("GRANT INSERT(d) ON merge_tree TO user1", settings=[("user","user0")])
            with And("I insert into a table"):
                node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')", settings=[("user","user1")])
            with Then("I check that I can read inserted data"):
                output = node.query("SELECT d FROM merge_tree FORMAT JSONEachRow").output
                assert output == '{"d":"2020-01-01"}', error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_GrantOption_Grant("1.0"),
)
def role_with_privilege_from_role_with_grant_option(self, table_type, node=None):
    """Check that a user is able to insert on a table with a role that was granted privilege
    by another role with grant option
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0,user1"), role(node, "role0,role1"):
            with When("I grant privilege with grant option to role"):
                node.query("GRANT INSERT(d) ON merge_tree TO role0 WITH GRANT OPTION")
            with And("I grant the role to a user"):
                node.query("GRANT role0 TO user0")
            with And("I grant privilege on a column I dont have permission on"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("GRANT INSERT(b) ON merge_tree TO role1", settings=[("user","user0")],
                            exitcode=exitcode, message=message)
            with And("I grant privilege to another role via grant option"):
                node.query("GRANT INSERT(d) ON merge_tree TO role1", settings=[("user","user0")])
            with And("I grant the second role to another user"):
                node.query("GRANT role1 TO user1")
            with And("I insert into a table"):
                node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')", settings=[("user","user1")])
            with Then("I check that I can read inserted data"):
                output = node.query("SELECT d FROM merge_tree FORMAT JSONEachRow").output
                assert output == '{"d":"2020-01-01"}', error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_GrantOption_Revoke("1.0"),
)
def revoke_privilege_from_user_via_user_with_grant_option(self, table_type, node=None):
    """Check that user is unable to revoke a column they don't have access to from a user.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0,user1"):
            with When("I grant privilege with grant option to user"):
                node.query("GRANT INSERT(d) ON merge_tree TO user0 WITH GRANT OPTION")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("REVOKE INSERT(b) ON merge_tree FROM user1", settings=[("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_GrantOption_Revoke("1.0"),
)
def revoke_privilege_from_role_via_user_with_grant_option(self, table_type, node=None):
    """Check that user is unable to revoke a column they dont have acces to from a role.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), role(node, "role0"):
            with When("I grant privilege with grant option to user"):
                node.query("GRANT INSERT(d) ON merge_tree TO user0 WITH GRANT OPTION")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("REVOKE INSERT(b) ON merge_tree FROM role0", settings=[("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_GrantOption_Revoke("1.0"),
)
def revoke_privilege_from_user_via_role_with_grant_option(self, table_type, node=None):
    """Check that user with a role is unable to revoke a column they dont have acces to from a user.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0,user1"), role(node, "role0"):
            with When("I grant privilege with grant option to a role"):
                node.query("GRANT INSERT(d) ON merge_tree TO role0 WITH GRANT OPTION")
            with And("I grant the role to a user"):
                node.query("GRANT role0 TO user0")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("REVOKE INSERT(b) ON merge_tree FROM user1", settings=[("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert_GrantOption_Revoke("1.0"),
)
def revoke_privilege_from_role_via_role_with_grant_option(self, table_type, node=None):
    """Check that user with a role is unable to revoke a column they dont have acces to from a role.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), role(node, "role0,role1"):
            with When("I grant privilege with grant option to a role"):
                node.query("GRANT INSERT(d) ON merge_tree TO user0 WITH GRANT OPTION")
            with And("I grant the role to a user"):
                node.query("GRANT role0 TO user0")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("REVOKE INSERT(b) ON merge_tree FROM role1", settings=[("user","user0")],
                            exitcode=exitcode, message=message)

@TestOutline(Feature)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Insert("1.0"),
)
@Examples("table_type", [
    (table_type, Requirements(requirement)) for table_type, requirement in table_requirements.items()
])
@Name("insert")
def feature(self, table_type, node="clickhouse1"):
    self.context.node = self.context.cluster.node(node)

    self.context.node1 = self.context.cluster.node("clickhouse1")
    self.context.node2 = self.context.cluster.node("clickhouse2")
    self.context.node3 = self.context.cluster.node("clickhouse3")

    Scenario(test=without_privilege)(table_type=table_type)
    Scenario(test=user_with_privilege)(table_type=table_type)
    Scenario(test=user_with_revoked_privilege)(table_type=table_type)
    Scenario(test=user_with_privilege_on_columns)(table_type=table_type)
    Scenario(test=role_with_privilege)(table_type=table_type)
    Scenario(test=role_with_revoked_privilege)(table_type=table_type)
    Scenario(test=user_with_revoked_role)(table_type=table_type)
    Scenario(test=role_with_privilege_on_columns)(table_type=table_type)
    Scenario(test=user_with_privilege_on_cluster)(table_type=table_type)
    Scenario(test=user_with_privilege_from_user_with_grant_option)(table_type=table_type)
    Scenario(test=role_with_privilege_from_user_with_grant_option)(table_type=table_type)
    Scenario(test=user_with_privilege_from_role_with_grant_option)(table_type=table_type)
    Scenario(test=role_with_privilege_from_role_with_grant_option)(table_type=table_type)
    Scenario(test=revoke_privilege_from_user_via_user_with_grant_option)(table_type=table_type)
    Scenario(test=revoke_privilege_from_role_via_user_with_grant_option)(table_type=table_type)
    Scenario(test=revoke_privilege_from_user_via_role_with_grant_option)(table_type=table_type)
    Scenario(test=revoke_privilege_from_role_via_role_with_grant_option)(table_type=table_type)