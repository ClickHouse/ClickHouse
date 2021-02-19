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
    "MergeTree": RQ_SRS_006_RBAC_Privileges_Select_MergeTree("1.0"),
    "ReplacingMergeTree": RQ_SRS_006_RBAC_Privileges_Select_ReplacingMergeTree("1.0"),
    "SummingMergeTree": RQ_SRS_006_RBAC_Privileges_Select_SummingMergeTree("1.0"),
    "AggregatingMergeTree": RQ_SRS_006_RBAC_Privileges_Select_AggregatingMergeTree("1.0"),
    "CollapsingMergeTree": RQ_SRS_006_RBAC_Privileges_Select_CollapsingMergeTree("1.0"),
    "VersionedCollapsingMergeTree": RQ_SRS_006_RBAC_Privileges_Select_VersionedCollapsingMergeTree("1.0"),
    "GraphiteMergeTree": RQ_SRS_006_RBAC_Privileges_Select_GraphiteMergeTree("1.0"),
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
        with Given("I have a user"):
            node.query(f"CREATE USER OR REPLACE {name}")
        yield
    finally:
        with Finally("I drop the user"):
            node.query(f"DROP USER IF EXISTS {name}")

@contextmanager
def role(node, role):
    try:
        with Given("I have a role"):
            node.query(f"CREATE ROLE OR REPLACE {role}")
        yield
    finally:
        with Finally("I drop the role"):
            node.query(f"DROP ROLE IF EXISTS {role}")

@TestScenario
def without_privilege(self, table_type, node=None):
    """Check that user without select privilege on a table is not able to select on that table.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"):
            with When("I run SELECT without privilege"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("SELECT * FROM merge_tree", settings = [("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_Grant("1.0"),
)
def user_with_privilege(self, table_type, node=None):
    """Check that user can select from a table on which they have select privilege.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with Given("I have some data inserted into table"):
            node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')")
        with user(node, "user88"):
            pass
        with user(node, "user0"):
            with When("I grant privilege"):
                node.query("GRANT SELECT ON merge_tree TO user0")
            with Then("I verify SELECT command"):
                user_select = node.query("SELECT d FROM merge_tree", settings = [("user","user0")])
                default = node.query("SELECT d FROM merge_tree")
                assert user_select.output == default.output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_Revoke("1.0"),
)
def user_with_revoked_privilege(self, table_type, node=None):
    """Check that user is unable to select from a table after select privilege
    on that table has been revoked from the user.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"):
            with When("I grant privilege"):
                node.query("GRANT SELECT ON merge_tree TO user0")
            with And("I revoke privilege"):
                node.query("REVOKE SELECT ON merge_tree FROM user0")
            with And("I use SELECT, throws exception"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("SELECT * FROM merge_tree", settings = [("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
def user_with_privilege_on_columns(self, table_type):
    Scenario(run=user_column_privileges,
             examples=Examples("grant_columns revoke_columns select_columns_fail select_columns_pass data_pass table_type",
                               [tuple(list(row)+[table_type]) for row in user_column_privileges.examples]))

@TestOutline(Scenario)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_Column("1.0"),
)
@Examples("grant_columns revoke_columns select_columns_fail select_columns_pass data_pass", [
    ("d", "d", "x", "d", '\'2020-01-01\''),
    ("d,a", "d", "x", "d", '\'2020-01-01\''),
    ("d,a,b", "d,a,b", "x", "d,b", '\'2020-01-01\',9'),
    ("d,a,b", "b", "y", "d,a,b", '\'2020-01-01\',\'woo\',9')
])
def user_column_privileges(self, grant_columns, select_columns_pass, data_pass, table_type, revoke_columns=None, select_columns_fail=None, node=None):
    """Check that user is able to select on granted columns
    and unable to select on not granted or revoked columns.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type), user(node, "user0"):
        with Given("The table has some data on some columns"):
            node.query(f"INSERT INTO merge_tree ({select_columns_pass}) VALUES ({data_pass})")
        with When("I grant select privilege"):
            node.query(f"GRANT SELECT({grant_columns}) ON merge_tree TO user0")
        if select_columns_fail is not None:
            with And("I select from not granted column"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query(f"SELECT ({select_columns_fail}) FROM merge_tree",
                            settings = [("user","user0")], exitcode=exitcode, message=message)
        with Then("I select from granted column, verify correct result"):
            user_select = node.query("SELECT d FROM merge_tree", settings = [("user","user0")])
            default = node.query("SELECT d FROM merge_tree")
            assert user_select.output == default.output
        if revoke_columns is not None:
            with When("I revoke select privilege for columns from user"):
                node.query(f"REVOKE SELECT({revoke_columns}) ON merge_tree FROM user0")
            with And("I select from revoked columns"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query(f"SELECT ({select_columns_pass}) FROM merge_tree", settings = [("user","user0")], exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_Grant("1.0"),
)
def role_with_privilege(self, table_type, node=None):
    """Check that user can select from a table after it is granted a role that
    has the select privilege for that table.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with Given("I have some data inserted into table"):
            node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')")
        with user(node, "user0"):
            with role(node, "role0"):
                with When("I grant select privilege to a role"):
                    node.query("GRANT SELECT ON merge_tree TO role0")
                with And("I grant role to the user"):
                    node.query("GRANT role0 TO user0")
                with Then("I verify SELECT command"):
                    user_select = node.query("SELECT d FROM merge_tree", settings = [("user","user0")])
                    default = node.query("SELECT d FROM merge_tree")
                    assert user_select.output == default.output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_Revoke("1.0"),
)
def role_with_revoked_privilege(self, table_type, node=None):
    """Check that user with a role that has select privilege on a table is unable
    to select from that table after select privilege has been revoked from the role.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), role(node, "role0"):
            with When("I grant privilege to a role"):
                node.query("GRANT SELECT ON merge_tree TO role0")
            with And("I grant the role to a user"):
                node.query("GRANT role0 TO user0")
            with And("I revoke privilege from the role"):
                node.query("REVOKE SELECT ON merge_tree FROM role0")
            with And("I select from the table"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("SELECT * FROM merge_tree", settings = [("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
def user_with_revoked_role(self, table_type, node=None):
    """Check that user with a role that has select privilege on a table is unable to 
    select from that table after the role with select privilege has been revoked from the user.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), role(node, "role0"):
            with When("I grant privilege to a role"):
                node.query("GRANT SELECT ON merge_tree TO role0")
            with And("I grant the role to a user"):
                node.query("GRANT role0 TO user0")
            with And("I revoke the role from the user"):
                node.query("REVOKE role0 FROM user0")
            with And("I select from the table"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("SELECT * FROM merge_tree", settings = [("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
def role_with_privilege_on_columns(self, table_type):
    Scenario(run=role_column_privileges,
             examples=Examples("grant_columns revoke_columns select_columns_fail select_columns_pass data_pass table_type",
                               [tuple(list(row)+[table_type]) for row in role_column_privileges.examples]))

@TestOutline(Scenario)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_Column("1.0"),
)
@Examples("grant_columns revoke_columns select_columns_fail select_columns_pass data_pass", [
    ("d", "d", "x", "d", '\'2020-01-01\''),
    ("d,a", "d", "x", "d", '\'2020-01-01\''),
    ("d,a,b", "d,a,b", "x", "d,b", '\'2020-01-01\',9'),
    ("d,a,b", "b", "y", "d,a,b", '\'2020-01-01\',\'woo\',9')
])
def role_column_privileges(self, grant_columns, select_columns_pass, data_pass, table_type, revoke_columns=None, select_columns_fail=None, node=None):
    """Check that user is able to select from granted columns and unable
    to select from not granted or revoked columns.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with Given("The table has some data on some columns"):
            node.query(f"INSERT INTO merge_tree ({select_columns_pass}) VALUES ({data_pass})")
        with user(node, "user0"), role(node, "role0"):
            with When("I grant select privilege"):
                node.query(f"GRANT SELECT({grant_columns}) ON merge_tree TO role0")
            with And("I grant the role to a user"):
                node.query("GRANT role0 TO user0")
            if select_columns_fail is not None:
                with And("I select from not granted column"):
                    exitcode, message = errors.not_enough_privileges(name="user0")
                    node.query(f"SELECT ({select_columns_fail}) FROM merge_tree",
                                settings = [("user","user0")], exitcode=exitcode, message=message)
            with Then("I verify SELECT command"):
                user_select = node.query("SELECT d FROM merge_tree", settings = [("user","user0")])
                default = node.query("SELECT d FROM merge_tree")
                assert user_select.output == default.output, error()
            if revoke_columns is not None:
                with When("I revoke select privilege for columns from role"):
                    node.query(f"REVOKE SELECT({revoke_columns}) ON merge_tree FROM role0")
                with And("I select from revoked columns"):
                    exitcode, message = errors.not_enough_privileges(name="user0")
                    node.query(f"SELECT ({select_columns_pass}) FROM merge_tree",
                                settings = [("user","user0")], exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Grant("1.0"),
)
def user_with_privilege_on_cluster(self, table_type, node=None):
    """Check that user is able to select from a table with
    privilege granted on a cluster.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        try:
            with Given("I have some data inserted into table"):
                node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')")
            with Given("I have a user on a cluster"):
                node.query("CREATE USER OR REPLACE user0 ON CLUSTER sharded_cluster")
            with When("I grant select privilege on a cluster"):
                node.query("GRANT ON CLUSTER sharded_cluster SELECT ON merge_tree TO user0")
            with Then("I verify SELECT command"):
                user_select = node.query("SELECT d FROM merge_tree", settings = [("user","user0")])
                default = node.query("SELECT d FROM merge_tree")
                assert user_select.output == default.output, error()
        finally:
            with Finally("I drop the user"):
                node.query("DROP USER user0 ON CLUSTER sharded_cluster")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Grant("1.0"),
)
def user_with_privilege_from_user_with_grant_option(self, table_type, node=None):
    """Check that user is able to select from a table when granted privilege
    from another user with grant option.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with Given("I have some data inserted into table"):
            node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')")
        with user(node, "user0"), user(node, "user1"):
            with When("I grant privilege with grant option to user"):
                node.query("GRANT SELECT ON merge_tree TO user0 WITH GRANT OPTION")
            with And("I grant privilege to another user via grant option"):
                node.query("GRANT SELECT ON merge_tree TO user1", settings = [("user","user0")])
            with Then("I verify SELECT command"):
                user_select = node.query("SELECT d FROM merge_tree", settings = [("user","user1")])
                default = node.query("SELECT d FROM merge_tree")
                assert user_select.output == default.output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Grant("1.0"),
)
def role_with_privilege_from_user_with_grant_option(self, table_type, node=None):
    """Check that user is able to select from a table when granted a role with
    select privilege that was granted by another user with grant option.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with Given("I have some data inserted into table"):
            node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')")
        with user(node, "user0"), user(node, "user1"), role(node, "role0"):
            with When("I grant privilege with grant option to user"):
                node.query("GRANT SELECT ON merge_tree TO user0 WITH GRANT OPTION")
            with And("I grant privilege to a role via grant option"):
                node.query("GRANT SELECT ON merge_tree TO role0", settings = [("user","user0")])
            with And("I grant the role to another user"):
                node.query("GRANT role0 TO user1")
            with Then("I verify SELECT command"):
                user_select = node.query("SELECT d FROM merge_tree", settings = [("user","user1")])
                default = node.query("SELECT d FROM merge_tree")
                assert user_select.output == default.output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Grant("1.0"),
)
def user_with_privilege_from_role_with_grant_option(self, table_type, node=None):
    """Check that user is able to select from a table when granted privilege from
    a role with grant option
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with Given("I have some data inserted into table"):
            node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')")
        with user(node, "user0"), user(node, "user1"), role(node, "role0"):
            with When("I grant privilege with grant option to a role"):
                node.query("GRANT SELECT ON merge_tree TO role0 WITH GRANT OPTION")
            with When("I grant role to a user"):
                node.query("GRANT role0 TO user0")
            with And("I grant privilege to a user via grant option"):
                node.query("GRANT SELECT ON merge_tree TO user1", settings = [("user","user0")])
            with Then("I verify SELECT command"):
                user_select = node.query("SELECT d FROM merge_tree", settings = [("user","user1")])
                default = node.query("SELECT d FROM merge_tree")
                assert user_select.output == default.output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Grant("1.0"),
)
def role_with_privilege_from_role_with_grant_option(self, table_type, node=None):
    """Check that a user is able to select from a table with a role that was
    granted privilege by another role with grant option
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with Given("I have some data inserted into table"):
            node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')")
        with user(node, "user0"), user(node, "user1"), role(node, "role0"), role(node, "role1"):
            with When("I grant privilege with grant option to role"):
                node.query("GRANT SELECT ON merge_tree TO role0 WITH GRANT OPTION")
            with And("I grant the role to a user"):
                node.query("GRANT role0 TO user0")
            with And("I grant privilege to another role via grant option"):
                node.query("GRANT SELECT ON merge_tree TO role1", settings = [("user","user0")])
            with And("I grant the second role to another user"):
                node.query("GRANT role1 TO user1")
            with Then("I verify SELECT command"):
                user_select = node.query("SELECT d FROM merge_tree", settings = [("user","user1")])
                default = node.query("SELECT d FROM merge_tree")
                assert user_select.output == default.output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Revoke("1.0"),
)
def revoke_privilege_from_user_via_user_with_grant_option(self, table_type, node=None):
    """Check that user is unable to revoke a column they don't have access to from a user.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), user(node, "user1"):
            with When("I grant privilege with grant option to user"):
                node.query("GRANT SELECT(d) ON merge_tree TO user0 WITH GRANT OPTION")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("REVOKE SELECT(b) ON merge_tree FROM user1", settings=[("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Revoke("1.0"),
)
def revoke_privilege_from_role_via_user_with_grant_option(self, table_type, node=None):
    """Check that user is unable to revoke a column they dont have acces to from a role.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), role(node, "role0"):
            with When("I grant privilege with grant option to user"):
                node.query("GRANT SELECT(d) ON merge_tree TO user0 WITH GRANT OPTION")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("REVOKE SELECT(b) ON merge_tree FROM role0", settings=[("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Revoke("1.0"),
)
def revoke_privilege_from_user_via_role_with_grant_option(self, table_type, node=None):
    """Check that user with a role is unable to revoke a column they dont have acces to from a user.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), user(node,"user1"), role(node, "role0"):
            with When("I grant privilege with grant option to a role"):
                node.query("GRANT SELECT(d) ON merge_tree TO role0 WITH GRANT OPTION")
            with And("I grant the role to a user"):
                node.query("GRANT role0 TO user0")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("REVOKE SELECT(b) ON merge_tree FROM user1", settings=[("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Revoke("1.0"),
)
def revoke_privilege_from_role_via_role_with_grant_option(self, table_type, node=None):
    """Check that user with a role is unable to revoke a column they dont have acces to from a role.
    """
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), role(node, "role0"), role(node, "role1"):
            with When("I grant privilege with grant option to a role"):
                node.query("GRANT SELECT(d) ON merge_tree TO user0 WITH GRANT OPTION")
            with And("I grant the role to a user"):
                node.query("GRANT role0 TO user0")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                exitcode, message = errors.not_enough_privileges(name="user0")
                node.query("REVOKE SELECT(b) ON merge_tree FROM role1", settings=[("user","user0")],
                            exitcode=exitcode, message=message)

@TestOutline(Feature)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
)
@Examples("table_type", [
    (table_type, Requirements(requirement)) for table_type, requirement in table_requirements.items()
])
@Name("select")
def feature(self, table_type, node="clickhouse1"):
    self.context.node = self.context.cluster.node(node)

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