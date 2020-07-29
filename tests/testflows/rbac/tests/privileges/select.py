from contextlib import contextmanager
import json

from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
import rbac.tests.errors as errors

table_types = {
    "MergeTree": "CREATE TABLE {name} (d DATE, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = MergeTree(d, (a, b), 111)",
    "CollapsingMergeTree": "CREATE TABLE {name} (d Date, a String, b UInt8, x String, y Int8, z UInt32) ENGINE = CollapsingMergeTree(d, (a, b), 111, y);"
}

exitcode, message = errors.not_enough_privileges(name="user0")

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

def input_output_equality_check(node, user, input_columns="d", input_data="2020-01-01"):
    """
    ensures that selecting some data from a specified user on some table gives the desired
    result. The desired result is specified from input_columns and input_data
    """
    data_list = [x.strip("'") for x in input_data.split(",")]
    input_dict = dict(zip(input_columns.split(","), data_list))
    output_dict = json.loads(node.query(f"select {input_columns} from merge_tree format JSONEachRow",
                                        settings = [("user", user)]).output)
    output_dict = {k:str(v) for (k,v) in output_dict.items()}
    return input_dict == output_dict

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0")
)
def without_privilege(self, table_type, node=None):
    """Check that user without select privilege on a table is not able to select on that table."""
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"):
            with When("I run SELECT without privilege"):
                node.query("SELECT * FROM merge_tree", settings = [("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select_Effect("1.0")
)
def user_with_privilege(self, table_type, node=None):
    """Check that user can select from a table on which they have select privilege."""
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with Given("I have some data inserted into table"):
            node.query("INSERT INTO merge_tree (d) VALUES ('2020-01-01')")
        with user(node, "user0"):
            with When("I grant privilege"):
                node.query("GRANT SELECT ON merge_tree TO user0")
            with And("I use SELECT"):
                node.query("SELECT d FROM merge_tree", settings = [("user","user0")])
            with Then("I check the SELECT call output is correct"):
                assert input_output_equality_check(node, "user0"), error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_Select_Effect("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_On_Effect("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_From("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_From_Effect("1.0")
)
def user_with_revoked_privilege(self, table_type, node=None):
    """
    Check that user is unable to select from a table after select privilege
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
                node.query("SELECT * FROM merge_tree", settings = [("user","user0")],
                            exitcode=exitcode, message=message)

@TestOutline(Scenario)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select_Effect("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_Select_Effect("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_PrivelegeColumns("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_PrivelegeColumns_Effect("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_On_Effect("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_From("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_From_Effect("1.0")
)
@Examples("grant_columns revoke_columns select_columns_fail select_columns_pass data_pass", [
    ("d", "d", "x", "d", '\'2020-01-01\''),
    ("d,a", "d", "x", "d", '\'2020-01-01\''),
    ("d,a,b", "d,a,b", "x", "d,b", '\'2020-01-01\',9'),
    ("d,a,b", "b", "y", "d,a,b", '\'2020-01-01\',\'woo\',9')
])
def user_with_privilege_on_columns(self, grant_columns, select_columns_pass, data_pass, table_type, revoke_columns=None, select_columns_fail=None, node=None):
    """
    Check that user is able to select on granted columns
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
                node.query(f"SELECT ({select_columns_fail}) FROM merge_tree",
                            settings = [("user","user0")], exitcode=exitcode, message=message)
        with Then("I select from granted column and check result"):
            assert input_output_equality_check(node, "user0", input_columns=select_columns_pass, input_data=data_pass), error()
        if revoke_columns is not None:
            with When("I revoke select privilege for columns from user"):
                node.query(f"REVOKE SELECT({revoke_columns}) ON merge_tree FROM user0")
            with And("I select from revoked columns"):
                node.query(f"SELECT ({select_columns_pass}) FROM merge_tree", settings = [("user","user0")], exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select_Effect("1.0")
)
def role_with_privilege(self, table_type, node=None):
    """
    Check that user can select from a table after it is granted a role that
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
                with Then("I check that SELECT call output is correct"):
                    assert input_output_equality_check(node, "user0"), error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_Select_Effect("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_On_Effect("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_From("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_From_Effect("1.0")
)
def role_with_revoked_privilege(self, table_type, node=None):
    """
    Check that user with a role that has select privilege on a table is unable
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
                node.query("SELECT * FROM merge_tree", settings = [("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select_Effect("1.0"),
    RQ_SRS_006_RBAC_Revoke_Role("1.0"),
    RQ_SRS_006_RBAC_Revoke_Role_Effect("1.0")
)
def user_with_revoked_role(self, table_type, node=None):
    """
    Check that user with a role that has select privilege on a table is unable to 
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
                node.query("SELECT * FROM merge_tree", settings = [("user","user0")],
                            exitcode=exitcode, message=message)

@TestOutline(Scenario)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select_Effect("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_Select_Effect("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_PrivelegeColumns("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_PrivelegeColumns_Effect("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_On_Effect("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_From("1.0"),
    RQ_SRS_006_RBAC_Revoke_Privilege_From_Effect("1.0")
)
@Examples("grant_columns revoke_columns select_columns_fail select_columns_pass data_pass", [
    ("d", "d", "x", "d", '\'2020-01-01\''),
    ("d,a", "d", "x", "d", '\'2020-01-01\''),
    ("d,a,b", "d,a,b", "x", "d,b", '\'2020-01-01\',9'),
    ("d,a,b", "b", "y", "d,a,b", '\'2020-01-01\',\'woo\',9')
])
def role_with_privilege_on_columns(self, grant_columns, select_columns_pass, data_pass, table_type, revoke_columns=None, select_columns_fail=None, node=None):
    """
    Check that user is able to select from granted columns and unable
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
                    node.query(f"SELECT ({select_columns_fail}) FROM merge_tree",
                                settings = [("user","user0")], exitcode=exitcode, message=message)
            with Then("I select from granted column and check result"):
                assert input_output_equality_check(node, "user0", input_columns=select_columns_pass, input_data=data_pass), error()
            if revoke_columns is not None:
                with When("I revoke select privilege for columns from role"):
                    node.query(f"REVOKE SELECT({revoke_columns}) ON merge_tree FROM role0")
                with And("I select from revoked columns"):
                    node.query(f"SELECT ({select_columns_pass}) FROM merge_tree",
                                settings = [("user","user0")], exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select_Effect("1.0"),
)
def user_with_privilege_on_cluster(self, table_type, node=None):
    """
    Check that user is able to select from a table with
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
            with Then("I check that SELECT call output is correct"):
                assert input_output_equality_check(node, "user0"), error()
        finally:
            with Finally("I drop the user"):
                node.query("DROP USER user0 ON CLUSTER sharded_cluster")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select_Effect("1.0"),
)
def user_with_privilege_from_user_with_grant_option(self, table_type, node=None):
    """
    Check that user is able to select from a table when granted privilege
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
            with Then("I check that SELECT call output is correct"):
                assert input_output_equality_check(node, "user1"), error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select_Effect("1.0"),
)
def role_with_privilege_from_user_with_grant_option(self, table_type, node=None):
    """
    Check that user is able to select from a table when granted a role with
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
            with Then("I check that SELECT call output is correct"):
                assert input_output_equality_check(node, "user1"), error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select_Effect("1.0"),
)
def user_with_privilege_from_role_with_grant_option(self, table_type, node=None):
    """
    Check that user is able to select from a table when granted privilege from 
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
            with Then("I check that SELECT call output is correct"):
                assert input_output_equality_check(node, "user1"), error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select_Effect("1.0"),
)
def role_with_privilege_from_role_with_grant_option(self, table_type, node=None):
    """
    Check that a user is able to select from a table with a role that was 
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
            with Then("I check that SELECT call output is correct"):
                assert input_output_equality_check(node, "user1"), error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_Privileges_GrantOption("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To_Effect("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_PrivilegeColumns("1.0"),
)
def revoke_privilege_from_user_via_user_with_grant_option(self, table_type, node=None):
    """Check that user is unable to revoke a column they don't have access to from a user."""
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), user(node, "user1"):
            with When("I grant privilege with grant option to user"):
                node.query("GRANT SELECT(d) ON merge_tree TO user0 WITH GRANT OPTION")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                node.query("REVOKE SELECT(b) ON merge_tree FROM user1", settings=[("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_Privileges_GrantOption("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To_Effect("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_PrivilegeColumns("1.0"),
)
def revoke_privilege_from_role_via_user_with_grant_option(self, table_type, node=None):
    """Check that user is unable to revoke a column they dont have acces to from a role."""
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), role(node, "role0"):
            with When("I grant privilege with grant option to user"):
                node.query("GRANT SELECT(d) ON merge_tree TO user0 WITH GRANT OPTION")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                node.query("REVOKE SELECT(b) ON merge_tree FROM role0", settings=[("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_Privileges_GrantOption("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To_Effect("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_PrivilegeColumns("1.0"),
)
def revoke_privilege_from_user_via_role_with_grant_option(self, table_type, node=None):
    """Check that user with a role is unable to revoke a column they dont have acces to from a user."""
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), user(node,"user1"), role(node, "role0"):
            with When("I grant privilege with grant option to a role"):
                node.query("GRANT SELECT(d) ON merge_tree TO role0 WITH GRANT OPTION")
            with And("I grant the role to a user"):
                node.query("GRANT role0 TO user0")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                node.query("REVOKE SELECT(b) ON merge_tree FROM user1", settings=[("user","user0")],
                            exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_Privileges_GrantOption("1.0"),
    RQ_SRS_006_RBAC_RequiredPrivileges_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_To_Effect("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_On("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"),
    RQ_SRS_006_RBAC_Grant_Privilege_PrivilegeColumns("1.0"),
)
def revoke_privilege_from_role_via_role_with_grant_option(self, table_type, node=None):
    """Check that user with a role is unable to revoke a column they dont have acces to from a role."""
    if node is None:
        node = self.context.node
    with table(node, "merge_tree", table_type):
        with user(node, "user0"), role(node, "role0"), role(node, "role1"):
            with When("I grant privilege with grant option to a role"):
                node.query("GRANT SELECT(d) ON merge_tree TO user0 WITH GRANT OPTION")
            with And("I grant the role to a user"):
                node.query("GRANT role0 TO user0")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                node.query("REVOKE SELECT(b) ON merge_tree FROM role1", settings=[("user","user0")],
                            exitcode=exitcode, message=message)

@TestOutline(Feature)
@Examples("table_type", [
    (key,) for key in table_types.keys()
])
@Name("select")
def feature(self, table_type, node="clickhouse1"):
    self.context.node = self.context.cluster.node(node)

    Scenario(test=without_privilege)(table_type=table_type)
    Scenario(test=user_with_privilege)(table_type=table_type)
    Scenario(test=user_with_revoked_privilege)(table_type=table_type)
    Scenario(run=user_with_privilege_on_columns,
        examples=Examples("grant_columns revoke_columns select_columns_fail select_columns_pass  data_pass table_type",
            [tuple(list(row)+[table_type]) for row in user_with_privilege_on_columns.examples]))
    Scenario(test=role_with_privilege)(table_type=table_type)
    Scenario(test=role_with_revoked_privilege)(table_type=table_type)
    Scenario(test=user_with_revoked_role)(table_type=table_type)
    Scenario(run=role_with_privilege_on_columns,
        examples=Examples("grant_columns revoke_columns select_columns_fail select_columns_pass  data_pass table_type",
            [tuple(list(row)+[table_type]) for row in role_with_privilege_on_columns.examples]))
    Scenario(test=user_with_privilege_on_cluster)(table_type=table_type)
    Scenario(test=user_with_privilege_from_user_with_grant_option)(table_type=table_type)
    Scenario(test=role_with_privilege_from_user_with_grant_option)(table_type=table_type)
    Scenario(test=user_with_privilege_from_role_with_grant_option)(table_type=table_type)
    Scenario(test=role_with_privilege_from_role_with_grant_option)(table_type=table_type)
    Scenario(test=revoke_privilege_from_user_via_user_with_grant_option)(table_type=table_type)
    Scenario(test=revoke_privilege_from_role_via_user_with_grant_option)(table_type=table_type)
    Scenario(test=revoke_privilege_from_user_via_role_with_grant_option)(table_type=table_type)
    Scenario(test=revoke_privilege_from_role_via_role_with_grant_option)(table_type=table_type)
