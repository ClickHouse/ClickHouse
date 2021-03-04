from contextlib import contextmanager
import json

from testflows.core import *
from testflows.asserts import error

from rbac.requirements import *
from rbac.helper.common import *
import rbac.helper.errors as errors

@TestScenario
def without_privilege(self, table_type, node=None):
    """Check that user without select privilege on a table is not able to select on that table.
    """
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with user(node, user_name):
            with When("I run SELECT without privilege"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(f"SELECT * FROM {table_name}", settings = [("user",user_name)],
                            exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_Grant("1.0"),
)
def user_with_privilege(self, table_type, node=None):
    """Check that user can select from a table on which they have select privilege.
    """
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with Given("I have some data inserted into table"):
            node.query(f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')")
        with user(node, user_name):
            with When("I grant privilege"):
                node.query(f"GRANT SELECT ON {table_name} TO {user_name}")
            with Then("I verify SELECT command"):
                user_select = node.query(f"SELECT d FROM {table_name}", settings = [("user",user_name)])
                default = node.query(f"SELECT d FROM {table_name}")
                assert user_select.output == default.output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_Revoke("1.0"),
)
def user_with_revoked_privilege(self, table_type, node=None):
    """Check that user is unable to select from a table after select privilege
    on that table has been revoked from the user.
    """
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with user(node, user_name):
            with When("I grant privilege"):
                node.query(f"GRANT SELECT ON {table_name} TO {user_name}")
            with And("I revoke privilege"):
                node.query(f"REVOKE SELECT ON {table_name} FROM {user_name}")
            with And("I use SELECT, throws exception"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(f"SELECT * FROM {table_name}", settings = [("user",user_name)],
                    exitcode=exitcode, message=message)

@TestScenario
def user_with_privilege_on_columns(self, table_type):
    Scenario(run=user_column_privileges,
        examples=Examples("grant_columns revoke_columns select_columns_fail select_columns_pass data_pass table_type",
            [tuple(list(row)+[table_type]) for row in user_column_privileges.examples]))

@TestOutline
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
    user_name = f"user_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type), user(node, user_name):
        with Given("The table has some data on some columns"):
            node.query(f"INSERT INTO {table_name} ({select_columns_pass}) VALUES ({data_pass})")
        with When("I grant select privilege"):
            node.query(f"GRANT SELECT({grant_columns}) ON {table_name} TO {user_name}")
        if select_columns_fail is not None:
            with And("I select from not granted column"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(f"SELECT ({select_columns_fail}) FROM {table_name}",
                    settings = [("user",user_name)], exitcode=exitcode, message=message)
        with Then("I select from granted column, verify correct result"):
            user_select = node.query(f"SELECT ({select_columns_pass}) FROM {table_name}", settings = [("user",user_name)])
            default = node.query(f"SELECT ({select_columns_pass}) FROM {table_name}")
            assert user_select.output == default.output
        if revoke_columns is not None:
            with When("I revoke select privilege for columns from user"):
                node.query(f"REVOKE SELECT({revoke_columns}) ON {table_name} FROM {user_name}")
            with And("I select from revoked columns"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(f"SELECT ({select_columns_pass}) FROM {table_name}", settings = [("user",user_name)], exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_Grant("1.0"),
)
def role_with_privilege(self, table_type, node=None):
    """Check that user can select from a table after it is granted a role that
    has the select privilege for that table.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with Given("I have some data inserted into table"):
            node.query(f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')")
        with user(node, user_name):
            with role(node, role_name):
                with When("I grant select privilege to a role"):
                    node.query(f"GRANT SELECT ON {table_name} TO {role_name}")
                with And("I grant role to the user"):
                    node.query(f"GRANT {role_name} TO {user_name}")
                with Then("I verify SELECT command"):
                    user_select = node.query(f"SELECT d FROM {table_name}", settings = [("user",user_name)])
                    default = node.query(f"SELECT d FROM {table_name}")
                    assert user_select.output == default.output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_Revoke("1.0"),
)
def role_with_revoked_privilege(self, table_type, node=None):
    """Check that user with a role that has select privilege on a table is unable
    to select from that table after select privilege has been revoked from the role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with user(node, user_name), role(node, role_name):
            with When("I grant privilege to a role"):
                node.query(f"GRANT SELECT ON {table_name} TO {role_name}")
            with And("I grant the role to a user"):
                node.query(f"GRANT {role_name} TO {user_name}")
            with And("I revoke privilege from the role"):
                node.query(f"REVOKE SELECT ON {table_name} FROM {role_name}")
            with And("I select from the table"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(f"SELECT * FROM {table_name}", settings = [("user",user_name)],
                    exitcode=exitcode, message=message)

@TestScenario
def user_with_revoked_role(self, table_type, node=None):
    """Check that user with a role that has select privilege on a table is unable to
    select from that table after the role with select privilege has been revoked from the user.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with user(node, user_name), role(node, role_name):
            with When("I grant privilege to a role"):
                node.query(f"GRANT SELECT ON {table_name} TO {role_name}")
            with And("I grant the role to a user"):
                node.query(f"GRANT {role_name} TO {user_name}")
            with And("I revoke the role from the user"):
                node.query(f"REVOKE {role_name} FROM {user_name}")
            with And("I select from the table"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(f"SELECT * FROM {table_name}", settings = [("user",user_name)],
                    exitcode=exitcode, message=message)

@TestScenario
def role_with_privilege_on_columns(self, table_type):
    Scenario(run=role_column_privileges,
        examples=Examples("grant_columns revoke_columns select_columns_fail select_columns_pass data_pass table_type",
            [tuple(list(row)+[table_type]) for row in role_column_privileges.examples]))

@TestOutline
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
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with Given("The table has some data on some columns"):
            node.query(f"INSERT INTO {table_name} ({select_columns_pass}) VALUES ({data_pass})")
        with user(node, user_name), role(node, role_name):
            with When("I grant select privilege"):
                node.query(f"GRANT SELECT({grant_columns}) ON {table_name} TO {role_name}")
            with And("I grant the role to a user"):
                node.query(f"GRANT {role_name} TO {user_name}")
            if select_columns_fail is not None:
                with And("I select from not granted column"):
                    exitcode, message = errors.not_enough_privileges(name=user_name)
                    node.query(f"SELECT ({select_columns_fail}) FROM {table_name}",
                        settings = [("user",user_name)], exitcode=exitcode, message=message)
            with Then("I verify SELECT command"):
                user_select = node.query(f"SELECT d FROM {table_name}", settings = [("user",user_name)])
                default = node.query(f"SELECT d FROM {table_name}")
                assert user_select.output == default.output, error()
            if revoke_columns is not None:
                with When("I revoke select privilege for columns from role"):
                    node.query(f"REVOKE SELECT({revoke_columns}) ON {table_name} FROM {role_name}")
                with And("I select from revoked columns"):
                    exitcode, message = errors.not_enough_privileges(name=user_name)
                    node.query(f"SELECT ({select_columns_pass}) FROM {table_name}",
                        settings = [("user",user_name)], exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Grant("1.0"),
)
def user_with_privilege_on_cluster(self, table_type, node=None):
    """Check that user is able to select from a table with
    privilege granted on a cluster.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        try:
            with Given("I have some data inserted into table"):
                node.query(f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')")
            with Given("I have a user on a cluster"):
                node.query(f"CREATE USER OR REPLACE {user_name} ON CLUSTER sharded_cluster")
            with When("I grant select privilege on a cluster"):
                node.query(f"GRANT ON CLUSTER sharded_cluster SELECT ON {table_name} TO {user_name}")
            with Then("I verify SELECT command"):
                user_select = node.query(f"SELECT d FROM {table_name}", settings = [("user",user_name)])
                default = node.query(f"SELECT d FROM {table_name}")
                assert user_select.output == default.output, error()
        finally:
            with Finally("I drop the user"):
                node.query(f"DROP USER {user_name} ON CLUSTER sharded_cluster")

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Grant("1.0"),
)
def user_with_privilege_from_user_with_grant_option(self, table_type, node=None):
    """Check that user is able to select from a table when granted privilege
    from another user with grant option.
    """
    user0_name = f"user0_{getuid()}"
    user1_name = f"user1_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with Given("I have some data inserted into table"):
            node.query(f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')")
        with user(node, f"{user0_name},{user1_name}"):
            with When("I grant privilege with grant option to user"):
                node.query(f"GRANT SELECT ON {table_name} TO {user0_name} WITH GRANT OPTION")
            with And("I grant privilege to another user via grant option"):
                node.query(f"GRANT SELECT ON {table_name} TO {user1_name}", settings = [("user",user0_name)])
            with Then("I verify SELECT command"):
                user_select = node.query(f"SELECT d FROM {table_name}", settings = [("user",user1_name)])
                default = node.query(f"SELECT d FROM {table_name}")
                assert user_select.output == default.output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Grant("1.0"),
)
def role_with_privilege_from_user_with_grant_option(self, table_type, node=None):
    """Check that user is able to select from a table when granted a role with
    select privilege that was granted by another user with grant option.
    """
    user0_name = f"user0_{getuid()}"
    user1_name = f"user1_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with Given("I have some data inserted into table"):
            node.query(f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')")
        with user(node, f"{user0_name},{user1_name}"), role(node, role_name):
            with When("I grant privilege with grant option to user"):
                node.query(f"GRANT SELECT ON {table_name} TO {user0_name} WITH GRANT OPTION")
            with And("I grant privilege to a role via grant option"):
                node.query(f"GRANT SELECT ON {table_name} TO {role_name}", settings = [("user",user0_name)])
            with And("I grant the role to another user"):
                node.query(f"GRANT {role_name} TO {user1_name}")
            with Then("I verify SELECT command"):
                user_select = node.query(f"SELECT d FROM {table_name}", settings = [("user",user1_name)])
                default = node.query(f"SELECT d FROM {table_name}")
                assert user_select.output == default.output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Grant("1.0"),
)
def user_with_privilege_from_role_with_grant_option(self, table_type, node=None):
    """Check that user is able to select from a table when granted privilege from
    a role with grant option
    """
    user0_name = f"user0_{getuid()}"
    user1_name = f"user1_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with Given("I have some data inserted into table"):
            node.query(f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')")
        with user(node, f"{user0_name},{user1_name}"), role(node, role_name):
            with When("I grant privilege with grant option to a role"):
                node.query(f"GRANT SELECT ON {table_name} TO {role_name} WITH GRANT OPTION")
            with When("I grant role to a user"):
                node.query(f"GRANT {role_name} TO {user0_name}")
            with And("I grant privilege to a user via grant option"):
                node.query(f"GRANT SELECT ON {table_name} TO {user1_name}", settings = [("user",user0_name)])
            with Then("I verify SELECT command"):
                user_select = node.query(f"SELECT d FROM {table_name}", settings = [("user",user1_name)])
                default = node.query(f"SELECT d FROM {table_name}")
                assert user_select.output == default.output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Grant("1.0"),
)
def role_with_privilege_from_role_with_grant_option(self, table_type, node=None):
    """Check that a user is able to select from a table with a role that was
    granted privilege by another role with grant option
    """
    user0_name = f"user0_{getuid()}"
    user1_name = f"user1_{getuid()}"
    role0_name = f"role0_{getuid()}"
    role1_name = f"role1_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with Given("I have some data inserted into table"):
            node.query(f"INSERT INTO {table_name} (d) VALUES ('2020-01-01')")
        with user(node, f"{user0_name},{user1_name}"), role(node, f"{role0_name},{role1_name}"):
            with When("I grant privilege with grant option to role"):
                node.query(f"GRANT SELECT ON {table_name} TO {role0_name} WITH GRANT OPTION")
            with And("I grant the role to a user"):
                node.query(f"GRANT {role0_name} TO {user0_name}")
            with And("I grant privilege to another role via grant option"):
                node.query(f"GRANT SELECT ON {table_name} TO {role1_name}", settings = [("user",user0_name)])
            with And("I grant the second role to another user"):
                node.query(f"GRANT {role1_name} TO {user1_name}")
            with Then("I verify SELECT command"):
                user_select = node.query(f"SELECT d FROM {table_name}", settings = [("user",user1_name)])
                default = node.query(f"SELECT d FROM {table_name}")
                assert user_select.output == default.output, error()

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Revoke("1.0"),
)
def revoke_privilege_from_user_via_user_with_grant_option(self, table_type, node=None):
    """Check that user is unable to revoke a column they don't have access to from a user.
    """
    user0_name = f"user0_{getuid()}"
    user1_name = f"user1_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with user(node, f"{user0_name},{user1_name}"):
            with When("I grant privilege with grant option to user"):
                node.query(f"GRANT SELECT(d) ON {table_name} TO {user0_name} WITH GRANT OPTION")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                exitcode, message = errors.not_enough_privileges(name=user0_name)
                node.query(f"REVOKE SELECT(b) ON {table_name} FROM {user1_name}", settings=[("user",user0_name)],
                    exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Revoke("1.0"),
)
def revoke_privilege_from_role_via_user_with_grant_option(self, table_type, node=None):
    """Check that user is unable to revoke a column they dont have acces to from a role.
    """
    user_name = f"user_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with user(node, user_name), role(node, role_name):
            with When("I grant privilege with grant option to user"):
                node.query(f"GRANT SELECT(d) ON {table_name} TO {user_name} WITH GRANT OPTION")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(f"REVOKE SELECT(b) ON {table_name} FROM {role_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Revoke("1.0"),
)
def revoke_privilege_from_user_via_role_with_grant_option(self, table_type, node=None):
    """Check that user with a role is unable to revoke a column they dont have acces to from a user.
    """
    user0_name = f"user0_{getuid()}"
    user1_name = f"user1_{getuid()}"
    role_name = f"role_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with user(node, f"{user0_name},{user1_name}"), role(node, role_name):
            with When("I grant privilege with grant option to a role"):
                node.query(f"GRANT SELECT(d) ON {table_name} TO {role_name} WITH GRANT OPTION")
            with And("I grant the role to a user"):
                node.query(f"GRANT {role_name} TO {user0_name}")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                exitcode, message = errors.not_enough_privileges(name=user0_name)
                node.query(f"REVOKE SELECT(b) ON {table_name} FROM {user1_name}", settings=[("user",user0_name)],
                    exitcode=exitcode, message=message)

@TestScenario
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select_GrantOption_Revoke("1.0"),
)
def revoke_privilege_from_role_via_role_with_grant_option(self, table_type, node=None):
    """Check that user with a role is unable to revoke a column they dont have acces to from a role.
    """
    user_name = f"user_{getuid()}"
    role0_name = f"role0_{getuid()}"
    role1_name = f"role1_{getuid()}"
    table_name = f"table_{getuid()}"
    if node is None:
        node = self.context.node
    with table(node, table_name, table_type):
        with user(node, user_name), role(node, f"{role0_name},{role1_name}"):
            with When("I grant privilege with grant option to a role"):
                node.query(f"GRANT SELECT(d) ON {table_name} TO {role0_name} WITH GRANT OPTION")
            with And("I grant the role to a user"):
                node.query(f"GRANT {role0_name} TO {user_name}")
            with Then("I revoke privilege on a column the user with grant option does not have access to"):
                exitcode, message = errors.not_enough_privileges(name=user_name)
                node.query(f"REVOKE SELECT(b) ON {table_name} FROM {role1_name}", settings=[("user",user_name)],
                    exitcode=exitcode, message=message)

@TestOutline(Feature)
@Requirements(
    RQ_SRS_006_RBAC_Privileges_Select("1.0"),
    RQ_SRS_006_RBAC_Privileges_Select_TableEngines("1.0")
)
@Examples("table_type", [
    (key,) for key in table_types.keys()
])
@Name("select")
def feature(self, table_type, parallel=None, stress=None, node="clickhouse1"):
    self.context.node = self.context.cluster.node(node)

    if stress is not None:
        self.context.stress = stress
    if parallel is not None:
        self.context.stress = parallel

    tasks = []
    pool = Pool(3)

    try:
        for scenario in loads(current_module(), Scenario):
            run_scenario(pool, tasks, scenario, {"table_type" : table_type})
    finally:
        join(tasks)