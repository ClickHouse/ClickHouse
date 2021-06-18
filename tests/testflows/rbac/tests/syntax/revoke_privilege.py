from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *

@contextmanager
def setup(node):
    try:
        with Given("I have some users and roles"):
            node.query("CREATE USER OR REPLACE user0 ON CLUSTER sharded_cluster")
            node.query("CREATE USER OR REPLACE user1")
            node.query("CREATE ROLE OR REPLACE role1")
        yield
    finally:
        with Finally("I drop the users and roles"):
            node.query("DROP USER IF EXISTS user0 ON CLUSTER sharded_cluster")
            node.query("DROP USER IF EXISTS user1")
            node.query("DROP ROLE IF EXISTS role1")


@TestOutline(Scenario)
@Examples("privilege on allow_column allow_introspection", [
    ("dictGet", ("db0.table0","db0.*","*.*","tb0","*"), False, False, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_DictGet("1.0"))),
    ("INTROSPECTION", ("*.*",), False, True, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_Introspection("1.0"))),
    ("SELECT", ("db0.table0","db0.*","*.*","tb0","*"), True, False, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_Select("1.0"))),
    ("INSERT",("db0.table0","db0.*","*.*","tb0","*"), True, False, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_Insert("1.0"))),
    ("ALTER",("db0.table0","db0.*","*.*","tb0","*"), False, False, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_Alter("1.0"))),
    ("CREATE",("db0.table0","db0.*","*.*","tb0","*"), False, False, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_Create("1.0"))),
    ("DROP",("db0.table0","db0.*","*.*","tb0","*"), False, False, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_Drop("1.0"))),
    ("TRUNCATE",("db0.table0","db0.*","*.*","tb0","*"), False, False, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_Truncate("1.0"))),
    ("OPTIMIZE",("db0.table0","db0.*","*.*","tb0","*"), False, False, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_Optimize("1.0"))),
    ("SHOW",("db0.table0","db0.*","*.*","tb0","*"), True, False, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_Show("1.0"))),
    ("KILL QUERY",("*.*",), False, False, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_KillQuery("1.0"))),
    ("ACCESS MANAGEMENT",("*.*",), False, False, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_AccessManagement("1.0"))),
    ("SYSTEM",("db0.table0","db0.*","*.*","tb0","*"), False, False, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_System("1.0"))),
    ("SOURCES",("*.*",), False, False, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_Sources("1.0"))),
    ("ALL",("*.*",), True, True, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_All("1.0"))),
    ("ALL PRIVILEGES",("*.*",), True, True, Requirements(RQ_SRS_006_RBAC_Revoke_Privilege_All("1.0"))), #alias for all
    ],)
def revoke_privileges(self, privilege, on, allow_column, allow_introspection, node="clickhouse1"):
    revoke_privilege(privilege=privilege, on=on, allow_column=allow_column, allow_introspection=allow_introspection, node=node)

@TestOutline(Scenario)
@Requirements([RQ_SRS_006_RBAC_Revoke_Privilege_Any("1.0") , RQ_SRS_006_RBAC_Revoke_Privilege_PrivelegeColumns("1.0")])
def revoke_privilege(self, privilege, on, allow_column, allow_introspection, node="clickhouse1"):
    node = self.context.cluster.node(node)
    for on_ in on:
        with When(f"I revoke {privilege} privilege from user on {on_}"):
            with setup(node):
                settings = []
                if allow_introspection:
                    settings.append(("allow_introspection_functions", 1))
                    node.query("SET allow_introspection_functions = 1")
                with When("I revoke privilege without columns"):
                    node.query(f"REVOKE {privilege} ON {on_} FROM user0", settings=settings)

                if allow_column and ('*' not in on_):
                    # Revoke column specific for some column 'x'
                    with When("I revoke privilege with columns"):
                        node.query(f"REVOKE {privilege}(x) ON {on_} FROM user0", settings=settings)

@TestFeature
@Name("revoke privilege")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check revoke privilege syntax.

    ```sql
    REVOKE [ON CLUSTER cluster_name] privilege
    [(column_name [,...])] [,...]
    ON {db.table|db.*|*.*|table|*}
    FROM {user | CURRENT_USER} [,...] | ALL | ALL EXCEPT {user | CURRENT_USER} [,...]
    ```
    """
    node = self.context.cluster.node(node)

    Scenario(run=revoke_privileges)

    with Scenario("I revoke privilege ON CLUSTER", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Privilege_Cluster("1.0"),
            RQ_SRS_006_RBAC_Revoke_Privilege_None("1.0")]):
        with setup(node):
            with When("I revoke privilege ON CLUSTER"):
                node.query("REVOKE ON CLUSTER sharded_cluster NONE FROM user0")

    with Scenario("I revoke privilege ON fake CLUSTER, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Privilege_Cluster("1.0"),
            RQ_SRS_006_RBAC_Revoke_Privilege_None("1.0")]):
        with setup(node):
            with When("I revoke privilege ON CLUSTER"):
                exitcode, message = errors.cluster_not_found("fake_cluster")
                node.query("REVOKE ON CLUSTER fake_cluster NONE FROM user0",
                            exitcode=exitcode, message=message)

    with Scenario("I revoke privilege from multiple users and roles", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Privilege_From("1.0"),
            RQ_SRS_006_RBAC_Revoke_Privilege_None("1.0")]):
        with setup(node):
            with When("I revoke privilege from multiple users"):
                node.query("REVOKE NONE FROM user0, user1, role1")

    with Scenario("I revoke privilege from current user", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Privilege_From("1.0"),
            RQ_SRS_006_RBAC_Revoke_Privilege_None("1.0")]):
        with setup(node):
            with When("I revoke privilege from current user"):
                node.query("REVOKE NONE FROM CURRENT_USER", settings = [("user","user0")])

    with Scenario("I revoke privilege from all users", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Privilege_From("1.0"),
            RQ_SRS_006_RBAC_Revoke_Privilege_None("1.0")]):
        with setup(node):
            with When("I revoke privilege from all users"):
                exitcode, message = errors.cannot_update_default()
                node.query("REVOKE NONE FROM ALL", exitcode=exitcode,message=message)

    with Scenario("I revoke privilege from default user", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Privilege_From("1.0"),
            RQ_SRS_006_RBAC_Revoke_Privilege_None("1.0")]):
        with setup(node):
            with When("I revoke privilege from default user"):
                exitcode, message = errors.cannot_update_default()
                node.query("REVOKE NONE FROM default", exitcode=exitcode,message=message)

    #By default, ClickHouse treats unnamed object as role
    with Scenario("I revoke privilege from nonexistent role, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Privilege_From("1.0"),
            RQ_SRS_006_RBAC_Revoke_Privilege_None("1.0")]):
        role = "role5"
        with Given(f"I ensure that role {role} does not exist"):
            node.query(f"DROP ROLE IF EXISTS {role}")
        with When(f"I revoke privilege from nonexistent role {role}"):
            exitcode, message = errors.role_not_found_in_disk(role)
            node.query(f"REVOKE NONE FROM {role}", exitcode=exitcode,message=message)

    with Scenario("I revoke privilege from ALL EXCEPT nonexistent role, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Privilege_From("1.0"),
            RQ_SRS_006_RBAC_Revoke_Privilege_None("1.0")]):
        role = "role5"
        with Given(f"I ensure that role {role} does not exist"):
            node.query(f"DROP ROLE IF EXISTS {role}")
        with When(f"I revoke privilege from nonexistent role {role}"):
            exitcode, message = errors.role_not_found_in_disk(role)
            node.query(f"REVOKE NONE FROM ALL EXCEPT {role}", exitcode=exitcode,message=message)

    with Scenario("I revoke privilege from all except some users and roles", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Privilege_From("1.0"),
            RQ_SRS_006_RBAC_Revoke_Privilege_None("1.0")]):
        with setup(node):
            with When("I revoke privilege all except some users"):
                node.query("REVOKE NONE FROM ALL EXCEPT default, user0, role1")

    with Scenario("I revoke privilege from all except current user", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Revoke_Privilege_From("1.0"),
            RQ_SRS_006_RBAC_Revoke_Privilege_None("1.0")]):
        with setup(node):
            with When("I revoke privilege from all except current user"):
                node.query("REVOKE NONE FROM ALL EXCEPT CURRENT_USER")