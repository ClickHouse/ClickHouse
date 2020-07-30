from contextlib import contextmanager

from testflows.core import *

from rbac.requirements import *
import rbac.tests.errors as errors

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
@Examples("privilege on allow_introspection", [
    ("dictGet", ("db0.table0","db0.*","*.*","tb0","*"), False, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_DictGet("1.0"))),
    ("INTROSPECTION", ("*.*",), True, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_Introspection("1.0"))),
    ("SELECT", ("db0.table0","db0.*","*.*","tb0","*"), False, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_Select("1.0"))),
    ("INSERT",("db0.table0","db0.*","*.*","tb0","*"), False, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_Insert("1.0"))),
    ("ALTER",("db0.table0","db0.*","*.*","tb0","*"), False, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_Alter("1.0"))),
    ("CREATE",("db0.table0","db0.*","*.*","tb0","*"), False, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_Create("1.0"))),
    ("DROP",("db0.table0","db0.*","*.*","tb0","*"), False, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_Drop("1.0"))),
    ("TRUNCATE",("db0.table0","db0.*","*.*","tb0","*"), False, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_Truncate("1.0"))),
    ("OPTIMIZE",("db0.table0","db0.*","*.*","tb0","*"), False, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_Optimize("1.0"))),
    ("SHOW",("db0.table0","db0.*","*.*","tb0","*"), False, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_Show("1.0"))),
    ("KILL QUERY",("*.*",), False, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_KillQuery("1.0"))),
    ("ACCESS MANAGEMENT",("*.*",), False, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_AccessManagement("1.0"))),
    ("SYSTEM",("db0.table0","db0.*","*.*","tb0","*"), False, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_System("1.0"))),
    ("SOURCES",("*.*",), False, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_Sources("1.0"))),
    ("ALL",("*.*",), True, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_All("1.0"))),
    ("ALL PRIVILEGES",("*.*",), True, Requirements(RQ_SRS_006_RBAC_Grant_Privilege_All("1.0"))), #alias for all
    ],)
def grant_privileges(self, privilege, on, allow_introspection, node="clickhouse1"):
    grant_privilege(privilege=privilege, on=on, allow_introspection=allow_introspection, node=node)
                    
@TestOutline(Scenario)
@Requirements(RQ_SRS_006_RBAC_Grant_Privilege_GrantOption("1.0"))
def grant_privilege(self, privilege, on, allow_introspection, node="clickhouse1"):   
    node = self.context.cluster.node(node)

    for on_ in on:
        with When(f"I grant {privilege} privilege to user on {on_}"):
            with setup(node):
                settings = []
                if allow_introspection:
                    settings.append(("allow_introspection_functions", 1))
                    node.query("SET allow_introspection_functions = 1")
                with When("I grant privilege without grant option"):
                    node.query(f"GRANT {privilege} ON {on_} TO user0", settings=settings)
                with When("I grant privilege with grant option"):
                    node.query(f"GRANT {privilege} ON {on_} TO user1 WITH GRANT OPTION", settings=settings)

                #grant column specific for some column 'x'
                with When("I grant privilege with columns"):
                    node.query(f"GRANT {privilege}(x) ON {on_} TO user0", settings=settings)
                    	
@TestFeature
@Name("grant privilege")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check grant privilege syntax.

    ```sql
    GRANT [ON CLUSTER cluster_name]
    privilege {SELECT | SELECT(columns) | INSERT | ALTER | CREATE | DROP | TRUNCATE | OPTIMIZE | SHOW | KILL QUERY | ACCESS MANAGEMENT | SYSTEM | INTROSPECTION | SOURCES | dictGet | NONE |ALL     [PRIVILEGES]} [, ...]
    ON {*.* | database.* | database.table | * | table}
    TO {user | role | CURRENT_USER} [,...]
    [WITH GRANT OPTION]
    ```
    """
    node = self.context.cluster.node(node)
    
    Scenario(run=grant_privileges)

    # with nonexistant object name, GRANT assumes type role
    with Scenario("I grant privilege to role that does not exist", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Privilege_None("1.0")]):
        with Given("I ensure that role does not exist"):
            node.query("DROP ROLE IF EXISTS role0")
        with When("I grant privilege ON CLUSTER"):
            exitcode, message = errors.role_not_found_in_disk(name="role0")
            node.query("GRANT NONE TO role0", exitcode=exitcode, message=message)
    
    with Scenario("I grant privilege ON CLUSTER", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Privilege_OnCluster("1.0"), 
            RQ_SRS_006_RBAC_Grant_Privilege_None("1.0")]):
        with setup(node):
            with When("I grant privilege ON CLUSTER"):
                node.query("GRANT ON CLUSTER sharded_cluster NONE TO user0")
    
    with Scenario("I grant privilege on fake cluster, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Privilege_OnCluster("1.0")]):
        with setup(node):
            with When("I grant privilege ON CLUSTER"):
                exitcode, message = errors.cluster_not_found("fake_cluster")
                node.query("GRANT ON CLUSTER fake_cluster NONE TO user0", exitcode=exitcode, message=message)
                
    with Scenario("I grant privilege to multiple users and roles", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Privilege_To("1.0"), 
            RQ_SRS_006_RBAC_Grant_Privilege_None("1.0")]):
        with setup(node):
            with When("I grant privilege to several users"):
                node.query("GRANT NONE TO user0, user1, role1")
                
    with Scenario("I grant privilege to current user", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Privilege_ToCurrentUser("1.0"), 
            RQ_SRS_006_RBAC_Grant_Privilege_None("1.0")]):
        with setup(node):
            with When("I grant privilege to current user"):
                node.query("GRANT NONE TO CURRENT_USER", settings = [("user","user0")])

    with Scenario("I grant privilege NONE to default user, throws exception", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Privilege_ToCurrentUser("1.0"), 
            RQ_SRS_006_RBAC_Grant_Privilege_None("1.0")]):
        with setup(node):
            with When("I grant privilege to current user"):
                exitcode, message = errors.cannot_update_default()
                node.query("GRANT NONE TO CURRENT_USER", exitcode=exitcode, message=message)
                
    with Scenario("I grant privilege with grant option", flags=TE, requirements=[
            RQ_SRS_006_RBAC_Grant_Privilege_GrantOption("1.0"), 
            RQ_SRS_006_RBAC_Grant_Privilege_None("1.0")]):
        with setup(node):
            with When("I grant privilege with grant option"):
               node.query("GRANT NONE ON *.* TO user0 WITH GRANT OPTION")
