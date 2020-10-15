from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *

@TestFeature
@Name("alter row policy")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check alter row policy query syntax.

    ```sql
    ALTER [ROW] POLICY [IF EXISTS] name [ON CLUSTER cluster_name] ON [database.]table
    [RENAME TO new_name]
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def cleanup(policy):
        try:
            with Given("I have a row policy"):
                node.query(f"CREATE ROW POLICY {policy} ON default.foo")
            yield
        finally:
            with Finally("I drop the row policy"):
                node.query(f"DROP ROW POLICY IF EXISTS {policy} ON default.foo")

    def cleanup_policy(policy):
        with Given(f"I ensure that policy {policy} does not exist"):
            node.query(f"DROP ROW POLICY IF EXISTS {policy} ON default.foo")

    try:
        with Given("I have a table and some roles"):
            node.query(f"CREATE TABLE default.foo (x UInt64, y String) Engine=Memory")
            node.query(f"CREATE ROLE role0")
            node.query(f"CREATE ROLE role1")

        with Scenario("I alter row policy with no options", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy0"):
                with When("I alter row policy"):
                    node.query("ALTER ROW POLICY policy0 ON default.foo")

        with Scenario("I alter row policy using short syntax with no options", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy1"):
                with When("I alter row policy short form"):
                    node.query("ALTER POLICY policy1 ON default.foo")

        with Scenario("I alter row policy, does not exist, throws exception", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            policy = "policy2"
            cleanup_policy(policy)
            with When(f"I alter row policy {policy} that doesn't exist"):
                exitcode, message = errors.row_policy_not_found_in_disk(name=f"{policy} ON default.foo")
                node.query(f"ALTER ROW POLICY {policy} ON default.foo", exitcode=exitcode, message=message)
            del policy

        with Scenario("I alter row policy if exists", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_IfExists("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy2"):
                with When("I alter row policy using if exists"):
                    node.query("ALTER ROW POLICY IF EXISTS policy2 ON default.foo")

        with Scenario("I alter row policy if exists, policy does not exist", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_IfExists("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            policy = "policy2"
            cleanup_policy(policy)
            with When(f"I alter row policy {policy} that doesn't exist"):
                node.query(f"ALTER ROW POLICY IF EXISTS {policy} ON default.foo")
            del policy

        with Scenario("I alter row policy to rename, target available", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_Rename("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy3"):
                with When("I alter row policy with rename"):
                    node.query("ALTER ROW POLICY policy3 ON default.foo RENAME TO policy3")

        with Scenario("I alter row policy to rename, target unavailable", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_Rename("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy3"):
                new_policy = "policy4"
                try:
                    with Given(f"Ensure target name {new_policy} is NOT available"):
                        node.query(f"CREATE ROW POLICY IF NOT EXISTS {new_policy} ON default.foo")
                    with When(f"I try to rename to {new_policy}"):
                        exitcode, message = errors.cannot_rename_row_policy(name="policy3 ON default.foo",
                                                                            name_new=f"{new_policy} ON default.foo")
                        node.query(f"ALTER ROW POLICY policy3 ON default.foo RENAME TO {new_policy}", exitcode=exitcode, message=message)
                finally:
                    with Finally(f"I cleanup target name {new_policy}"):
                        node.query(f"DROP ROW POLICY IF EXISTS {new_policy} ON default.foo")
            del new_policy

        with Scenario("I alter row policy to permissive", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_Access_Permissive("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy4"):
                with When("I alter row policy as permissive"):
                    node.query("ALTER ROW POLICY policy4 ON default.foo AS PERMISSIVE")

        with Scenario("I alter row policy to restrictive", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_Access_Restrictive("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy5"):
                with When("I alter row policy as restrictive"):
                    node.query("ALTER ROW POLICY policy5 ON default.foo AS RESTRICTIVE")

        with Scenario("I alter row policy for select", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_ForSelect("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy6"):
                with When("I alter row policy using for select"):
                    node.query("ALTER ROW POLICY policy6 ON default.foo FOR SELECT USING x > 10")

        with Scenario("I alter row policy using condition", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_Condition("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy6"):
                with When("I alter row policy wtih condition"):
                    node.query("ALTER ROW POLICY policy6 ON default.foo USING x > 10")

        with Scenario("I alter row policy using condition none", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_Condition_None("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy7"):
                with When("I alter row policy using no condition"):
                    node.query("ALTER ROW POLICY policy7 ON default.foo USING NONE")

        with Scenario("I alter row policy to one role", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy8"):
                with When("I alter row policy to a role"):
                    node.query("ALTER ROW POLICY policy8 ON default.foo TO role0")

        with Scenario("I alter row policy to assign to role that does not exist, throws exception", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment("1.0")]):
            role = "role2"
            with cleanup("policy8a"):
                with Given(f"I drop {role} if it exists"):
                    node.query(f"DROP ROLE IF EXISTS {role}")
                with Then(f"I alter a row policy, assign to role {role}, which does not exist"):
                    exitcode, message = errors.role_not_found_in_disk(name=role)
                    node.query(f"ALTER ROW POLICY policy8a ON default.foo TO {role}", exitcode=exitcode, message=message)
            del role

        with Scenario("I alter row policy to assign to all excpet role that does not exist, throws exception", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment("1.0")]):
            role = "role2"
            with cleanup("policy8a"):
                with Given(f"I drop {role} if it exists"):
                    node.query(f"DROP ROLE IF EXISTS {role}")
                with Then(f"I alter a row policy, assign to all except role {role}, which does not exist"):
                    exitcode, message = errors.role_not_found_in_disk(name=role)
                    node.query(f"ALTER ROW POLICY policy8a ON default.foo TO ALL EXCEPT {role}", exitcode=exitcode, message=message)
            del role

        with Scenario("I alter row policy assigned to multiple roles", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy9"):
                with When("I alter row policy to multiple roles"):
                    node.query("ALTER ROW POLICY policy9 ON default.foo TO role0, role1")

        with Scenario("I alter row policy assigned to all", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment_All("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy10"):
                with When("I alter row policy to all"):
                    node.query("ALTER ROW POLICY policy10 ON default.foo TO ALL")

        with Scenario("I alter row policy assigned to all except one role", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment_AllExcept("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy11"):
                with When("I alter row policy to all except"):
                    node.query("ALTER ROW POLICY policy11 ON default.foo TO ALL EXCEPT role0")

        with Scenario("I alter row policy assigned to all except multiple roles", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment_AllExcept("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy12"):
                with When("I alter row policy to all except multiple roles"):
                    node.query("ALTER ROW POLICY policy12 ON default.foo TO ALL EXCEPT role0, role1")

        with Scenario("I alter row policy assigned to none", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_Assignment_None("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with cleanup("policy12"):
                with When("I alter row policy to no assignment"):
                    node.query("ALTER ROW POLICY policy12 ON default.foo TO NONE")

        # Official syntax: ON CLUSTER cluster_name ON database.table
        # Working syntax: both orderings of ON CLUSTER and TABLE clauses work

        with Scenario("I alter row policy on cluster", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_OnCluster("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            try:
                with Given("I have a row policy"):
                    node.query("CREATE ROW POLICY policy13 ON CLUSTER sharded_cluster ON default.foo")
                with When("I run alter row policy command"):
                    node.query("ALTER ROW POLICY policy13 ON CLUSTER sharded_cluster ON default.foo")
            finally:
                with Finally("I drop the row policy"):
                    node.query("DROP ROW POLICY IF EXISTS policy13 ON CLUSTER sharded_cluster ON default.foo")

        with Scenario("I alter row policy on fake cluster, throws exception", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_OnCluster("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            with When("I run alter row policy command"):
                exitcode, message = errors.cluster_not_found("fake_cluster")
                node.query("ALTER ROW POLICY policy13 ON CLUSTER fake_cluster ON default.foo", exitcode=exitcode, message=message)

        with Scenario("I alter row policy on cluster after table", flags=TE, requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Alter_OnCluster("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Alter_On("1.0")]):
            try:
                with Given("I have a row policy"):
                    node.query("CREATE ROW POLICY policy14 ON default.foo ON CLUSTER sharded_cluster")
                with When("I run create row policy command"):
                    node.query("ALTER ROW POLICY policy14 ON default.foo ON CLUSTER sharded_cluster")
            finally:
                with Finally("I drop the row policy"):
                    node.query("DROP ROW POLICY IF EXISTS policy14 ON default.foo ON CLUSTER sharded_cluster")
    finally:
        with Finally("I drop the table and the roles"):
            node.query(f"DROP TABLE IF EXISTS default.foo")
            node.query(f"DROP ROLE IF EXISTS role0, role1")