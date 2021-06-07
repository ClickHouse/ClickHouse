from contextlib import contextmanager

from testflows.core import *

import rbac.helper.errors as errors
from rbac.requirements import *

@TestFeature
@Name("create row policy")
@Args(format_description=False)
def feature(self, node="clickhouse1"):
    """Check create row policy query syntax.

    ```sql
    CREATE [ROW] POLICY [IF NOT EXISTS | OR REPLACE] policy_name [ON CLUSTER cluster_name] ON [db.]table
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING condition]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
    ```
    """
    node = self.context.cluster.node(node)

    @contextmanager
    def cleanup(policy, on="default.foo"):
        try:
            with Given(f"I ensure the row policy does not already exist on {on}"):
                node.query(f"DROP ROW POLICY IF EXISTS {policy} ON {on}")
            yield
        finally:
            with Finally(f"I drop the row policy on {on}"):
                node.query(f"DROP ROW POLICY IF EXISTS {policy} ON {on}")

    def create_policy(policy, on="default.foo"):
        with Given(f"I ensure I do have policy {policy} on {on}"):
                node.query(f"CREATE ROW POLICY OR REPLACE {policy} ON {on}")

    try:
        with Given("I have a table and some roles"):
            node.query(f"CREATE TABLE default.foo (x UInt64, y String) Engine=Memory")
            node.query(f"CREATE ROLE role0")
            node.query(f"CREATE ROLE role1")

        with Scenario("I create row policy with no options", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            with cleanup("policy0"):
                with When("I create row policy"):
                    node.query("CREATE ROW POLICY policy0 ON default.foo")

        with Scenario("I create row policy using short syntax with no options", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            with cleanup("policy1"):
                with When("I create row policy short form"):
                    node.query("CREATE POLICY policy1 ON default.foo")

        with Scenario("I create row policy that already exists, throws exception", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            policy = "policy0"
            with cleanup(policy):
                create_policy(policy)
                with When(f"I create row policy {policy}"):
                    exitcode, message = errors.cannot_insert_row_policy(name=f"{policy} ON default.foo")
                    node.query(f"CREATE ROW POLICY {policy} ON default.foo", exitcode=exitcode, message=message)
            del policy

        with Scenario("I create row policy if not exists, policy does not exist", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_IfNotExists("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            with cleanup("policy2"):
                with When("I create row policy with if not exists"):
                    node.query("CREATE ROW POLICY IF NOT EXISTS policy2 ON default.foo")

        with Scenario("I create row policy if not exists, policy does exist", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_IfNotExists("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            policy = "policy2"
            with cleanup(policy):
                create_policy(policy)
                with When(f"I create row policy {policy} with if not exists"):
                    node.query(f"CREATE ROW POLICY IF NOT EXISTS {policy} ON default.foo")
            del policy

        with Scenario("I create row policy or replace, policy does not exist", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_Replace("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            with cleanup("policy3"):
                with When("I create row policy with or replace"):
                    node.query("CREATE ROW POLICY OR REPLACE policy3 ON default.foo")

        with Scenario("I create row policy or replace, policy does exist", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_Replace("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            policy = "policy3"
            with cleanup(policy):
                create_policy(policy)
                with When(f"I create row policy {policy} with or replace"):
                    node.query(f"CREATE ROW POLICY OR REPLACE {policy} ON default.foo")
            del policy

        with Scenario("I create row policy as permissive", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_Access_Permissive("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            with cleanup("policy4"):
                with When("I create row policy as permissive"):
                    node.query("CREATE ROW POLICY policy4 ON default.foo AS PERMISSIVE")

        with Scenario("I create row policy as restrictive", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_Access_Restrictive("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            with cleanup("policy5"):
                with When("I create row policy as restrictive"):
                    node.query("CREATE ROW POLICY policy5 ON default.foo AS RESTRICTIVE")

        with Scenario("I create row policy for select", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_ForSelect("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_Condition("1.0")]):
            with cleanup("policy6"):
                with When("I create row policy with for select"):
                    node.query("CREATE ROW POLICY policy6 ON default.foo FOR SELECT USING x > 10")

        with Scenario("I create row policy using condition", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_Condition("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            with cleanup("policy6"):
                with When("I create row policy with condition"):
                    node.query("CREATE ROW POLICY policy6 ON default.foo USING x > 10")

        with Scenario("I create row policy assigned to one role", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_Assignment("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            with cleanup("policy7"):
                with When("I create row policy for one role"):
                    node.query("CREATE ROW POLICY policy7 ON default.foo TO role0")

        with Scenario("I create row policy to assign to role that does not exist, throws exception", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_Assignment("1.0")]):
            role = "role2"
            with cleanup("policy8a"):
                with Given(f"I drop {role} if it exists"):
                    node.query(f"DROP ROLE IF EXISTS {role}")
                with Then(f"I create a row policy, assign to role {role}, which does not exist"):
                    exitcode, message = errors.role_not_found_in_disk(name=role)
                    node.query(f"CREATE ROW POLICY policy8a ON default.foo TO {role}", exitcode=exitcode, message=message)
            del role

        with Scenario("I create row policy to assign to all excpet role that does not exist, throws exception", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_Assignment("1.0")]):
            role = "role2"
            with cleanup("policy8a"):
                with Given(f"I drop {role} if it exists"):
                    node.query(f"DROP ROLE IF EXISTS {role}")
                with Then(f"I create a row policy, assign to all except role {role}, which does not exist"):
                    exitcode, message = errors.role_not_found_in_disk(name=role)
                    node.query(f"CREATE ROW POLICY policy8a ON default.foo TO ALL EXCEPT {role}", exitcode=exitcode, message=message)
            del role

        with Scenario("I create row policy assigned to multiple roles", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_Assignment("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            with cleanup("policy8b"):
                with When("I create row policy for multiple roles"):
                    node.query("CREATE ROW POLICY policy8b ON default.foo TO role0, role1")

        with Scenario("I create row policy assigned to all", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_Assignment_All("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            with cleanup("policy9"):
                with When("I create row policy for all"):
                    node.query("CREATE ROW POLICY policy9 ON default.foo TO ALL")

        with Scenario("I create row policy assigned to all except one role", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_Assignment_AllExcept("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            with cleanup("policy10"):
                with When("I create row policy for all except one"):
                    node.query("CREATE ROW POLICY policy10 ON default.foo TO ALL EXCEPT role0")

        with Scenario("I create row policy assigned to all except multiple roles", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_Assignment_AllExcept("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            with cleanup("policy11"):
                with When("I create row policy for all except multiple roles"):
                    node.query("CREATE ROW POLICY policy11 ON default.foo TO ALL EXCEPT role0, role1")

        with Scenario("I create row policy assigned to none", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_Assignment_None("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            with cleanup("policy11"):
                with When("I create row policy for none"):
                    node.query("CREATE ROW POLICY policy11 ON default.foo TO NONE")

        with Scenario("I create row policy on cluster", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_OnCluster("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            try:
                with When("I run create row policy command on cluster"):
                    node.query("CREATE ROW POLICY policy12 ON CLUSTER sharded_cluster ON default.foo")
            finally:
                with Finally("I drop the row policy from cluster"):
                    node.query("DROP ROW POLICY IF EXISTS policy12 ON default.foo ON CLUSTER sharded_cluster")

        with Scenario("I create row policy on fake cluster, throws exception", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_OnCluster("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            with When("I run create row policy command"):
                exitcode, message = errors.cluster_not_found("fake_cluster")
                node.query("CREATE ROW POLICY policy13 ON CLUSTER fake_cluster ON default.foo", exitcode=exitcode, message=message)

        with Scenario("I create row policy on cluster after table", requirements=[
                RQ_SRS_006_RBAC_RowPolicy_Create_OnCluster("1.0"),
                RQ_SRS_006_RBAC_RowPolicy_Create_On("1.0")]):
            try:
                with When("I run create row policy command on cluster"):
                    node.query("CREATE ROW POLICY policy12 ON default.foo ON CLUSTER sharded_cluster")
            finally:
                with Finally("I drop the row policy from cluster"):
                    node.query("DROP ROW POLICY IF EXISTS policy12 ON default.foo ON CLUSTER sharded_cluster")
    finally:
        with Finally("I drop the table and the roles"):
            node.query(f"DROP TABLE IF EXISTS default.foo")
            node.query(f"DROP ROLE IF EXISTS role0, role1")