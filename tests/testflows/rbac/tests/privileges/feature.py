from testflows.core import *

@TestFeature
@Name("privileges")
def feature(self):
    Feature(run=load("rbac.tests.privileges.insert", "feature"), flags=TE)