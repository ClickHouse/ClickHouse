from testflows.core import *

@TestFeature
@Name("privileges")
def feature(self):
    Feature(run=load("privileges.insert", "feature"), flags=TE)
    Feature(run=load("privileges.select", "feature"), flags=TE)
