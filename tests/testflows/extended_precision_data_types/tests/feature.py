from testflows.core import *
from testflows.core.name import basename, parentname
from testflows._core.testtype import TestSubType


@TestFeature
@Name("tests")
def feature(self):
    """Check functions with Int128, Int256, UInt256, and Decimal256."""
    Feature(run=load("extended_precision_data_types.tests.conversion", "feature"))
    Feature(run=load("extended_precision_data_types.tests.arithmetic", "feature"))
    Feature(run=load("extended_precision_data_types.tests.array_tuple_map", "feature"))
    Feature(run=load("extended_precision_data_types.tests.comparison", "feature"))
    Feature(run=load("extended_precision_data_types.tests.logical", "feature"))
    Feature(run=load("extended_precision_data_types.tests.mathematical", "feature"))
    Feature(run=load("extended_precision_data_types.tests.rounding", "feature"))
    Feature(run=load("extended_precision_data_types.tests.bit", "feature"))
    Feature(run=load("extended_precision_data_types.tests.null", "feature"))
    Feature(run=load("extended_precision_data_types.tests.table", "feature"))
