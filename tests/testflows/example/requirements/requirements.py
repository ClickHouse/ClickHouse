# These requirements were auto generated
# from software requirements specification (SRS)
# document by TestFlows v1.6.200627.1211752.
# Do not edit by hand but re-generate instead
# using 'tfs requirements generate' command.
from testflows.core import Requirement

RQ_CH_SRS001_Example = Requirement(
        name='RQ.CH-SRS001.Example',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        'This is a long description of the requirement that can include any\n'
        'relevant information. \n'
        '\n'
        'The one-line block that follows the requirement defines the `version` \n'
        'of the requirement. The version is controlled manually and is used\n'
        'to indicate material changes to the requirement that would \n'
        'require tests that cover this requirement to be updated.\n'
        '\n'
        'It is a good practice to use requirement names that are broken\n'
        'up into groups. It is not recommended to use only numbers\n'
        'because if the requirement must be moved the numbering will not match.\n'
        'Therefore, the requirement name should start with the group\n'
        'name which is then followed by a number if any. For example,\n'
        '\n'
        '    RQ.SRS001.Group.Subgroup.1\n'
        '\n'
        "To keep names short, try to use abbreviations for the requirement's group name.\n"
        ),
        link=None
    )

RQ_CH_SRS001_Example_Subgroup = Requirement(
        name='RQ.CH-SRS001.Example.Subgroup',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        'This an example of a sub-requirement of the [RQ.CH-SRS001.Example](#rqch-srs001example).\n'
        ),
        link=None
    )

RQ_CH_SRS001_Example_Select_1 = Requirement(
        name='RQ.CH-SRS001.Example.Select.1',
        version='1.0',
        priority=None,
        group=None,
        type=None,
        uid=None,
        description=(
        '[ClickHouse] SHALL return `1` when user executes query\n'
        '\n'
        '```sql\n'
        'SELECT 1\n'
        '```\n'
        ),
        link=None
    )
