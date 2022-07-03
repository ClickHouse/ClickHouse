# -*- coding: utf-8 -*-
import time

from testflows.core import *
from testflows.asserts import error

from map_type.requirements import *
from map_type.tests.common import *


@TestOutline
def select_map(self, map, output, exitcode=0, message=None):
    """Create a map using select statement."""
    node = self.context.node

    with When("I create a map using select", description=map):
        r = node.query(f"SELECT {map}", exitcode=exitcode, message=message)

    with Then("I expect output to match", description=output):
        assert r.output == output, error()


@TestOutline
def table_map(
    self,
    type,
    data,
    select,
    filter,
    exitcode,
    message,
    check_insert=False,
    order_by=None,
):
    """Check using a map column in a table."""
    uid = getuid()
    node = self.context.node

    if order_by is None:
        order_by = "m"

    with Given(f"table definition with {type}"):
        sql = (
            "CREATE TABLE {name} (m "
            + type
            + ") ENGINE = MergeTree() ORDER BY "
            + order_by
        )

    with And(f"I create a table", description=sql):
        table = create_table(name=uid, statement=sql)

    with When("I insert data into the map column"):
        if check_insert:
            node.query(
                f"INSERT INTO {table} VALUES {data}", exitcode=exitcode, message=message
            )
        else:
            node.query(f"INSERT INTO {table} VALUES {data}")

    if not check_insert:
        with And("I try to read from the table"):
            node.query(
                f"SELECT {select} FROM {table} WHERE {filter} FORMAT JSONEachRow",
                exitcode=exitcode,
                message=message,
            )


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Key_String("1.0"))
@Examples(
    "map output",
    [
        ("map('',1)", "{'':1}", Name("empty string")),
        ("map('hello',1)", "{'hello':1}", Name("non-empty string")),
        ("map('Gãńdåłf_Thê_Gręât',1)", "{'Gãńdåłf_Thê_Gręât':1}", Name("utf-8 string")),
        ("map('hello there',1)", "{'hello there':1}", Name("multi word string")),
        ("map('hello',1,'there',2)", "{'hello':1,'there':2}", Name("multiple keys")),
        ("map(toString(1),1)", "{'1':1}", Name("toString")),
        ("map(toFixedString('1',1),1)", "{'1':1}", Name("toFixedString")),
        ("map(toNullable('1'),1)", "{'1':1}", Name("Nullable")),
        ("map(toNullable(NULL),1)", "{NULL:1}", Name("Nullable(NULL)")),
        ("map(toLowCardinality('1'),1)", "{'1':1}", Name("LowCardinality(String)")),
        (
            "map(toLowCardinality(toFixedString('1',1)),1)",
            "{'1':1}",
            Name("LowCardinality(FixedString)"),
        ),
    ],
    row_format="%20s,%20s",
)
def select_map_with_key_string(self, map, output):
    """Create a map using select that has key string type."""
    select_map(map=map, output=output)


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Value_String("1.0"))
@Examples(
    "map output",
    [
        ("map('key','')", "{'key':''}", Name("empty string")),
        ("map('key','hello')", "{'key':'hello'}", Name("non-empty string")),
        (
            "map('key','Gãńdåłf_Thê_Gręât')",
            "{'key':'Gãńdåłf_Thê_Gręât'}",
            Name("utf-8 string"),
        ),
        (
            "map('key','hello there')",
            "{'key':'hello there'}",
            Name("multi word string"),
        ),
        (
            "map('key','hello','key2','there')",
            "{'key':'hello','key2':'there'}",
            Name("multiple keys"),
        ),
        ("map('key',toString(1))", "{'key':'1'}", Name("toString")),
        ("map('key',toFixedString('1',1))", "{'key':'1'}", Name("toFixedString")),
        ("map('key',toNullable('1'))", "{'key':'1'}", Name("Nullable")),
        ("map('key',toNullable(NULL))", "{'key':NULL}", Name("Nullable(NULL)")),
        (
            "map('key',toLowCardinality('1'))",
            "{'key':'1'}",
            Name("LowCardinality(String)"),
        ),
        (
            "map('key',toLowCardinality(toFixedString('1',1)))",
            "{'key':'1'}",
            Name("LowCardinality(FixedString)"),
        ),
    ],
    row_format="%20s,%20s",
)
def select_map_with_value_string(self, map, output):
    """Create a map using select that has value string type."""
    select_map(map=map, output=output)


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Value_Array("1.0"))
@Examples(
    "map output",
    [
        ("map('key',[])", "{'key':[]}", Name("empty Array")),
        ("map('key',[1,2,3])", "{'key':[1,2,3]}", Name("non-empty array of ints")),
        (
            "map('key',['1','2','3'])",
            "{'key':['1','2','3']}",
            Name("non-empty array of strings"),
        ),
        (
            "map('key',[map(1,2),map(2,3)])",
            "{'key':[{1:2},{2:3}]}",
            Name("non-empty array of maps"),
        ),
        (
            "map('key',[map(1,[map(1,[1])]),map(2,[map(2,[3])])])",
            "{'key':[{1:[{1:[1]}]},{2:[{2:[3]}]}]}",
            Name("non-empty array of maps of array of maps"),
        ),
    ],
)
def select_map_with_value_array(self, map, output):
    """Create a map using select that has value array type."""
    select_map(map=map, output=output)


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Value_Integer("1.0"))
@Examples(
    "map output",
    [
        ("(map(1,127,2,0,3,-128))", "{1:127,2:0,3:-128}", Name("Int8")),
        ("(map(1,0,2,255))", "{1:0,2:255}", Name("UInt8")),
        ("(map(1,32767,2,0,3,-32768))", "{1:32767,2:0,3:-32768}", Name("Int16")),
        ("(map(1,0,2,65535))", "{1:0,2:65535}", Name("UInt16")),
        (
            "(map(1,2147483647,2,0,3,-2147483648))",
            "{1:2147483647,2:0,3:-2147483648}",
            Name("Int32"),
        ),
        ("(map(1,0,2,4294967295))", "{1:0,2:4294967295}", Name("UInt32")),
        (
            "(map(1,9223372036854775807,2,0,3,-9223372036854775808))",
            '{1:"9223372036854775807",2:"0",3:"-9223372036854775808"}',
            Name("Int64"),
        ),
        (
            "(map(1,0,2,18446744073709551615))",
            "{1:0,2:18446744073709551615}",
            Name("UInt64"),
        ),
        (
            "(map(1,170141183460469231731687303715884105727,2,0,3,-170141183460469231731687303715884105728))",
            "{1:1.7014118346046923e38,2:0,3:-1.7014118346046923e38}",
            Name("Int128"),
        ),
        (
            "(map(1,57896044618658097711785492504343953926634992332820282019728792003956564819967,2,0,3,-57896044618658097711785492504343953926634992332820282019728792003956564819968))",
            "{1:5.78960446186581e76,2:0,3:-5.78960446186581e76}",
            Name("Int256"),
        ),
        (
            "(map(1,0,2,115792089237316195423570985008687907853269984665640564039457584007913129639935))",
            "{1:0,2:1.157920892373162e77}",
            Name("UInt256"),
        ),
        ("(map(1,toNullable(1)))", "{1:1}", Name("toNullable")),
        ("(map(1,toNullable(NULL)))", "{1:NULL}", Name("toNullable(NULL)")),
    ],
)
def select_map_with_value_integer(self, map, output):
    """Create a map using select that has value integer type."""
    select_map(map=map, output=output)


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Key_Integer("1.0"))
@Examples(
    "map output",
    [
        ("(map(127,1,0,1,-128,1))", "{127:1,0:1,-128:1}", Name("Int8")),
        ("(map(0,1,255,1))", "{0:1,255:1}", Name("UInt8")),
        ("(map(32767,1,0,1,-32768,1))", "{32767:1,0:1,-32768:1}", Name("Int16")),
        ("(map(0,1,65535,1))", "{0:1,65535:1}", Name("UInt16")),
        (
            "(map(2147483647,1,0,1,-2147483648,1))",
            "{2147483647:1,0:1,-2147483648:1}",
            Name("Int32"),
        ),
        ("(map(0,1,4294967295,1))", "{0:1,4294967295:1}", Name("UInt32")),
        (
            "(map(9223372036854775807,1,0,1,-9223372036854775808,1))",
            '{"9223372036854775807":1,"0":1,"-9223372036854775808":1}',
            Name("Int64"),
        ),
        (
            "(map(0,1,18446744073709551615,1))",
            "{0:1,18446744073709551615:1}",
            Name("UInt64"),
        ),
        (
            "(map(170141183460469231731687303715884105727,1,0,1,-170141183460469231731687303715884105728,1))",
            "{1.7014118346046923e38:1,0:1,-1.7014118346046923e38:1}",
            Name("Int128"),
        ),
        (
            "(map(57896044618658097711785492504343953926634992332820282019728792003956564819967,1,0,1,-57896044618658097711785492504343953926634992332820282019728792003956564819968,1))",
            "{5.78960446186581e76:1,0:1,-5.78960446186581e76:1}",
            Name("Int256"),
        ),
        (
            "(map(0,1,115792089237316195423570985008687907853269984665640564039457584007913129639935,1))",
            "{0:1,1.157920892373162e77:1}",
            Name("UInt256"),
        ),
        ("(map(toNullable(1),1))", "{1:1}", Name("toNullable")),
        ("(map(toNullable(NULL),1))", "{NULL:1}", Name("toNullable(NULL)")),
    ],
)
def select_map_with_key_integer(self, map, output):
    """Create a map using select that has key integer type."""
    select_map(map=map, output=output)


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Key_String("1.0"))
@Examples(
    "type data output",
    [
        (
            "Map(String, Int8)",
            "('2020-01-01', map('',1))",
            '{"d":"2020-01-01","m":{"":1}}',
            Name("empty string"),
        ),
        (
            "Map(String, Int8)",
            "('2020-01-01', map('hello',1))",
            '{"d":"2020-01-01","m":{"hello":1}}',
            Name("non-empty string"),
        ),
        (
            "Map(String, Int8)",
            "('2020-01-01', map('Gãńdåłf_Thê_Gręât',1))",
            '{"d":"2020-01-01","m":{"Gãńdåłf_Thê_Gręât":1}}',
            Name("utf-8 string"),
        ),
        (
            "Map(String, Int8)",
            "('2020-01-01', map('hello there',1))",
            '{"d":"2020-01-01","m":{"hello there":1}}',
            Name("multi word string"),
        ),
        (
            "Map(String, Int8)",
            "('2020-01-01', map('hello',1,'there',2))",
            '{"d":"2020-01-01","m":{"hello":1,"there":2}}',
            Name("multiple keys"),
        ),
        (
            "Map(String, Int8)",
            "('2020-01-01', map(toString(1),1))",
            '{"d":"2020-01-01","m":{"1":1}}',
            Name("toString"),
        ),
        (
            "Map(FixedString(1), Int8)",
            "('2020-01-01', map(toFixedString('1',1),1))",
            '{"d":"2020-01-01","m":{"1":1}}',
            Name("FixedString"),
        ),
        (
            "Map(Nullable(String), Int8)",
            "('2020-01-01', map(toNullable('1'),1))",
            '{"d":"2020-01-01","m":{"1":1}}',
            Name("Nullable"),
        ),
        (
            "Map(Nullable(String), Int8)",
            "('2020-01-01', map(toNullable(NULL),1))",
            '{"d":"2020-01-01","m":{null:1}}',
            Name("Nullable(NULL)"),
        ),
        (
            "Map(LowCardinality(String), Int8)",
            "('2020-01-01', map(toLowCardinality('1'),1))",
            '{"d":"2020-01-01","m":{"1":1}}',
            Name("LowCardinality(String)"),
        ),
        (
            "Map(LowCardinality(String), Int8)",
            "('2020-01-01', map('1',1))",
            '{"d":"2020-01-01","m":{"1":1}}',
            Name("LowCardinality(String) cast from String"),
        ),
        (
            "Map(LowCardinality(String), LowCardinality(String))",
            "('2020-01-01', map('1','1'))",
            '{"d":"2020-01-01","m":{"1":"1"}}',
            Name("LowCardinality(String) for key and value"),
        ),
        (
            "Map(LowCardinality(FixedString(1)), Int8)",
            "('2020-01-01', map(toLowCardinality(toFixedString('1',1)),1))",
            '{"d":"2020-01-01","m":{"1":1}}',
            Name("LowCardinality(FixedString)"),
        ),
    ],
)
def table_map_with_key_string(self, type, data, output):
    """Check what values we can insert into map type column with key string."""
    insert_into_table(type=type, data=data, output=output)


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Key_String("1.0"))
@Examples(
    "type data output select",
    [
        (
            "Map(String, Int8)",
            "('2020-01-01', map('',1))",
            '{"m":1}',
            "m[''] AS m",
            Name("empty string"),
        ),
        (
            "Map(String, Int8)",
            "('2020-01-01', map('hello',1))",
            '{"m":1}',
            "m['hello'] AS m",
            Name("non-empty string"),
        ),
        (
            "Map(String, Int8)",
            "('2020-01-01', map('Gãńdåłf_Thê_Gręât',1))",
            '{"m":1}',
            "m['Gãńdåłf_Thê_Gręât'] AS m",
            Name("utf-8 string"),
        ),
        (
            "Map(String, Int8)",
            "('2020-01-01', map('hello there',1))",
            '{"m":1}',
            "m['hello there'] AS m",
            Name("multi word string"),
        ),
        (
            "Map(String, Int8)",
            "('2020-01-01', map('hello',1,'there',2))",
            '{"m":1}',
            "m['hello'] AS m",
            Name("multiple keys"),
        ),
        (
            "Map(String, Int8)",
            "('2020-01-01', map(toString(1),1))",
            '{"m":1}',
            "m['1'] AS m",
            Name("toString"),
        ),
        (
            "Map(FixedString(1), Int8)",
            "('2020-01-01', map(toFixedString('1',1),1))",
            '{"m":1}',
            "m['1'] AS m",
            Name("FixedString"),
        ),
        (
            "Map(Nullable(String), Int8)",
            "('2020-01-01', map(toNullable('1'),1))",
            '{"m":1}}',
            "m['1'] AS m",
            Name("Nullable"),
        ),
        (
            "Map(Nullable(String), Int8)",
            "('2020-01-01', map(toNullable(NULL),1))",
            '{"m":1}',
            "m[null] AS m",
            Name("Nullable(NULL)"),
        ),
        (
            "Map(LowCardinality(String), Int8)",
            "('2020-01-01', map(toLowCardinality('1'),1))",
            '{"m":1}}',
            "m['1'] AS m",
            Name("LowCardinality(String)"),
        ),
        (
            "Map(LowCardinality(String), Int8)",
            "('2020-01-01', map('1',1))",
            '{"m":1}',
            "m['1'] AS m",
            Name("LowCardinality(String) cast from String"),
        ),
        (
            "Map(LowCardinality(String), LowCardinality(String))",
            "('2020-01-01', map('1','1'))",
            '{"m":"1"}',
            "m['1'] AS m",
            Name("LowCardinality(String) for key and value"),
        ),
        (
            "Map(LowCardinality(FixedString(1)), Int8)",
            "('2020-01-01', map(toLowCardinality(toFixedString('1',1)),1))",
            '{"m":1}',
            "m['1'] AS m",
            Name("LowCardinality(FixedString)"),
        ),
    ],
)
def table_map_select_key_with_key_string(self, type, data, output, select):
    """Check what values we can insert into map type column with key string and if key can be selected."""
    insert_into_table(type=type, data=data, output=output, select=select)


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Value_String("1.0"))
@Examples(
    "type data output",
    [
        (
            "Map(String, String)",
            "('2020-01-01', map('key',''))",
            '{"d":"2020-01-01","m":{"key":""}}',
            Name("empty string"),
        ),
        (
            "Map(String, String)",
            "('2020-01-01', map('key','hello'))",
            '{"d":"2020-01-01","m":{"key":"hello"}}',
            Name("non-empty string"),
        ),
        (
            "Map(String, String)",
            "('2020-01-01', map('key','Gãńdåłf_Thê_Gręât'))",
            '{"d":"2020-01-01","m":{"key":"Gãńdåłf_Thê_Gręât"}}',
            Name("utf-8 string"),
        ),
        (
            "Map(String, String)",
            "('2020-01-01', map('key', 'hello there'))",
            '{"d":"2020-01-01","m":{"key":"hello there"}}',
            Name("multi word string"),
        ),
        (
            "Map(String, String)",
            "('2020-01-01', map('key','hello','key2','there'))",
            '{"d":"2020-01-01","m":{"key":"hello","key2":"there"}}',
            Name("multiple keys"),
        ),
        (
            "Map(String, String)",
            "('2020-01-01', map('key', toString(1)))",
            '{"d":"2020-01-01","m":{"key":"1"}}',
            Name("toString"),
        ),
        (
            "Map(String, FixedString(1))",
            "('2020-01-01', map('key',toFixedString('1',1)))",
            '{"d":"2020-01-01","m":{"key":"1"}}',
            Name("FixedString"),
        ),
        (
            "Map(String, Nullable(String))",
            "('2020-01-01', map('key',toNullable('1')))",
            '{"d":"2020-01-01","m":{"key":"1"}}',
            Name("Nullable"),
        ),
        (
            "Map(String, Nullable(String))",
            "('2020-01-01', map('key',toNullable(NULL)))",
            '{"d":"2020-01-01","m":{"key":null}}',
            Name("Nullable(NULL)"),
        ),
        (
            "Map(String, LowCardinality(String))",
            "('2020-01-01', map('key',toLowCardinality('1')))",
            '{"d":"2020-01-01","m":{"key":"1"}}',
            Name("LowCardinality(String)"),
        ),
        (
            "Map(String, LowCardinality(String))",
            "('2020-01-01', map('key','1'))",
            '{"d":"2020-01-01","m":{"key":"1"}}',
            Name("LowCardinality(String) cast from String"),
        ),
        (
            "Map(LowCardinality(String), LowCardinality(String))",
            "('2020-01-01', map('1','1'))",
            '{"d":"2020-01-01","m":{"1":"1"}}',
            Name("LowCardinality(String) for key and value"),
        ),
        (
            "Map(String, LowCardinality(FixedString(1)))",
            "('2020-01-01', map('key',toLowCardinality(toFixedString('1',1))))",
            '{"d":"2020-01-01","m":{"key":"1"}}',
            Name("LowCardinality(FixedString)"),
        ),
    ],
)
def table_map_with_value_string(self, type, data, output):
    """Check what values we can insert into map type column with value string."""
    insert_into_table(type=type, data=data, output=output)


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Value_String("1.0"))
@Examples(
    "type data output",
    [
        (
            "Map(String, String)",
            "('2020-01-01', map('key',''))",
            '{"m":""}',
            Name("empty string"),
        ),
        (
            "Map(String, String)",
            "('2020-01-01', map('key','hello'))",
            '{"m":"hello"}',
            Name("non-empty string"),
        ),
        (
            "Map(String, String)",
            "('2020-01-01', map('key','Gãńdåłf_Thê_Gręât'))",
            '{"m":"Gãńdåłf_Thê_Gręât"}',
            Name("utf-8 string"),
        ),
        (
            "Map(String, String)",
            "('2020-01-01', map('key', 'hello there'))",
            '{"m":"hello there"}',
            Name("multi word string"),
        ),
        (
            "Map(String, String)",
            "('2020-01-01', map('key','hello','key2','there'))",
            '{"m":"hello"}',
            Name("multiple keys"),
        ),
        (
            "Map(String, String)",
            "('2020-01-01', map('key', toString(1)))",
            '{"m":"1"}',
            Name("toString"),
        ),
        (
            "Map(String, FixedString(1))",
            "('2020-01-01', map('key',toFixedString('1',1)))",
            '{"m":"1"}',
            Name("FixedString"),
        ),
        (
            "Map(String, Nullable(String))",
            "('2020-01-01', map('key',toNullable('1')))",
            '{"m":"1"}',
            Name("Nullable"),
        ),
        (
            "Map(String, Nullable(String))",
            "('2020-01-01', map('key',toNullable(NULL)))",
            '{"m":null}',
            Name("Nullable(NULL)"),
        ),
        (
            "Map(String, LowCardinality(String))",
            "('2020-01-01', map('key',toLowCardinality('1')))",
            '{"m":"1"}',
            Name("LowCardinality(String)"),
        ),
        (
            "Map(String, LowCardinality(String))",
            "('2020-01-01', map('key','1'))",
            '{"m":"1"}',
            Name("LowCardinality(String) cast from String"),
        ),
        (
            "Map(LowCardinality(String), LowCardinality(String))",
            "('2020-01-01', map('key','1'))",
            '{"m":"1"}',
            Name("LowCardinality(String) for key and value"),
        ),
        (
            "Map(String, LowCardinality(FixedString(1)))",
            "('2020-01-01', map('key',toLowCardinality(toFixedString('1',1))))",
            '{"m":"1"}',
            Name("LowCardinality(FixedString)"),
        ),
    ],
)
def table_map_select_key_with_value_string(self, type, data, output):
    """Check what values we can insert into map type column with value string and if it can be selected by key."""
    insert_into_table(type=type, data=data, output=output, select="m['key'] AS m")


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Value_Integer("1.0"))
@Examples(
    "type data output",
    [
        (
            "Map(Int8, Int8)",
            "('2020-01-01', map(1,127,2,0,3,-128))",
            '{"d":"2020-01-01","m":{"1":127,"2":0,"3":-128}}',
            Name("Int8"),
        ),
        (
            "Map(Int8, UInt8)",
            "('2020-01-01', map(1,0,2,255))",
            '{"d":"2020-01-01","m":{"1":0,"2":255}}',
            Name("UInt8"),
        ),
        (
            "Map(Int8, Int16)",
            "('2020-01-01', map(1,127,2,0,3,-128))",
            '{"d":"2020-01-01","m":{"1":32767,"2":0,"3":-32768}}',
            Name("Int16"),
        ),
        (
            "Map(Int8, UInt16)",
            "('2020-01-01', map(1,0,2,65535))",
            '{"d":"2020-01-01","m":{"1":0,"2":65535}}',
            Name("UInt16"),
        ),
        (
            "Map(Int8, Int32)",
            "('2020-01-01', map(1,127,2,0,3,-128))",
            '{"d":"2020-01-01","m":{"1":2147483647,"2":0,"3":-2147483648}}',
            Name("Int32"),
        ),
        (
            "Map(Int8, UInt32)",
            "('2020-01-01', map(1,0,2,4294967295))",
            '{"d":"2020-01-01","m":{"1":0,"2":4294967295}}',
            Name("UInt32"),
        ),
        (
            "Map(Int8, Int64)",
            "('2020-01-01', map(1,9223372036854775807,2,0,3,-9223372036854775808))",
            '{"d":"2020-01-01","m":{1:"9223372036854775807",2:"0",3:"-9223372036854775808"}}',
            Name("Int64"),
        ),
        (
            "Map(Int8, UInt64)",
            "('2020-01-01', map(1,0,2,18446744073709551615))",
            '{"d":"2020-01-01","m":{1:"0",2:"18446744073709551615"}}',
            Name("UInt64"),
        ),
        (
            "Map(Int8, Int128)",
            "('2020-01-01', map(1,170141183460469231731687303715884105727,2,0,3,-170141183460469231731687303715884105728))",
            '{"d":"2020-01-01","m":{1:"170141183460469231731687303715884105727",2:"0",3:"-170141183460469231731687303715884105728"}}',
            Name("Int128"),
        ),
        (
            "Map(Int8, Int256)",
            "('2020-01-01', map(1,57896044618658097711785492504343953926634992332820282019728792003956564819967,2,0,3,-57896044618658097711785492504343953926634992332820282019728792003956564819968))",
            '{"d":"2020-01-01","m":{1:"57896044618658097711785492504343953926634992332820282019728792003956564819967",2:"0",3:"-57896044618658097711785492504343953926634992332820282019728792003956564819968"}}',
            Name("Int256"),
        ),
        (
            "Map(Int8, UInt256)",
            "('2020-01-01', map(1,0,2,115792089237316195423570985008687907853269984665640564039457584007913129639935))",
            '{"d":"2020-01-01","m":{1:"0",2:"115792089237316195423570985008687907853269984665640564039457584007913129639935"}}',
            Name("UInt256"),
        ),
        (
            "Map(Int8, Nullable(Int8))",
            "('2020-01-01', map(1,toNullable(1)))",
            '{"d":"2020-01-01","m":{"1":1}}',
            Name("toNullable"),
        ),
        (
            "Map(Int8, Nullable(Int8))",
            "('2020-01-01', map(1,toNullable(NULL)))",
            '{"d":"2020-01-01","m":{"1":null}}',
            Name("toNullable(NULL)"),
        ),
    ],
)
def table_map_with_value_integer(self, type, data, output):
    """Check what values we can insert into map type column with value integer."""
    insert_into_table(type=type, data=data, output=output)


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Value_Array("1.0"))
@Examples(
    "type data output",
    [
        (
            "Map(String, Array(Int8))",
            "('2020-01-01', map('key',[]))",
            '{"d":"2020-01-01","m":{"key":[]}}',
            Name("empty array"),
        ),
        (
            "Map(String, Array(Int8))",
            "('2020-01-01', map('key',[1,2,3]))",
            '{"d":"2020-01-01","m":{"key":[1,2,3]}}',
            Name("non-empty array of ints"),
        ),
        (
            "Map(String, Array(String))",
            "('2020-01-01', map('key',['1','2','3']))",
            '{"d":"2020-01-01","m":{"key":["1","2","3"]}}',
            Name("non-empty array of strings"),
        ),
        (
            "Map(String, Array(Map(Int8, Int8)))",
            "('2020-01-01', map('key',[map(1,2),map(2,3)]))",
            '{"d":"2020-01-01","m":{"key":[{"1":2},{"2":3}]}}',
            Name("non-empty array of maps"),
        ),
        (
            "Map(String, Array(Map(Int8, Array(Map(Int8, Array(Int8))))))",
            "('2020-01-01', map('key',[map(1,[map(1,[1])]),map(2,[map(2,[3])])]))",
            '{"d":"2020-01-01","m":{"key":[{"1":[{"1":[1]}]},{"2":[{"2":[3]}]}]}}',
            Name("non-empty array of maps of array of maps"),
        ),
    ],
)
def table_map_with_value_array(self, type, data, output):
    """Check what values we can insert into map type column with value Array."""
    insert_into_table(type=type, data=data, output=output)


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Key_Integer("1.0"))
@Examples(
    "type data output",
    [
        (
            "Map(Int8, Int8)",
            "('2020-01-01', map(127,1,0,1,-128,1))",
            '{"d":"2020-01-01","m":{"127":1,"0":1,"-128":1}}',
            Name("Int8"),
        ),
        (
            "Map(UInt8, Int8)",
            "('2020-01-01', map(0,1,255,1))",
            '{"d":"2020-01-01","m":{"0":1,"255":1}}',
            Name("UInt8"),
        ),
        (
            "Map(Int16, Int8)",
            "('2020-01-01', map(127,1,0,1,-128,1))",
            '{"d":"2020-01-01","m":{"32767":1,"0":1,"-32768":1}}',
            Name("Int16"),
        ),
        (
            "Map(UInt16, Int8)",
            "('2020-01-01', map(0,1,65535,1))",
            '{"d":"2020-01-01","m":{"0":1,"65535":1}}',
            Name("UInt16"),
        ),
        (
            "Map(Int32, Int8)",
            "('2020-01-01', map(2147483647,1,0,1,-2147483648,1))",
            '{"d":"2020-01-01","m":{"2147483647":1,"0":1,"-2147483648":1}}',
            Name("Int32"),
        ),
        (
            "Map(UInt32, Int8)",
            "('2020-01-01', map(0,1,4294967295,1))",
            '{"d":"2020-01-01","m":{"0":1,"4294967295":1}}',
            Name("UInt32"),
        ),
        (
            "Map(Int64, Int8)",
            "('2020-01-01', map(9223372036854775807,1,0,1,-9223372036854775808,1))",
            '{"d":"2020-01-01","m":{"9223372036854775807":1,"0":1,"-9223372036854775808":1}}',
            Name("Int64"),
        ),
        (
            "Map(UInt64, Int8)",
            "('2020-01-01', map(0,1,18446744073709551615,1))",
            '{"d":"2020-01-01","m":{"0":1,"18446744073709551615":1}}',
            Name("UInt64"),
        ),
        (
            "Map(Int128, Int8)",
            "('2020-01-01', map(170141183460469231731687303715884105727,1,0,1,-170141183460469231731687303715884105728,1))",
            '{"d":"2020-01-01","m":{170141183460469231731687303715884105727:1,0:1,"-170141183460469231731687303715884105728":1}}',
            Name("Int128"),
        ),
        (
            "Map(Int256, Int8)",
            "('2020-01-01', map(57896044618658097711785492504343953926634992332820282019728792003956564819967,1,0,1,-57896044618658097711785492504343953926634992332820282019728792003956564819968,1))",
            '{"d":"2020-01-01","m":{"57896044618658097711785492504343953926634992332820282019728792003956564819967":1,"0":1,"-57896044618658097711785492504343953926634992332820282019728792003956564819968":1}}',
            Name("Int256"),
        ),
        (
            "Map(UInt256, Int8)",
            "('2020-01-01', map(0,1,115792089237316195423570985008687907853269984665640564039457584007913129639935,1))",
            '{"d":"2020-01-01","m":{"0":1,"115792089237316195423570985008687907853269984665640564039457584007913129639935":1}}',
            Name("UInt256"),
        ),
        (
            "Map(Nullable(Int8), Int8)",
            "('2020-01-01', map(toNullable(1),1))",
            '{"d":"2020-01-01","m":{1:1}}',
            Name("toNullable"),
        ),
        (
            "Map(Nullable(Int8), Int8)",
            "('2020-01-01', map(toNullable(NULL),1))",
            '{"d":"2020-01-01","m":{null:1}}',
            Name("toNullable(NULL)"),
        ),
    ],
)
def table_map_with_key_integer(self, type, data, output):
    """Check what values we can insert into map type column with key integer."""
    insert_into_table(type=type, data=data, output=output)


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Key_Integer("1.0"))
@Examples(
    "type data output select",
    [
        (
            "Map(Int8, Int8)",
            "('2020-01-01', map(127,1,0,1,-128,1))",
            '{"m":1}',
            "m[127] AS m",
            Name("Int8"),
        ),
        (
            "Map(UInt8, Int8)",
            "('2020-01-01', map(0,1,255,1))",
            '{"m":2}',
            "(m[255] + m[0]) AS m",
            Name("UInt8"),
        ),
        (
            "Map(Int16, Int8)",
            "('2020-01-01', map(127,1,0,1,-128,1))",
            '{"m":3}',
            "(m[-128] + m[0] + m[-128]) AS m",
            Name("Int16"),
        ),
        (
            "Map(UInt16, Int8)",
            "('2020-01-01', map(0,1,65535,1))",
            '{"m":2}',
            "(m[0] + m[65535]) AS m",
            Name("UInt16"),
        ),
        (
            "Map(Int32, Int8)",
            "('2020-01-01', map(2147483647,1,0,1,-2147483648,1))",
            '{"m":3}',
            "(m[2147483647] + m[0] + m[-2147483648]) AS m",
            Name("Int32"),
        ),
        (
            "Map(UInt32, Int8)",
            "('2020-01-01', map(0,1,4294967295,1))",
            '{"m":2}',
            "(m[0] + m[4294967295]) AS m",
            Name("UInt32"),
        ),
        (
            "Map(Int64, Int8)",
            "('2020-01-01', map(9223372036854775807,1,0,1,-9223372036854775808,1))",
            '{"m":3}',
            "(m[9223372036854775807] + m[0] + m[-9223372036854775808]) AS m",
            Name("Int64"),
        ),
        (
            "Map(UInt64, Int8)",
            "('2020-01-01', map(0,1,18446744073709551615,1))",
            '{"m":2}',
            "(m[0] + m[18446744073709551615]) AS m",
            Name("UInt64"),
        ),
        (
            "Map(Int128, Int8)",
            "('2020-01-01', map(170141183460469231731687303715884105727,1,0,1,-170141183460469231731687303715884105728,1))",
            '{"m":3}',
            "(m[170141183460469231731687303715884105727] + m[0] + m[-170141183460469231731687303715884105728]) AS m",
            Name("Int128"),
        ),
        (
            "Map(Int256, Int8)",
            "('2020-01-01', map(57896044618658097711785492504343953926634992332820282019728792003956564819967,1,0,1,-57896044618658097711785492504343953926634992332820282019728792003956564819968,1))",
            '{"m":3}',
            "(m[57896044618658097711785492504343953926634992332820282019728792003956564819967] + m[0] + m[-57896044618658097711785492504343953926634992332820282019728792003956564819968]) AS m",
            Name("Int256"),
        ),
        (
            "Map(UInt256, Int8)",
            "('2020-01-01', map(0,1,115792089237316195423570985008687907853269984665640564039457584007913129639935,1))",
            '{"m":2}',
            "(m[0] + m[115792089237316195423570985008687907853269984665640564039457584007913129639935]) AS m",
            Name("UInt256"),
        ),
        (
            "Map(Nullable(Int8), Int8)",
            "('2020-01-01', map(toNullable(1),1))",
            '{"m":1}',
            "m[1] AS m",
            Name("toNullable"),
        ),
        (
            "Map(Nullable(Int8), Int8)",
            "('2020-01-01', map(toNullable(NULL),1))",
            '{"m":1}',
            "m[null] AS m",
            Name("toNullable(NULL)"),
        ),
    ],
)
def table_map_select_key_with_key_integer(self, type, data, output, select):
    """Check what values we can insert into map type column with key integer and if we can use the key to select the value."""
    insert_into_table(type=type, data=data, output=output, select=select)


@TestOutline(Scenario)
@Requirements(
    RQ_SRS_018_ClickHouse_Map_DataType_ArrayOfMaps("1.0"),
    RQ_SRS_018_ClickHouse_Map_DataType_NestedWithMaps("1.0"),
)
@Examples(
    "type data output partition_by",
    [
        (
            "Array(Map(String, Int8))",
            "('2020-01-01', [map('hello',1),map('hello',1,'there',2)])",
            '{"d":"2020-01-01","m":[{"hello":1},{"hello":1,"there":2}]}',
            "m",
            Name("Array(Map(String, Int8))"),
        ),
        (
            "Nested(x Map(String, Int8))",
            "('2020-01-01', [map('hello',1)])",
            '{"d":"2020-01-01","m.x":[{"hello":1}]}',
            "m.x",
            Name("Nested(x Map(String, Int8)"),
        ),
    ],
)
def table_with_map_inside_another_type(self, type, data, output, partition_by):
    """Check what values we can insert into a type that has map type."""
    insert_into_table(type=type, data=data, output=output, partition_by=partition_by)


@TestOutline
def insert_into_table(self, type, data, output, partition_by="m", select="*"):
    """Check we can insert data into a table."""
    uid = getuid()
    node = self.context.node

    with Given(f"table definition with {type}"):
        sql = (
            "CREATE TABLE {name} (d DATE, m "
            + type
            + ") ENGINE = MergeTree() PARTITION BY "
            + partition_by
            + " ORDER BY d"
        )

    with Given(f"I create a table", description=sql):
        table = create_table(name=uid, statement=sql)

    with When("I insert data", description=data):
        sql = f"INSERT INTO {table} VALUES {data}"
        node.query(sql)

    with And("I select rows from the table"):
        r = node.query(f"SELECT {select} FROM {table} FORMAT JSONEachRow")

    with Then("I expect output to match", description=output):
        assert r.output == output, error()


@TestScenario
@Requirements(
    RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_MixedKeyOrValueTypes("1.0")
)
def select_map_with_invalid_mixed_key_and_value_types(self):
    """Check that creating a map with mixed key types fails."""
    node = self.context.node
    exitcode = 130
    message = "DB::Exception: There is no supertype for types String, UInt8 because some of them are String/FixedString and some of them are not"

    with Check(
        "attempt to create a map using SELECT with mixed key types then it fails"
    ):
        node.query("SELECT map('hello',1,2,3)", exitcode=exitcode, message=message)

    with Check(
        "attempt to create a map using SELECT with mixed value types then it fails"
    ):
        node.query("SELECT map(1,'hello',2,2)", exitcode=exitcode, message=message)


@TestScenario
@Requirements(
    RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_InvalidNumberOfArguments("1.0")
)
def select_map_with_invalid_number_of_arguments(self):
    """Check that creating a map with invalid number of arguments fails."""
    node = self.context.node
    exitcode = 42
    message = "DB::Exception: Function map requires even number of arguments"

    with When("I create a map using SELECT with invalid number of arguments"):
        node.query("SELECT map(1,2,3)", exitcode=exitcode, message=message)


@TestScenario
def select_map_empty(self):
    """Check that we can can create a empty map by not passing any arguments."""
    node = self.context.node

    with When("I create a map using SELECT with no arguments"):
        r = node.query("SELECT map()")

    with Then("it should create an empty map"):
        assert r.output == "{}", error()


@TestScenario
def insert_invalid_mixed_key_and_value_types(self):
    """Check that inserting a map with mixed key or value types fails."""
    uid = getuid()
    node = self.context.node
    exitcode = 130
    message = "DB::Exception: There is no supertype for types String, UInt8 because some of them are String/FixedString and some of them are not"

    with Given(f"table definition with {type}"):
        sql = "CREATE TABLE {name} (d DATE, m Map(String, Int8)) ENGINE = MergeTree() PARTITION BY m ORDER BY d"

    with And(f"I create a table", description=sql):
        table = create_table(name=uid, statement=sql)

    with When("I insert a map with mixed key types then it should fail"):
        sql = f"INSERT INTO {table} VALUES ('2020-01-01', map('hello',1,2,3))"
        node.query(sql, exitcode=exitcode, message=message)

    with When("I insert a map with mixed value types then it should fail"):
        sql = f"INSERT INTO {table} VALUES ('2020-01-01', map(1,'hello',2,2))"
        node.query(sql, exitcode=exitcode, message=message)


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_DuplicatedKeys("1.0"))
@Examples(
    "type data output",
    [
        (
            "Map(String, String)",
            "('2020-01-01', map('hello','there','hello','over there'))",
            '{"d":"2020-01-01","m":{"hello":"there","hello":"over there"}}',
            Name("Map(String, String))"),
        ),
        (
            "Map(Int64, String)",
            "('2020-01-01', map(12345,'there',12345,'over there'))",
            '{"d":"2020-01-01","m":{"12345":"there","12345":"over there"}}',
            Name("Map(Int64, String))"),
        ),
    ],
)
def table_map_with_duplicated_keys(self, type, data, output):
    """Check that map supports duplicated keys."""
    insert_into_table(type=type, data=data, output=output)


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_DuplicatedKeys("1.0"))
@Examples(
    "map output",
    [
        (
            "map('hello','there','hello','over there')",
            "{'hello':'there','hello':'over there'}",
            Name("String"),
        ),
        (
            "map(12345,'there',12345,'over there')",
            "{12345:'there',12345:'over there'}",
            Name("Integer"),
        ),
    ],
)
def select_map_with_duplicated_keys(self, map, output):
    """Check creating a map with duplicated keys."""
    select_map(map=map, output=output)


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Value_Retrieval_KeyNotFound("1.0"))
def select_map_key_not_found(self):
    node = self.context.node

    with When("map is empty"):
        node.query(
            "SELECT map() AS m, m[1]",
            exitcode=43,
            message="DB::Exception: Illegal types of arguments",
        )

    with When("map has integer values"):
        r = node.query("SELECT map(1,2) AS m, m[2] FORMAT Values")
    with Then("zero should be returned for key that is not found"):
        assert r.output == "({1:2},0)", error()

    with When("map has string values"):
        r = node.query("SELECT map(1,'2') AS m, m[2] FORMAT Values")
    with Then("empty string should be returned for key that is not found"):
        assert r.output == "({1:'2'},'')", error()

    with When("map has array values"):
        r = node.query("SELECT map(1,[2]) AS m, m[2] FORMAT Values")
    with Then("empty array be returned for key that is not found"):
        assert r.output == "({1:[2]},[])", error()


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Value_Retrieval_KeyNotFound("1.0"))
@Examples(
    "type data select exitcode message",
    [
        (
            "Map(UInt8, UInt8), y Int8",
            "(y) VALUES (1)",
            "m[1] AS v",
            0,
            '{"v":0}',
            Name("empty map"),
        ),
        (
            "Map(UInt8, UInt8)",
            "VALUES (map(1,2))",
            "m[2] AS v",
            0,
            '{"v":0}',
            Name("map has integer values"),
        ),
        (
            "Map(UInt8, String)",
            "VALUES (map(1,'2'))",
            "m[2] AS v",
            0,
            '{"v":""}',
            Name("map has string values"),
        ),
        (
            "Map(UInt8, Array(Int8))",
            "VALUES (map(1,[2]))",
            "m[2] AS v",
            0,
            '{"v":[]}',
            Name("map has array values"),
        ),
    ],
)
def table_map_key_not_found(self, type, data, select, exitcode, message, order_by=None):
    """Check values returned from a map column when key is not found."""
    uid = getuid()
    node = self.context.node

    if order_by is None:
        order_by = "m"

    with Given(f"table definition with {type}"):
        sql = (
            "CREATE TABLE {name} (m "
            + type
            + ") ENGINE = MergeTree() ORDER BY "
            + order_by
        )

    with And(f"I create a table", description=sql):
        table = create_table(name=uid, statement=sql)

    with When("I insert data into the map column"):
        node.query(f"INSERT INTO {table} {data}")

    with And("I try to read from the table"):
        node.query(
            f"SELECT {select} FROM {table} FORMAT JSONEachRow",
            exitcode=exitcode,
            message=message,
        )


@TestScenario
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Value_Retrieval_KeyInvalid("1.0"))
def invalid_key(self):
    """Check when key is not valid."""
    node = self.context.node

    with When("I try to use an integer key that is too large"):
        node.query(
            "SELECT map(1,2) AS m, m[256]",
            exitcode=43,
            message="DB::Exception: Illegal types of arguments",
        )

    with When("I try to use an integer key that is negative when key is unsigned"):
        node.query(
            "SELECT map(1,2) AS m, m[-1]",
            exitcode=43,
            message="DB::Exception: Illegal types of arguments",
        )

    with When("I try to use a string key when key is an integer"):
        node.query(
            "SELECT map(1,2) AS m, m['1']",
            exitcode=43,
            message="DB::Exception: Illegal types of arguments",
        )

    with When("I try to use an integer key when key is a string"):
        r = node.query(
            "SELECT map('1',2) AS m, m[1]",
            exitcode=43,
            message="DB::Exception: Illegal types of arguments",
        )

    with When("I try to use an empty key when key is a string"):
        r = node.query(
            "SELECT map('1',2) AS m, m[]",
            exitcode=62,
            message="DB::Exception: Syntax error: failed at position",
        )

    with When("I try to use wrong type conversion in key"):
        r = node.query(
            "SELECT map(1,2) AS m, m[toInt8('1')]",
            exitcode=43,
            message="DB::Exception: Illegal types of arguments",
        )

    with When(
        "in array of maps I try to use an integer key that is negative when key is unsigned"
    ):
        node.query(
            "SELECT [map(1,2)] AS m, m[1][-1]",
            exitcode=43,
            message="DB::Exception: Illegal types of arguments",
        )

    with When("I try to use a NULL key when key is not nullable"):
        r = node.query("SELECT map(1,2) AS m, m[NULL] FORMAT Values")
    with Then("it should return NULL"):
        assert r.output == "({1:2},NULL)", error()


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Value_Retrieval_KeyInvalid("1.0"))
@Examples(
    "type data select exitcode message order_by",
    [
        (
            "Map(UInt8, UInt8)",
            "(map(1,2))",
            "m[256] AS v",
            0,
            '{"v":0}',
            "m",
            Name("key too large)"),
        ),
        (
            "Map(UInt8, UInt8)",
            "(map(1,2))",
            "m[-1] AS v",
            0,
            '{"v":0}',
            "m",
            Name("key is negative"),
        ),
        (
            "Map(UInt8, UInt8)",
            "(map(1,2))",
            "m['1'] AS v",
            43,
            "DB::Exception: Illegal types of arguments",
            "m",
            Name("string when key is integer"),
        ),
        (
            "Map(String, UInt8)",
            "(map('1',2))",
            "m[1] AS v",
            43,
            "DB::Exception: Illegal types of arguments",
            "m",
            Name("integer when key is string"),
        ),
        (
            "Map(String, UInt8)",
            "(map('1',2))",
            "m[] AS v",
            62,
            "DB::Exception: Syntax error: failed at position",
            "m",
            Name("empty when key is string"),
        ),
        (
            "Map(UInt8, UInt8)",
            "(map(1,2))",
            "m[toInt8('1')] AS v",
            0,
            '{"v":2}',
            "m",
            Name("wrong type conversion when key is integer"),
        ),
        (
            "Map(String, UInt8)",
            "(map('1',2))",
            "m[toFixedString('1',1)] AS v",
            0,
            '{"v":2}',
            "m",
            Name("wrong type conversion when key is string"),
        ),
        (
            "Map(UInt8, UInt8)",
            "(map(1,2))",
            "m[NULL] AS v",
            0,
            '{"v":null}',
            "m",
            Name("NULL key when key is not nullable"),
        ),
        (
            "Array(Map(UInt8, UInt8))",
            "([map(1,2)])",
            "m[1]['1'] AS v",
            43,
            "DB::Exception: Illegal types of arguments",
            "m",
            Name("string when key is integer in array of maps"),
        ),
        (
            "Nested(x Map(UInt8, UInt8))",
            "([map(1,2)])",
            "m.x[1]['1'] AS v",
            43,
            "DB::Exception: Illegal types of arguments",
            "m.x",
            Name("string when key is integer in nested map"),
        ),
    ],
)
def table_map_invalid_key(self, type, data, select, exitcode, message, order_by="m"):
    """Check selecting values from a map column using an invalid key."""
    uid = getuid()
    node = self.context.node

    with Given(f"table definition with {type}"):
        sql = (
            "CREATE TABLE {name} (m "
            + type
            + ") ENGINE = MergeTree() ORDER BY "
            + order_by
        )

    with And(f"I create a table", description=sql):
        table = create_table(name=uid, statement=sql)

    with When("I insert data into the map column"):
        node.query(f"INSERT INTO {table} VALUES {data}")

    with And("I try to read from the table"):
        node.query(
            f"SELECT {select} FROM {table} FORMAT JSONEachRow",
            exitcode=exitcode,
            message=message,
        )


@TestOutline(Scenario)
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Value_Retrieval("1.0"))
@Examples(
    "type data select filter exitcode message order_by",
    [
        (
            "Map(UInt8, UInt8)",
            "(map(1,1)),(map(1,2)),(map(2,3))",
            "m[1] AS v",
            "1=1 ORDER BY m[1]",
            0,
            '{"v":0}\n{"v":1}\n{"v":2}',
            None,
            Name("select the same key from all the rows"),
        ),
        (
            "Map(String, String)",
            "(map('a','b')),(map('c','d','e','f')),(map('e','f'))",
            "m",
            "m = map('e','f','c','d')",
            0,
            "",
            None,
            Name("filter rows by map having different pair order"),
        ),
        (
            "Map(String, String)",
            "(map('a','b')),(map('c','d','e','f')),(map('e','f'))",
            "m",
            "m = map('c','d','e','f')",
            0,
            '{"m":{"c":"d","e":"f"}}',
            None,
            Name("filter rows by map having the same pair order"),
        ),
        (
            "Map(String, String)",
            "(map('a','b')),(map('e','f'))",
            "m",
            "m = map()",
            0,
            "",
            None,
            Name("filter rows by empty map"),
        ),
        (
            "Map(String, Int8)",
            "(map('a',1,'b',2)),(map('a',2)),(map('b',3))",
            "m",
            "m['a'] = 1",
            0,
            '{"m":{"a":1,"b":2}}',
            None,
            Name("filter rows by map key value"),
        ),
        (
            "Map(String, Int8)",
            "(map('a',1,'b',2)),(map('a',2)),(map('b',3))",
            "m",
            "m['a'] = 1 AND m['b'] = 2",
            0,
            '{"m":{"a":1,"b":2}}',
            None,
            Name("filter rows by map multiple key value combined with AND"),
        ),
        (
            "Map(String, Int8)",
            "(map('a',1,'b',2)),(map('a',2)),(map('b',3))",
            "m",
            "m['a'] = 1 OR m['b'] = 3",
            0,
            '{"m":{"a":1,"b":2}}\n{"m":{"b":3}}',
            None,
            Name("filter rows by map multiple key value combined with OR"),
        ),
        (
            "Map(String, Array(Int8))",
            "(map('a',[])),(map('b',[1])),(map('c',[2]))",
            "m['b'] AS v",
            "m['b'] IN ([1],[2])",
            0,
            '{"v":[1]}',
            None,
            Name("filter rows by map array value using IN"),
        ),
        (
            "Map(String, Nullable(String))",
            "(map('a',NULL)),(map('a',1))",
            "m",
            "isNull(m['a']) = 1",
            0,
            '{"m":{"a":null}}',
            None,
            Name("select map with nullable value"),
        ),
    ],
)
def table_map_queries(
    self, type, data, select, filter, exitcode, message, order_by=None
):
    """Check retrieving map values and using maps in queries."""
    uid = getuid()
    node = self.context.node

    if order_by is None:
        order_by = "m"

    with Given(f"table definition with {type}"):
        sql = (
            "CREATE TABLE {name} (m "
            + type
            + ") ENGINE = MergeTree() ORDER BY "
            + order_by
        )

    with And(f"I create a table", description=sql):
        table = create_table(name=uid, statement=sql)

    with When("I insert data into the map column"):
        node.query(f"INSERT INTO {table} VALUES {data}")

    with And("I try to read from the table"):
        node.query(
            f"SELECT {select} FROM {table} WHERE {filter} FORMAT JSONEachRow",
            exitcode=exitcode,
            message=message,
        )


@TestOutline(Scenario)
@Requirements(
    RQ_SRS_018_ClickHouse_Map_DataType_Invalid_Nullable("1.0"),
    RQ_SRS_018_ClickHouse_Map_DataType_Invalid_NothingNothing("1.0"),
)
@Examples(
    "type exitcode message",
    [
        (
            "Nullable(Map(String, String))",
            43,
            "DB::Exception: Nested type Map(String,String) cannot be inside Nullable type",
            Name("nullable map"),
        ),
        (
            "Map(Nothing, Nothing)",
            37,
            "DB::Exception: Column `m` with type Map(Nothing,Nothing) is not allowed in key expression, it's not comparable",
            Name("map with nothing type for key and value"),
        ),
    ],
)
def table_map_unsupported_types(self, type, exitcode, message):
    """Check creating a table with unsupported map column types."""
    uid = getuid()
    node = self.context.node

    try:
        with When(f"I create a table definition with {type}"):
            sql = f"CREATE TABLE {uid} (m " + type + ") ENGINE = MergeTree() ORDER BY m"
            node.query(sql, exitcode=exitcode, message=message)
    finally:
        with Finally("drop table if any"):
            node.query(f"DROP TABLE IF EXISTS {uid}")


@TestOutline(Scenario)
@Requirements(
    RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_TupleOfArraysToMap("1.0"),
    RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_TupleOfArraysMap_Invalid("1.0"),
)
@Examples(
    "tuple type exitcode message",
    [
        (
            "([1, 2, 3], ['Ready', 'Steady', 'Go'])",
            "Map(UInt8, String)",
            0,
            "{1:'Ready',2:'Steady',3:'Go'}",
            Name("int -> int"),
        ),
        (
            "([1, 2, 3], ['Ready', 'Steady', 'Go'])",
            "Map(String, String)",
            0,
            "{'1':'Ready','2':'Steady','3':'Go'}",
            Name("int -> string"),
        ),
        (
            "(['1', '2', '3'], ['Ready', 'Steady', 'Go'])",
            "Map(UInt8, String)",
            0,
            "{1:'Ready',187:'Steady',143:'Go'}",
            Name("string -> int"),
        ),
        (
            "([],[])",
            "Map(String, String)",
            0,
            "{}",
            Name("empty arrays to map str:str"),
        ),
        (
            "([],[])",
            "Map(UInt8, Array(Int8))",
            0,
            "{}",
            Name("empty arrays to map uint8:array"),
        ),
        (
            "([[1]],['hello'])",
            "Map(String, String)",
            0,
            "{'[1]':'hello'}",
            Name("array -> string"),
        ),
        (
            "([(1,2),(3,4)])",
            "Map(UInt8, UInt8)",
            0,
            "{1:2,3:4}",
            Name("array of two tuples"),
        ),
        (
            "([1, 2], ['Ready', 'Steady', 'Go'])",
            "Map(UInt8, String)",
            53,
            "DB::Exception: CAST AS Map can only be performed from tuple of arrays with equal sizes",
            Name("unequal array sizes"),
        ),
    ],
)
def cast_tuple_of_two_arrays_to_map(self, tuple, type, exitcode, message):
    """Check casting Tuple(Array, Array) to a map type."""
    node = self.context.node

    with When("I try to cast tuple", description=tuple):
        node.query(
            f"SELECT CAST({tuple}, '{type}') AS map", exitcode=exitcode, message=message
        )


@TestOutline(Scenario)
@Requirements(
    RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_TupleOfArraysToMap("1.0"),
    RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_TupleOfArraysMap_Invalid("1.0"),
)
@Examples(
    "tuple type exitcode message check_insert",
    [
        (
            "(([1, 2, 3], ['Ready', 'Steady', 'Go']))",
            "Map(UInt8, String)",
            0,
            '{"m":{"1":"Ready","2":"Steady","3":"Go"}}',
            False,
            Name("int -> int"),
        ),
        (
            "(([1, 2, 3], ['Ready', 'Steady', 'Go']))",
            "Map(String, String)",
            0,
            '{"m":{"1":"Ready","2":"Steady","3":"Go"}}',
            False,
            Name("int -> string"),
        ),
        (
            "((['1', '2', '3'], ['Ready', 'Steady', 'Go']))",
            "Map(UInt8, String)",
            0,
            "",
            True,
            Name("string -> int"),
        ),
        (
            "(([],[]))",
            "Map(String, String)",
            0,
            '{"m":{}}',
            False,
            Name("empty arrays to map str:str"),
        ),
        (
            "(([],[]))",
            "Map(UInt8, Array(Int8))",
            0,
            '{"m":{}}',
            False,
            Name("empty arrays to map uint8:array"),
        ),
        (
            "(([[1]],['hello']))",
            "Map(String, String)",
            53,
            "DB::Exception: Type mismatch in IN or VALUES section",
            True,
            Name("array -> string"),
        ),
        (
            "(([(1,2),(3,4)]))",
            "Map(UInt8, UInt8)",
            0,
            '{"m":{"1":2,"3":4}}',
            False,
            Name("array of two tuples"),
        ),
        (
            "(([1, 2], ['Ready', 'Steady', 'Go']))",
            "Map(UInt8, String)",
            53,
            "DB::Exception: CAST AS Map can only be performed from tuple of arrays with equal sizes",
            True,
            Name("unequal array sizes"),
        ),
    ],
)
def table_map_cast_tuple_of_arrays_to_map(
    self, tuple, type, exitcode, message, check_insert
):
    """Check converting Tuple(Array, Array) into map on insert into a map type column."""
    table_map(
        type=type,
        data=tuple,
        select="*",
        filter="1=1",
        exitcode=exitcode,
        message=message,
        check_insert=check_insert,
    )


@TestOutline(Scenario)
@Requirements(
    RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_ArrayOfTuplesToMap("1.0"),
    RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_ArrayOfTuplesToMap_Invalid(
        "1.0"
    ),
)
@Examples(
    "tuple type exitcode message",
    [
        (
            "([(1,2),(3,4)])",
            "Map(UInt8, UInt8)",
            0,
            "{1:2,3:4}",
            Name("array of two tuples"),
        ),
        (
            "([(1,2),(3)])",
            "Map(UInt8, UInt8)",
            130,
            "DB::Exception: There is no supertype for types Tuple(UInt8, UInt8), UInt8 because some of them are Tuple and some of them are not",
            Name("not a tuple"),
        ),
        (
            "([(1,2),(3,)])",
            "Map(UInt8, UInt8)",
            130,
            "DB::Exception: There is no supertype for types Tuple(UInt8, UInt8), Tuple(UInt8) because Tuples have different sizes",
            Name("invalid tuple"),
        ),
    ],
)
def cast_array_of_two_tuples_to_map(self, tuple, type, exitcode, message):
    """Check casting Array(Tuple(K,V)) to a map type."""
    node = self.context.node

    with When("I try to cast tuple", description=tuple):
        node.query(
            f"SELECT CAST({tuple}, '{type}') AS map", exitcode=exitcode, message=message
        )


@TestOutline(Scenario)
@Requirements(
    RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_ArrayOfTuplesToMap("1.0"),
    RQ_SRS_018_ClickHouse_Map_DataType_Conversion_From_ArrayOfTuplesToMap_Invalid(
        "1.0"
    ),
)
@Examples(
    "tuple type exitcode message check_insert",
    [
        (
            "(([(1,2),(3,4)]))",
            "Map(UInt8, UInt8)",
            0,
            '{"m":{"1":2,"3":4}}',
            False,
            Name("array of two tuples"),
        ),
        (
            "(([(1,2),(3)]))",
            "Map(UInt8, UInt8)",
            130,
            "DB::Exception: There is no supertype for types Tuple(UInt8, UInt8), UInt8 because some of them are Tuple and some of them are not",
            True,
            Name("not a tuple"),
        ),
        (
            "(([(1,2),(3,)]))",
            "Map(UInt8, UInt8)",
            130,
            "DB::Exception: There is no supertype for types Tuple(UInt8, UInt8), Tuple(UInt8) because Tuples have different sizes",
            True,
            Name("invalid tuple"),
        ),
    ],
)
def table_map_cast_array_of_two_tuples_to_map(
    self, tuple, type, exitcode, message, check_insert
):
    """Check converting Array(Tuple(K,V),...) into map on insert into a map type column."""
    table_map(
        type=type,
        data=tuple,
        select="*",
        filter="1=1",
        exitcode=exitcode,
        message=message,
        check_insert=check_insert,
    )


@TestScenario
@Requirements(
    RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Keys_InlineDefinedMap("1.0")
)
def subcolumns_keys_using_inline_defined_map(self):
    node = self.context.node
    exitcode = 47
    message = "DB::Exception: Missing columns: 'c.keys'"

    with When("I try to access keys sub-column using an inline defined map"):
        node.query(
            "SELECT map( 'aa', 4, '44' , 5) as c, c.keys",
            exitcode=exitcode,
            message=message,
        )


@TestScenario
@Requirements(
    RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Values_InlineDefinedMap("1.0")
)
def subcolumns_values_using_inline_defined_map(self):
    node = self.context.node
    exitcode = 47
    message = "DB::Exception: Missing columns: 'c.values'"

    with When("I try to access values sub-column using an inline defined map"):
        node.query(
            "SELECT map( 'aa', 4, '44' , 5) as c, c.values",
            exitcode=exitcode,
            message=message,
        )


@TestOutline(Scenario)
@Requirements(
    RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Keys("1.0"),
    RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Keys_ArrayFunctions("1.0"),
    RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Values("1.0"),
    RQ_SRS_018_ClickHouse_Map_DataType_SubColumns_Values_ArrayFunctions("1.0"),
)
@Examples(
    "type data select filter exitcode message",
    [
        # keys
        (
            "Map(String, String)",
            "(map('a','b','c','d')),(map('e','f'))",
            "m.keys AS keys",
            "1=1",
            0,
            '{"keys":["a","c"]}\n{"keys":["e"]}',
            Name("select keys"),
        ),
        (
            "Map(String, String)",
            "(map('a','b','c','d')),(map('e','f'))",
            "m.keys AS keys",
            "has(m.keys, 'e')",
            0,
            '{"keys":["e"]}',
            Name("filter by using keys in an array function"),
        ),
        (
            "Map(String, String)",
            "(map('a','b','c','d')),(map('e','f'))",
            "has(m.keys, 'e') AS r",
            "1=1",
            0,
            '{"r":0}\n{"r":1}',
            Name("column that uses keys in an array function"),
        ),
        # values
        (
            "Map(String, String)",
            "(map('a','b','c','d')),(map('e','f'))",
            "m.values AS values",
            "1=1",
            0,
            '{"values":["b","d"]}\n{"values":["f"]}',
            Name("select values"),
        ),
        (
            "Map(String, String)",
            "(map('a','b','c','d')),(map('e','f'))",
            "m.values AS values",
            "has(m.values, 'f')",
            0,
            '{"values":["f"]}',
            Name("filter by using values in an array function"),
        ),
        (
            "Map(String, String)",
            "(map('a','b','c','d')),(map('e','f'))",
            "has(m.values, 'f') AS r",
            "1=1",
            0,
            '{"r":0}\n{"r":1}',
            Name("column that uses values in an array function"),
        ),
    ],
)
def subcolumns(self, type, data, select, filter, exitcode, message, order_by=None):
    """Check usage of sub-columns in queries."""
    table_map(
        type=type,
        data=data,
        select=select,
        filter=filter,
        exitcode=exitcode,
        message=message,
        order_by=order_by,
    )


@TestScenario
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Functions_Length("1.0"))
def length(self):
    """Check usage of length function with map data type."""
    table_map(
        type="Map(String, String)",
        data="(map('a','b','c','d')),(map('e','f'))",
        select="length(m) AS len, m",
        filter="length(m) = 1",
        exitcode=0,
        message='{"len":"1","m":{"e":"f"}}',
    )


@TestScenario
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Functions_Empty("1.0"))
def empty(self):
    """Check usage of empty function with map data type."""
    table_map(
        type="Map(String, String)",
        data="(map('e','f'))",
        select="empty(m) AS em, m",
        filter="empty(m) <> 1",
        exitcode=0,
        message='{"em":0,"m":{"e":"f"}}',
    )


@TestScenario
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Functions_NotEmpty("1.0"))
def notempty(self):
    """Check usage of notEmpty function with map data type."""
    table_map(
        type="Map(String, String)",
        data="(map('e','f'))",
        select="notEmpty(m) AS em, m",
        filter="notEmpty(m) = 1",
        exitcode=0,
        message='{"em":1,"m":{"e":"f"}}',
    )


@TestScenario
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_MapAdd("1.0"))
def cast_from_mapadd(self):
    """Check converting the result of mapAdd function to a map data type."""
    select_map(
        map="CAST(mapAdd(([toUInt8(1), 2], [1, 1]), ([toUInt8(1), 2], [1, 1])), 'Map(Int8, Int8)')",
        output="{1:2,2:2}",
    )


@TestScenario
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_MapSubstract("1.0"))
def cast_from_mapsubstract(self):
    """Check converting the result of mapSubstract function to a map data type."""
    select_map(
        map="CAST(mapSubtract(([toUInt8(1), 2], [toInt32(1), 1]), ([toUInt8(1), 2], [toInt32(2), 1])), 'Map(Int8, Int8)')",
        output="{1:-1,2:0}",
    )


@TestScenario
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map_MapPopulateSeries("1.0"))
def cast_from_mappopulateseries(self):
    """Check converting the result of mapPopulateSeries function to a map data type."""
    select_map(
        map="CAST(mapPopulateSeries([1,2,4], [11,22,44], 5), 'Map(Int8, Int8)')",
        output="{1:11,2:22,3:0,4:44,5:0}",
    )


@TestScenario
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Functions_MapContains("1.0"))
def mapcontains(self):
    """Check usages of mapContains function with map data type."""
    node = self.context.node

    with Example("key in map"):
        table_map(
            type="Map(String, String)",
            data="(map('e','f')),(map('a','b'))",
            select="m",
            filter="mapContains(m, 'a')",
            exitcode=0,
            message='{"m":{"a":"b"}}',
        )

    with Example("key not in map"):
        table_map(
            type="Map(String, String)",
            data="(map('e','f')),(map('a','b'))",
            select="m",
            filter="NOT mapContains(m, 'a')",
            exitcode=0,
            message='{"m":{"e":"f"}}',
        )

    with Example("null key not in map"):
        table_map(
            type="Map(Nullable(String), String)",
            data="(map('e','f')),(map('a','b'))",
            select="m",
            filter="mapContains(m, NULL)",
            exitcode=0,
            message="",
        )

    with Example("null key in map"):
        table_map(
            type="Map(Nullable(String), String)",
            data="(map('e','f')),(map('a','b')),(map(NULL,'c'))",
            select="m",
            filter="mapContains(m, NULL)",
            exitcode=0,
            message='{null:"c"}',
        )

    with Example("select nullable key"):
        node.query(
            "SELECT map(NULL, 1, 2, 3) AS m, mapContains(m, toNullable(toUInt8(2)))",
            exitcode=0,
            message="{2:3}",
        )


@TestScenario
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Functions_MapKeys("1.0"))
def mapkeys(self):
    """Check usages of mapKeys function with map data type."""
    with Example("key in map"):
        table_map(
            type="Map(String, String)",
            data="(map('e','f')),(map('a','b'))",
            select="m",
            filter="has(mapKeys(m), 'a')",
            exitcode=0,
            message='{"m":{"a":"b"}}',
        )

    with Example("key not in map"):
        table_map(
            type="Map(String, String)",
            data="(map('e','f')),(map('a','b'))",
            select="m",
            filter="NOT has(mapKeys(m), 'a')",
            exitcode=0,
            message='{"m":{"e":"f"}}',
        )

    with Example("null key not in map"):
        table_map(
            type="Map(Nullable(String), String)",
            data="(map('e','f')),(map('a','b'))",
            select="m",
            filter="has(mapKeys(m), NULL)",
            exitcode=0,
            message="",
        )

    with Example("null key in map"):
        table_map(
            type="Map(Nullable(String), String)",
            data="(map('e','f')),(map('a','b')),(map(NULL,'c'))",
            select="m",
            filter="has(mapKeys(m), NULL)",
            exitcode=0,
            message='{"m":{null:"c"}}',
        )

    with Example("select keys from column"):
        table_map(
            type="Map(Nullable(String), String)",
            data="(map('e','f')),(map('a','b')),(map(NULL,'c'))",
            select="mapKeys(m) AS keys",
            filter="1 = 1",
            exitcode=0,
            message='{"keys":["a"]}\n{"keys":["e"]}\n{"keys":[null]}',
        )


@TestScenario
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Functions_MapValues("1.0"))
def mapvalues(self):
    """Check usages of mapValues function with map data type."""
    with Example("value in map"):
        table_map(
            type="Map(String, String)",
            data="(map('e','f')),(map('a','b'))",
            select="m",
            filter="has(mapValues(m), 'b')",
            exitcode=0,
            message='{"m":{"a":"b"}}',
        )

    with Example("value not in map"):
        table_map(
            type="Map(String, String)",
            data="(map('e','f')),(map('a','b'))",
            select="m",
            filter="NOT has(mapValues(m), 'b')",
            exitcode=0,
            message='{"m":{"e":"f"}}',
        )

    with Example("null value not in map"):
        table_map(
            type="Map(String, Nullable(String))",
            data="(map('e','f')),(map('a','b'))",
            select="m",
            filter="has(mapValues(m), NULL)",
            exitcode=0,
            message="",
        )

    with Example("null value in map"):
        table_map(
            type="Map(String, Nullable(String))",
            data="(map('e','f')),(map('a','b')),(map('c',NULL))",
            select="m",
            filter="has(mapValues(m), NULL)",
            exitcode=0,
            message='{"m":{"c":null}}',
        )

    with Example("select values from column"):
        table_map(
            type="Map(String, Nullable(String))",
            data="(map('e','f')),(map('a','b')),(map('c',NULL))",
            select="mapValues(m) AS values",
            filter="1 = 1",
            exitcode=0,
            message='{"values":["b"]}\n{"values":[null]}\n{"values":["f"]}',
        )


@TestScenario
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Functions_InlineDefinedMap("1.0"))
def functions_with_inline_defined_map(self):
    """Check that a map defined inline inside the select statement
    can be used with functions that work with maps.
    """
    with Example("mapKeys"):
        select_map(
            map="map(1,2,3,4) as map, mapKeys(map) AS keys", output="{1:2,3:4}\t[1,3]"
        )

    with Example("mapValyes"):
        select_map(
            map="map(1,2,3,4) as map, mapValues(map) AS values",
            output="{1:2,3:4}\t[2,4]",
        )

    with Example("mapContains"):
        select_map(
            map="map(1,2,3,4) as map, mapContains(map, 1) AS contains",
            output="{1:2,3:4}\t1",
        )


@TestScenario
def empty_map(self):
    """Check creating of an empty map `{}` using the map() function
    when inserting data into a map type table column.
    """
    table_map(
        type="Map(String, String)",
        data="(map('e','f')),(map())",
        select="m",
        filter="1=1",
        exitcode=0,
        message='{"m":{}}\n{"m":{"e":"f"}}',
    )


@TestScenario
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Performance_Vs_TupleOfArrays("1.0"))
def performance_vs_two_tuple_of_arrays(self, len=10, rows=6000000):
    """Check performance of using map data type vs Tuple(Array, Array)."""
    uid = getuid()
    node = self.context.node

    with Given(f"table with Tuple(Array(Int8),Array(Int8))"):
        sql = "CREATE TABLE {name} (pairs Tuple(Array(Int8),Array(Int8))) ENGINE = MergeTree() ORDER BY pairs"
        tuple_table = create_table(name=f"tuple_{uid}", statement=sql)

    with And(f"table with Map(Int8,Int8)"):
        sql = "CREATE TABLE {name} (pairs Map(Int8,Int8)) ENGINE = MergeTree() ORDER BY pairs"
        map_table = create_table(name=f"map_{uid}", statement=sql)

    with When("I insert data into table with tuples"):
        keys = range(len)
        values = range(len)
        start_time = time.time()
        node.query(
            f"INSERT INTO {tuple_table} SELECT ({keys},{values}) FROM numbers({rows})"
        )
        tuple_insert_time = time.time() - start_time
        metric("tuple insert time", tuple_insert_time, "sec")

    with When("I insert data into table with a map"):
        keys = range(len)
        values = range(len)
        start_time = time.time()
        node.query(
            f"INSERT INTO {map_table} SELECT ({keys},{values}) FROM numbers({rows})"
        )
        map_insert_time = time.time() - start_time
        metric("map insert time", map_insert_time, "sec")

    with And("I retrieve particular key value from table with tuples"):
        start_time = time.time()
        node.query(
            f"SELECT sum(arrayFirst((v, k) -> k = {len-1}, tupleElement(pairs, 2), tupleElement(pairs, 1))) AS sum FROM {tuple_table}",
            exitcode=0,
            message=f"{rows*(len-1)}",
        )
        tuple_select_time = time.time() - start_time
        metric("tuple(array, array) select time", tuple_select_time, "sec")

    with And("I retrieve particular key value from table with map"):
        start_time = time.time()
        node.query(
            f"SELECT sum(pairs[{len-1}]) AS sum FROM {map_table}",
            exitcode=0,
            message=f"{rows*(len-1)}",
        )
        map_select_time = time.time() - start_time
        metric("map select time", map_select_time, "sec")

    metric("insert difference", (1 - map_insert_time / tuple_insert_time) * 100, "%")
    metric("select difference", (1 - map_select_time / tuple_select_time) * 100, "%")


@TestScenario
@Requirements(RQ_SRS_018_ClickHouse_Map_DataType_Performance_Vs_ArrayOfTuples("1.0"))
def performance_vs_array_of_tuples(self, len=10, rows=6000000):
    """Check performance of using map data type vs Array(Tuple(K,V))."""
    uid = getuid()
    node = self.context.node

    with Given(f"table with Array(Tuple(K,V))"):
        sql = "CREATE TABLE {name} (pairs Array(Tuple(Int8, Int8))) ENGINE = MergeTree() ORDER BY pairs"
        array_table = create_table(name=f"tuple_{uid}", statement=sql)

    with And(f"table with Map(Int8,Int8)"):
        sql = "CREATE TABLE {name} (pairs Map(Int8,Int8)) ENGINE = MergeTree() ORDER BY pairs"
        map_table = create_table(name=f"map_{uid}", statement=sql)

    with When("I insert data into table with an array of tuples"):
        pairs = list(zip(range(len), range(len)))
        start_time = time.time()
        node.query(f"INSERT INTO {array_table} SELECT ({pairs}) FROM numbers({rows})")
        array_insert_time = time.time() - start_time
        metric("array insert time", array_insert_time, "sec")

    with When("I insert data into table with a map"):
        keys = range(len)
        values = range(len)
        start_time = time.time()
        node.query(
            f"INSERT INTO {map_table} SELECT ({keys},{values}) FROM numbers({rows})"
        )
        map_insert_time = time.time() - start_time
        metric("map insert time", map_insert_time, "sec")

    with And("I retrieve particular key value from table with an array of tuples"):
        start_time = time.time()
        node.query(
            f"SELECT sum(arrayFirst((v) -> v.1 = {len-1}, pairs).2) AS sum FROM {array_table}",
            exitcode=0,
            message=f"{rows*(len-1)}",
        )
        array_select_time = time.time() - start_time
        metric("array(tuple(k,v)) select time", array_select_time, "sec")

    with And("I retrieve particular key value from table with map"):
        start_time = time.time()
        node.query(
            f"SELECT sum(pairs[{len-1}]) AS sum FROM {map_table}",
            exitcode=0,
            message=f"{rows*(len-1)}",
        )
        map_select_time = time.time() - start_time
        metric("map select time", map_select_time, "sec")

    metric("insert difference", (1 - map_insert_time / array_insert_time) * 100, "%")
    metric("select difference", (1 - map_select_time / array_select_time) * 100, "%")


@TestScenario
def performance(self, len=10, rows=6000000):
    """Check insert and select performance of using map data type."""
    uid = getuid()
    node = self.context.node

    with Given("table with Map(Int8,Int8)"):
        sql = "CREATE TABLE {name} (pairs Map(Int8,Int8)) ENGINE = MergeTree() ORDER BY pairs"
        map_table = create_table(name=f"map_{uid}", statement=sql)

    with When("I insert data into table with a map"):
        values = [x for pair in zip(range(len), range(len)) for x in pair]
        start_time = time.time()
        node.query(
            f"INSERT INTO {map_table} SELECT (map({','.join([str(v) for v in values])})) FROM numbers({rows})"
        )
        map_insert_time = time.time() - start_time
        metric("map insert time", map_insert_time, "sec")

    with And("I retrieve particular key value from table with map"):
        start_time = time.time()
        node.query(
            f"SELECT sum(pairs[{len-1}]) AS sum FROM {map_table}",
            exitcode=0,
            message=f"{rows*(len-1)}",
        )
        map_select_time = time.time() - start_time
        metric("map select time", map_select_time, "sec")


# FIXME: add tests for different table engines


@TestFeature
@Name("tests")
@Requirements(
    RQ_SRS_018_ClickHouse_Map_DataType("1.0"),
    RQ_SRS_018_ClickHouse_Map_DataType_Functions_Map("1.0"),
)
def feature(self, node="clickhouse1"):
    self.context.node = self.context.cluster.node(node)

    for scenario in loads(current_module(), Scenario):
        scenario()
