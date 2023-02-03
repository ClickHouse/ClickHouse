@0x803231eaa402b968;

struct NestedNestedOne
{
    nestednestednumber @0 : UInt64;
}
struct NestedNestedTwo
{
    nestednestedtext @0 : Text;
}
struct NestedOne
{
    nestednestedone @0 : NestedNestedOne;
    nestednestedtwo @1 : NestedNestedTwo;
    nestednumber @2: UInt64;
}
struct NestedTwo
{
    nestednestedone @0 : NestedNestedOne;
    nestednestedtwo @1 : NestedNestedTwo;
    nestedtext @2 : Text;
}
struct CapnProto
{
    number @0 : UInt64;
    string @1 : Text;
    nestedone @2 : NestedOne;
    nestedtwo @3 : NestedTwo;
    nestedthree @4 : NestedNestedTwo;
}
