@0x9ef128e10a8010b8;

struct Nested2
{
    field1 @0 : UInt32;
    field2 @1 : UInt32;
}

struct Nested
{
    field1 @0 : UInt32;
    nested @1 : Nested2;
    nestedList @2 : List(Nested2);
}

struct Message
{
    nested @0 : Nested;
    nestedList @1 : List(Nested);
}

