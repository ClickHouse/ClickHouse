@0x9ef128e10a8010b8;

struct Nested
{
    field1 @0 : UInt32;
    field2 @1 : UInt32;
}

struct Message
{
    field1 @0 : UInt32;
    nested @1 : Nested;
}
