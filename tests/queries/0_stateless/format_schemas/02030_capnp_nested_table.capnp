@0x9ef128e10a8010b3;


struct Nested1
{
    one @0 : UInt64;
    two @1 : UInt64;
}

struct Nested
{
    value @0 : List(UInt64);
    array @1 : List(List(UInt64));
    tuple @2 : List(Nested1);
}

struct Message
{
    nested @0 : Nested;
}
