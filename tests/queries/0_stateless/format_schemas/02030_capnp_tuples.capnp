@0x9ef128e10a8010b8;

struct Nested5
{
    x @0 : UInt64;    
}

struct Nested4
{
    nested2 @0 : Nested5;
}

struct Nested3
{
    nested1 @0 : Nested4;
}

struct Nested2
{
    three @0 : UInt64;
    four @1 : UInt64;
}

struct Nested1
{
    one @0 : UInt64;
    two @1 : Nested2;
}

struct Message
{
    value @0 : UInt64;
    tuple1 @1 : Nested1;
    tuple2 @2 : Nested3;
}
