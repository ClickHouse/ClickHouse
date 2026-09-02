@0x9ef128e10a8010b2;

struct Nested1
{
    b @0 : UInt64;
    c @1 : List(List(UInt64));
}

struct Nested2
{
    e @0 : List(List(Nested3));
    h @1 : List(Nested4);
}

struct Nested3
{
    f @0 : UInt64;
    g @1 : UInt64;
}

struct Nested4
{
    k @0 : List(UInt64);    
}

struct Nested
{
    a @0 : Nested1;
    d @1 : List(Nested2);
}

struct Message
{
    value @0 : UInt64;
    nested @1 : Nested;
}
