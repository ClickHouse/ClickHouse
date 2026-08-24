@0x9ef128e12a8010b2;

struct Nested1
{
    d @0 : UInt64;
    e @1 : Nested2;
}

struct Nested2
{
    f @0 : UInt64;
}

struct Nested
{
    b @0 : UInt64;
    c @1 : Nested1;
}

struct Message
{
    a @0 : Nested;
}
