@0xd8dd7b35452d1c4c;

struct FakeNullable1
{
    union
    {
        value @0 : Text;
        null @1 : Void;
        trash @2 : Text;
    }
}

struct FakeNullable2
{
    value @0 : Text;
    null @1 : Void;
}

struct Message
{
    nullable1 @0 : FakeNullable1;
    nullable2 @1 : FakeNullable2;
}
