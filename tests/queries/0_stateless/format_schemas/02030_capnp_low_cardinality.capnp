@0x9ef128e10a8010b7;

struct NullableText
{
    union
    {
        value @0 : Text;
        null @1 : Void;
    }
}

struct Message
{
    lc1 @0 : Text;
    lc2 @1 : NullableText;
    lc3 @2 : List(NullableText);
}
