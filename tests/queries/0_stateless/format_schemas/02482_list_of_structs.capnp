@0xb6ecde1cd54a101d;

struct Nested {
    nested @0 :List(MyField);
}

struct MyField {
    x @0 :Int64;
    y @1 :Int64;
}

