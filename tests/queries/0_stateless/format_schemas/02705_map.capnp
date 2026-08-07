@0xdbb9ad1f14bf0b36;

struct Message {
    struct Map {
        struct Entry {
            key @0 : Text;
            value @1 : UInt32;
        }

        entries @0 : List(Entry);
    }

    map @0 : Map;
}
