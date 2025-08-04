export function* sequenceInserter({start_time = 0, interval = 0, parts = 1, bytes}) {
    // Helper function to handle values or generators
    function get(x) {
        if (typeof x === 'object' && x !== null && typeof x.next === 'function')
            return x.next().value;
        return x;
    }

    yield { type: 'sleep', delay: get(start_time) };
    let count = get(parts);
    for (let i = 0; i < count; i++) {
        yield { type: 'insert', bytes: get(bytes) };
        yield { type: 'sleep', delay: get(interval) };
    }
}
