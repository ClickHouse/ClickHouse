export function* sequenceInserter({start_time, interval, parts, bytes})
{
    yield {type: 'sleep', delay: start_time};
    for (let i = 0; i < parts; i++)
    {
        yield {type: 'insert', bytes};
        yield {type: 'sleep', delay: interval};
    }
}
