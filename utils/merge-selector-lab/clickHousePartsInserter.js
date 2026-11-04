import { queryClickHouse } from './queryClickHouse.js';

export async function* clickHousePartsInserter({host, user, password, query, table, database, partition})
{
    if (!query)
    {
        let where = "WHERE active = 1";
        if (table)
            where += ` AND table = ${table}`;
        if (database)
            where += ` AND database = ${database}`;
        if (partition)
            where += ` AND partition = ${partition}`;
        query = `SELECT * FROM system.parts ${where} ORDER BY min_block_number`;
    }
    let rows = [];
    await queryClickHouse({
        host,
        user,
        password,
        query,
        for_each_row: (data) => rows.push(data)
    });
    yield {type: 'sleep', delay: 0};
    for (const row of rows)
        yield {type: 'insert', bytes: +row.bytes_on_disk}; // TODO(serxa): pass modification_time
}
