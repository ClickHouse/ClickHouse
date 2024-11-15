export function* clickHousePartsInserter({host, user, password, query, table, database, partition})
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
    queryClickHouse({
        host,
        user,
        password,
        query,
        for_each_row: (data) => rows.push(data)
    });
    for (const row of rows)
        yield {type: 'insert', bytes}; // TODO(serxa): pass modification_time
}