let add_http_cors_header = (location.protocol != 'file:');

export async function queryClickHouse({host, user, password, query, is_stopping, for_each_row})
{
    // Construct URL
    let url = `${host}?default_format=JSONEachRow&enable_http_compression=1`
    if (add_http_cors_header)
        // For debug purposes, you may set add_http_cors_header from the browser console
        url += '&add_http_cors_header=1';
    if (user)
        url += `&user=${encodeURIComponent(user)}`;
    if (password)
        url += `&password=${encodeURIComponent(password)}`;

    let response, reply, error;
    try
    {
        // Send the query
        response = await fetch(url, { method: "POST", body: query });
        if (!response.ok)
            throw new Error(`HTTP error. Status: ${response.status}`);

        // Initiate stream processing of response body
        const reader = response.body.getReader();
        const decoder = new TextDecoder();

        // Read data row by row
        let buffer = '';
        while (true)
        {
            const { done, value } = await reader.read();
            if (done)
                break;
            if (is_stopping && is_stopping())
                break;

            buffer += decoder.decode(value, { stream: true });
            let lines = buffer.split('\n');
            for (const line of lines.slice(0, -1))
            {
                if (is_stopping && is_stopping())
                    break;
                const data = JSON.parse(line);
                await for_each_row(data);
            }
            buffer = lines[lines.length - 1];
        }
    }
    catch (e)
    {
        console.log(e);
        error = e.toString();
    }
}