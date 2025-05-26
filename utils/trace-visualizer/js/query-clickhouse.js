let add_http_cors_header = (location.protocol != 'file:');

async function queryClickHouse({host, user, password, query, is_stopping, for_each_row, on_error, controller}) {
    // Construct URL
    let url = `${host}?default_format=JSONEachRow&enable_http_compression=1`
    if (add_http_cors_header)
        // For debug purposes, you may set add_http_cors_header from the browser console
        url += '&add_http_cors_header=1';
    if (user)
        url += `&user=${encodeURIComponent(user)}`;
    if (password)
        url += `&password=${encodeURIComponent(password)}`;

    console.log("QUERY", query);

    let response, reply, error;
    try {
        // Send the query
        response = await fetch(url, {
            method: "POST",
            body: query,
            signal: controller.signal,
            headers: { 'Authorization': 'never' }
        });

        if (!response.ok) {
            const reply = await response.text();
            console.log(reply);
            for (line of reply.split('\n')) {
                if (line.startsWith(`{"exception":`)) {
                    throw new Error(`HTTP Status: ${response.status}. Error: ${JSON.parse(line).exception}`);
                }
            }
            throw new Error(`HTTP Status: ${response.status}. Error: ${reply.toString()}`);
        }

        // Initiate stream processing of response body
        const reader = response.body.getReader();
        const decoder = new TextDecoder();

        // Read data row by row
        let buffer = '';
        while (true) {
            const { done, value } = await reader.read();
            if (done)
                break;
            if (is_stopping && is_stopping())
                break;

            buffer += decoder.decode(value, { stream: true });
            let lines = buffer.split('\n');
            for (const line of lines.slice(0, -1)) {
                if (is_stopping && is_stopping())
                    break;
                const data = JSON.parse(line);
                await for_each_row(data);
            }
            buffer = lines[lines.length - 1];
        }
    } catch (e) {
        console.log("CLICKHOUSE QUERY FAILED", e);
        if (on_error) {
            if (e instanceof TypeError) {
                on_error("Network error");
            } else if (e.name === 'AbortError') {
                on_error("Query was cancelled");
            } else {
                on_error(e.toString());
            }
        }
    }
}
