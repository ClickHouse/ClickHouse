addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request))
})

async function handleRequest(request) {
  let url = new URL(request.url);
  url.hostname = 'play-api.clickhouse.tech';
  url.port = 8443;
  url.pathname =  url.pathname.replace('/api/', '/');
  let newHdrs = new Headers()
  
  const init = {
    body: request.body,
    headers: request.headers,
    method: request.method
  }

  let response = await fetch(url, init);

  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText
  })
}
