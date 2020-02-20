addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request))
})

async function handleRequest(request) {
  let url = new URL(request.url);
  url.hostname = 'repo.yandex.ru';
  url.pathname = '/clickhouse' + url.pathname;
  return fetch(url)
}
