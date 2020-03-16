addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request))
})

async function handleRequest(request) {
  let raw = await fetch('https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/README.md');
  let text = await raw.text();
  let lines = text.split('\n');
  let skip = true;
  let events = [];
  for (let idx in lines) {
      let line = lines[idx];
      if (skip) {
          if (line.includes('Upcoming Events')) {
              skip = false;
          }
      } else {
          if (!line) { continue; };
          line = line.split('](');
          var tail = line[1].split(') ');
          events.push({
            'signup_link': tail[0],
            'event_name': line[0].replace('* [', ''),
            'event_date': tail[1].slice(0, -1).replace('on ', '')
          });
      }
  }
           
  let response = new Response(JSON.stringify({
    'events': events
  }));
  response.headers.set('Content-Type', 'application/json');
  return response;
}
