
addEventListener('fetch', event => {
  event.respondWith(handleRequest(event.request))
})

async function handleRequest(request) {
  if (request.method != 'POST') {
    return new Response('Bad request', {
      status: 400,
      statusText: 'Bad request'
    });
  }
  let url = new URL('https://api.sendgrid.com/v3/mail/send');
  let newHdrs = new Headers();
  newHdrs.set('Authorization', 'Bearer ' + SENDGRID_TOKEN);
  newHdrs.set('Content-Type', 'application/json');
  let args = await request.json();
  let subject = args['name'] + ' wants to meet';
  let content = '';
  let argsKeys = Object.keys(args);
  if (['name', 'email', 'city', 'company'].filter(n=>!argsKeys.includes(n)).length) {
    return new Response('Bad request', {
      status: 400,
      statusText: 'Bad request'
    });
  }
  for (let key in args) {
    content += key.charAt(0).toUpperCase() + key.slice(1);
    content += ':\r\n' + args[key] + '\r\n\r\n';
  }
  let body = {
    "personalizations": [
      {
        "to": [
            {
              "email": "clickhouse-feedback@yandex-team.ru",
              "name": "ClickHouse Core Team"
            }
          ],
        "subject": subject
      }
    ], "content": [
      {
        "type": "text/plain",
        "value": content
      }
    ], "from": {
      "email": "no-reply@clickhouse.tech",
      "name": "ClickHouse Website"
      }, "reply_to": 
      {
        "email": "no-reply@clickhouse.tech",
        "name": "ClickHouse Website"
      }  
    };
  const init = {
    body: JSON.stringify(body),
    headers: newHdrs,
    method: 'POST'
  }

  let response = await fetch(url, init);
  let status = 200;
  if (response.status != 202) {
    status = 200;
  }

  return new Response('{}', {
    status: status,
    statusText: response.statusText.replace('Accepted', 'OK'),
    headers: new Headers({
      'Content-Type': 'application/json'
    })
  })
}
