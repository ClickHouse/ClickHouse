## Tool to upload results to CI ClickHouse

Currently allows to upload results from `junit_to_html` tool to  ClickHouse CI

```
usage: upload_test_results [-h] --sha SHA --pr PR --file FILE --type
                           {suites,cases} [--user USER] --password PASSWORD
                           [--ca-cert CA_CERT] [--host HOST] [--db DB]

Upload test result to CI ClickHouse.

optional arguments:
  -h, --help            show this help message and exit
  --sha SHA             sha of current commit
  --pr PR               pr of current commit. 0 for master
  --file FILE           file to upload
  --type {suites,cases}
                        Export type
  --user USER           user name
  --password PASSWORD   password
  --ca-cert CA_CERT     CA certificate path
  --host HOST           CI ClickHouse host
  --db DB               CI ClickHouse database name
```

$ ./upload_test_results --sha "cf7eaee3301d4634acdacbfa308ddbe0cc6a061d" --pr "0" --file xyz/cases.jer --type cases --password $PASSWD

CI checks has single commit sha and pr identifier.
While uploading your local results for testing purposes try to use correct sha and pr.

CA Certificate for ClickHouse CI can be obtained from Yandex.Cloud where CI database is hosted
``` bash
wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" -O YandexInternalRootCA.crt
```