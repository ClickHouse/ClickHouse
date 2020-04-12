# Упрощённый скрипт для бекпортирования

Это упрощённый скрипт для бекпортирования. Он определяет, какие пулреквесты ещё не бекпортировали из мастера в указанную ветку. Запускать скрипт нужно из папки, где он лежит, указав ему название ветки. Он предполагает, что ваш апстримный remote называется origin.
```
cd my-clickhouse-repo/utils/simple-backport
git fetch origin
time GITHUB_TOKEN=<my github token> ./backport.sh 20.1
```

Скрипт выведет примитивный отчёт:
```
$ time GITHUB_TOKEN=<my github token> ~/backport.sh 20.3
144 PRs differ between 20.3 and master.
backport	https://github.com/ClickHouse/ClickHouse/pull/10135
backport	https://github.com/ClickHouse/ClickHouse/pull/10121
...
backport	https://github.com/ClickHouse/ClickHouse/pull/9808
backport	https://github.com/ClickHouse/ClickHouse/pull/9410

real	0m1.213s
user	0m1.065s
sys	0m0.311s
```

Также в рабочей папке сгенерируется отчёт `<ваша-ветка>-report.tsv`:

```
$ cat 20.3-report.tsv 
skip	10153	https://github.com/ClickHouse/ClickHouse/pull/10153	pr10153.json
skip	10147	https://github.com/ClickHouse/ClickHouse/pull/10147	pr10147.json
no-backport	10138	https://github.com/ClickHouse/ClickHouse/pull/10138	pr10138.json
backport	10135	https://github.com/ClickHouse/ClickHouse/pull/10135	pr10135.json
skip	10134	https://github.com/ClickHouse/ClickHouse/pull/10134	pr10134.json
...
```

Можно кликать по ссылкам прям из консоли, а можно ещё проще: 

```
$ cat <ветка>-report.tsv | grep ^backport | cut -f3
$ cat <ветка>-report.tsv | grep ^backport | cut -f3 | xargs -n1 xdg-open
```

Такая команда откроет в браузере все пулреквесты, которые надо бекпортировать. Есть и другие статусы, посмотрите какие:

```
$ cat 20.1-report.tsv | cut -f1 | sort | uniq -c | sort -rn
    446 skip
     38 done
     25 conflict
     18 backport
     10 no-backport
```


### Как разметить пулреквест?
По умолчанию бекпортируются все пулреквесты, у которых в описании указана категория чейнжлога Bug fix. Если этого недостаточно, используйте теги:
* v20.1-backported -- этот пулреквест уже бекпортирован в ветку 20.1. На случай, если автоматически не определилось.
* v20.1-no-backport -- в ветку 20.1 бекпортировать не нужно.
* pr-no-backport -- ни в какие ветки бекпортировать не нужно.
* v20.1-conflicts -- при бекпорте в 20.1 произошёл конфликт. Такие пулреквесты скрипт пропускает, к ним можно потом вернуться.
* pr-must-backport -- нужно бекпортировать в поддерживаемые ветки.
* v20.1-must-backport -- нужно бекпортировать в 20.1.


### Я поправил пулреквест, почему скрипт не видит?
В процессе работы скрипт кеширует данные о пулреквестах в текущей папке, чтобы экономить квоту гитхаба. Удалите закешированные файлы, например, для всех реквестов, которые не помечены как пропущенные:
```
$ cat <ваша-ветка>-report.tsv | grep -v "^skip" | cut -f4
$ cat <ваша-ветка>-report.tsv | grep -v "^skip" | cut -f4 | xargs rm
```


