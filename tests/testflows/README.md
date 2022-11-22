## ClickHouse Tests in [TestFlows]

This directory contains integration tests written using [TestFlows] 
that involves several ClickHouse instances, custom configs, ZooKeeper, etc.

## Supported environment

* [Ubuntu] 18.04
* [Python 3] >= 3.6

## Prerequisites

* [Docker] [install](https://docs.docker.com/compose/install/)
* [Docker Compose] [install](https://docs.docker.com/engine/install/)
* [TestFlows] [install](https://testflows.com/handbook/#Installation) 

## Running tests locally

You can run tests locally by passing `--local` and `--clickhouse-binary-path` to the `regression.py`.

* `--local` specifies that regression will be run locally
* `--clickhouse-binary-path` specifies the path to the ClickHouse binary that will be used during the regression run

> Note: you can pass `-h` or `--help` argument to the `regression.py` to see a help message.
>
> ```bash
> python3 regression.py -h
> ```

> Note: make sure that the ClickHouse binary has correct permissions. 
> If you are using `/usr/bin/clickhouse` its owner and group is set to `root:root` by default 
> and it needs to be changed to `clickhouse:clickhouse`. You can change the owner and the group 
> using the following command.
> 
> ```bash
> sudo chown clickhouse:clickhouse /usr/bin/clickhouse
> ```

Using the default ClickHouse installation and its server binary at `/usr/bin/clickhouse`, you can run 
regressions locally using the following command.

```bash
python3 regression.py --local --clickhouse-binary-path "/usr/bin/clickhouse"
```

## Output Verbosity

You can control verbosity of the output by specifying the output format with `-o` or `--output` option.
See `--help` for more details.

## Running Only Selected Tests

You can run only the selected tests by passing `--only` option to the `regression.py`.

For example,

```bash
./regression.py --local --clickhouse-binary-path /usr/bin/clickhouse --only "/clickhouse/rbac/syntax/grant privilege/*"
```

will execute all `rbac/syntax/grant privilege` tests.

If you want to run only a single test such as the `/clickhouse/rbac/syntax/grant privilege/grant privileges/privilege='KILL QUERY', on=('*.*',), allow_introspection=False` you can do it as follows

```bash
./regression.py --local --clickhouse-binary-path /usr/bin/clickhouse --only "/clickhouse/rbac/syntax/grant privilege/grant privileges/privilege='KILL QUERY', on=('[*].[*]',), allow_introspection=False/*"
```

> Note that you need to surround special characters such as `*` with square brackets, for example `[*]`.

> Note that you need to end the filtering pattern with `/*` to run all the steps inside the test.

For more information, please see [Filtering](https://testflows.com/handbook/#Filtering) section in the [TestFlows Handbook].

## How To Debug Why Test Failed

### Step 1: find which tests failed

If [TestFlows] check does not pass you should look at the end of the `test_run.txt.out.log` to find the list
of failing tests. For example,

```bash
clickhouse_testflows_tests_volume
Start tests
➤ Dec 02,2020 22:22:24 /clickhouse
...
Failing

✘ [ Fail ] /clickhouse/rbac/syntax/grant privilege/grant privileges/privilege='SELECT', on=('db0.table0', 'db0.*', '*.*', 'tb0', '*'), allow_column=True, allow_introspection=False
✘ [ Fail ] /clickhouse/rbac/syntax/grant privilege/grant privileges
✘ [ Fail ] /clickhouse/rbac/syntax/grant privilege
✘ [ Fail ] /clickhouse/rbac/syntax
✘ [ Fail ] /clickhouse/rbac
✘ [ Fail ] /clickhouse
```

In this case the failing test is

```
/clickhouse/rbac/syntax/grant privilege/grant privileges/privilege='SELECT', on=('db0.table0', 'db0.*', '*.*', 'tb0', '*'), allow_column=True, allow_introspection=False
```

while the others

```
✘ [ Fail ] /clickhouse/rbac/syntax/grant privilege/grant privileges
✘ [ Fail ] /clickhouse/rbac/syntax/grant privilege
✘ [ Fail ] /clickhouse/rbac/syntax
✘ [ Fail ] /clickhouse/rbac
✘ [ Fail ] /clickhouse
```

failed because the first fail gets "bubble-up" the test execution tree all the way to the top level test which is the
`/clickhouse`.

### Step 2: download `test.log` that contains all raw messages

You need to download the `test.log` that contains all raw messages.

### Step 3: get messages for the failing test

Once you know the name of the failing test and you have the `test.log` that contains all the raw messages
for all the tests, you can use `tfs show test messages` command. 

> You get the `tfs` command by installing [TestFlows]. 

For example,

```bash
cat test.log | tfs show test messages "/clickhouse/rbac/syntax/grant privilege/grant privileges/privilege='SELECT', on=\('db0.table0', 'db0.\*', '\*.\*', 'tb0', '\*'\), allow_column=True, allow_introspection=False"
```

> Note: that characters that are treated as special in extended regular expressions need to be escaped. In this case
> we have to escape the `*`, `(`, and the `)` characters in the test name.

### Step 4: working with the `test.log`

You can use the `test.log` with many of the commands provided by the 
`tfs` utility. 

> See `tfs --help` for more information. 

For example, you can get a list of failing tests from the `test.log` using the
`tfs show fails` command as follows

```bash
$ cat test.log | tfs show fails
```

or get the results using the `tfs show results` command as follows

```bash
$ cat test.log | tfs show results
```

or you can transform the log to see only the new fails using the
`tfs transform fail --new` command as follows

```bash
$ cat test.log | tfs transform fails --new
```

[Python 3]: https://www.python.org/
[Ubuntu]: https://ubuntu.com/ 
[TestFlows]: https://testflows.com
[TestFlows Handbook]: https://testflows.com/handbook/
[Docker]: https://www.docker.com/
[Docker Compose]: https://docs.docker.com/compose/
