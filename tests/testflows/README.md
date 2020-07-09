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

[Python 3]: https://www.python.org/
[Ubuntu]: https://ubuntu.com/ 
[TestFlows]: https://testflows.com
[Docker]: https://www.docker.com/
[Docker Compose]: https://docs.docker.com/compose/
