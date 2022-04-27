# Contribution

We keep things simple. Execute all commands in the root folder.

## Requirements

- docker - tested on version 20.10.12.
- golang >= go1.17.6

## Building

Creates a binary `clickhouse-diagnostics` in the local folder. Build will be versioned according to a timestamp. For a versioned release see [Releasing](#releasing).

```bash
make build
```

## Linting

We use [golangci-lint](https://golangci-lint.run/). We use a container to run so no need to install.

```bash
make lint-go
```

## Running Tests

```bash
make test
```

For a coverage report,

```bash
make test-coverage
```

## Adding Collectors

TODO


## Adding Outputs

TODO

## Frames

## Parameter Types


## Releasing

We use git tags for versioning, embedding this information in the binary. For releases we use [goreleaser](https://goreleaser.com). Installation instructions [here](https://goreleaser.com/install/).

To release a new version:

1. Identify current version

```bash
git tag
```

2. Create a new tag with a higher version number e.g.

```bash
git tag -a v1.0.0-beta.3 -m "A summary comment"
```

The tag name must be preceded by a `v` and follow semantic versioning rules.

3. Push the tag to the repo (appropriate permissions required) e.g.

`git push origin v1.0.0-beta.3`

4. Ensure you have a GITHUB_TOKEN set with permissions to the repository. Create a token [here](https://github.com/settings/tokens/new) with all repo scope permissions. Ensure this is set as an environment variable i.e.

`export GITHUB_TOKEN=blah`

5. Run a release

```bash
make release
```

Binaries for each platform will be output in the `dist` directory. Confirm version information has been embedded e.g.

```bash
./dist/clickhouse-diagnostics_linux_amd64/clickhouse-diagnostics version
```

We also produce rpm and deb packages.