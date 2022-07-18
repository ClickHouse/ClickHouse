GOCMD=go
GOTEST=$(GOCMD) test
BINARY_NAME=clickhouse-diagnostics
BUILD_DIR=dist

TIMESTAMP := $(shell date +%Y%m%d-%H%M)
COMMIT := $(shell git rev-parse --short HEAD)
DEVLDFLAGS = -ldflags "-X github.com/ClickHouse/clickhouse-diagnostics/cmd.Version=v.dev-${TIMESTAMP} -X github.com/ClickHouse/clickhouse-diagnostics/cmd.Commit=${COMMIT}"

# override with env variable to test other versions e.g. 21.11.10.1
CLICKHOUSE_VERSION ?= latest

GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
CYAN   := $(shell tput -Txterm setaf 6)
RESET  := $(shell tput -Txterm sgr0)

.PHONY: all test build vendor release lint-go test-coverages dep

all: help

release: ## Release is delegated to goreleaser
	$(shell goreleaser release --rm-dist)

## Build:
build: ## Build a binary for local use
	# timestamped version
	$(GOCMD) build ${DEVLDFLAGS} -o $(BINARY_NAME) .

clean: ## Remove build related file
	rm ${BINARY_NAME}
	rm -f checkstyle-report.xml ./coverage.xml ./profile.cov

vendor: ## Copy of all packages needed to support builds and tests in the vendor directory
	$(GOCMD) mod vendor

test: ## Run the tests of the project
	CLICKHOUSE_VERSION=$(CLICKHOUSE_VERSION) $(GOTEST) -v -race `go list ./... | grep -v ./internal/platform/test`

lint-go: ## Use golintci-lint
	docker run --rm -v $(shell pwd):/app -w /app golangci/golangci-lint:latest-alpine golangci-lint run

test-coverage: ## Run the tests of the project and export the coverage
	CLICKHOUSE_VERSION=$(CLICKHOUSE_VERSION) $(GOTEST) -cover -covermode=count -coverprofile=profile.cov `go list ./... | grep -v ./internal/platform/test`
	$(GOCMD) tool cover -func profile.cov

dep:
  $(shell go mod download)

help: ## Show this help.
	@echo ''
	@echo 'Usage:'
	@echo '  ${YELLOW}make${RESET} ${GREEN}<target>${RESET}'
	@echo ''
	@echo 'Targets:'
	@awk 'BEGIN {FS = ":.*?## "} { \
		if (/^[a-zA-Z_-]+:.*?##.*$$/) {printf "    ${YELLOW}%-20s${GREEN}%s${RESET}\n", $$1, $$2} \
		else if (/^## .*$$/) {printf "  ${CYAN}%s${RESET}\n", substr($$1,4)} \
		}' $(MAKEFILE_LIST)
