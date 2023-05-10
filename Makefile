SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c

export DOCKER_BUILDKIT=1
export COMPOSE_DOCKER_CLI_BUILD=1

build:
	poetry build

ci-docker-compose := docker compose -f .ci/docker-compose.yml

test: utest itest

check:
	pre-commit run --all --show-diff-on-failure --color always

utest:
	$(ci-docker-compose) build
	$(ci-docker-compose) down
	$(ci-docker-compose) run --rm xtdb_py_unit

itest:
	$(ci-docker-compose) build
	$(ci-docker-compose) down
	$(ci-docker-compose) run --rm xtdb_py_integration
