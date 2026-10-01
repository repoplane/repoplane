SHELL := /bin/bash

# The fleets checkout the e2e suite drives: ../fleets unless told otherwise.
REPOPLANE_FLEETS_DIR ?= $(abspath ../fleets)
export REPOPLANE_FLEETS_DIR
SANDBOX ?= local

.PHONY: help build lint boundaries unit db e2e e2e-all ci up down

## Show this help
help:
	@awk '/^## /{doc=substr($$0,4); next} /^[a-z][a-z0-9-]*:/{if(doc!=""){printf "  \033[1m%-11s\033[0m %s\n", substr($$1,1,length($$1)-1), doc; doc=""}; next} {doc=""}' $(MAKEFILE_LIST)

## Build the three binaries
build:
	cargo build -p repoplane-api -p repoplane-worker -p repoplane-cli

## Formatting, clippy, and the component boundaries
lint: boundaries
	cargo fmt --all -- --check
	cargo clippy --workspace --all-targets -- -D warnings

# The audit claim, checked rather than asserted: the component that holds the database cannot
# reach a forge, the one that reaches forges cannot reach the database, the workflows touch
# neither, and the CLI is only an HTTP client. See docs/adr/0001, 0002 and 0012.
## Check that no binary links what its role forbids
boundaries:
	@fail=0; \
	check() { if cargo tree -p $$1 -e normal --prefix none | grep -qE "^($$2) "; then echo "boundary: $$1 links one of $$2"; fail=1; fi; }; \
	check repoplane-cli    'sqlx|repoplane-store|repoplane-forge|repoplane-flow|temporalio-[a-z-]+'; \
	check repoplane-worker 'sqlx|repoplane-store'; \
	check repoplane-api    'repoplane-forge|reqwest'; \
	check repoplane-flow   'sqlx|repoplane-store|repoplane-forge|reqwest'; \
	check repoplane-proto  'sqlx|reqwest|repoplane-forge|repoplane-store|temporalio-[a-z-]+'; \
	exit $$fail
	@echo "boundaries: ok"

## Unit tests: no Docker, no network
unit:
	cargo test --workspace --exclude repoplane-e2e

## The run steps against a real Postgres: races and repeats no e2e run can hit on purpose (Docker)
db:
	REPOPLANE_DB_TESTS=1 cargo test -p repoplane-store --test runs

## End-to-end on one fleets sandbox: SANDBOX=local (Docker) | gh | gl | ado (tokens: source ../fleets/.env)
e2e: build
	REPOPLANE_E2E_SANDBOX=$(SANDBOX) cargo test -p repoplane-e2e -- --nocapture

## End-to-end on all four sandboxes, one after another
e2e-all: build
	@for s in local gh gl ado; do REPOPLANE_E2E_SANDBOX=$$s cargo test -p repoplane-e2e -- --nocapture || exit 1; done

## What CI runs on a pull request
ci: lint unit db
	$(MAKE) e2e SANDBOX=local

## Postgres, Temporal and a local Forgejo for trying repoplane by hand (see README)
up:
	docker compose up -d --wait

## Stop them and drop their data
down:
	docker compose down -v
