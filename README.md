# repoplane

**An accurate inventory of your repository fleet, across Forgejo, GitHub, GitLab and Azure DevOps.**

Repoplane connects to an organisation on a forge and keeps a record of every repository in it:
name, default branch, topics, archived state, and the HEAD commit of every default branch. It
is the ground the rest of repoplane stands on: rules evaluated at an exact revision, and fleet
changes verified by re-evaluation rather than by counting merged pull requests.

```console
$ repoplane connection add forgejo --name lab --org acme-services \
    --auth token --token-env FORGEJO_TOKEN --base-url http://localhost:3000

$ repoplane connection reconcile lab --wait
Reconcile   rec_06G9N084KXVH9BQD3DW01R4TA4
Status      succeeded

Discovered     6
Recorded       6
Inaccessible   0

$ repoplane repo list --topic node --archived false
REPOSITORY                    BRANCH   ARCHIVED   HEAD       TOPICS
acme-services/billing-api     main     false      22d00302   node service sonar
acme-services/ledger-worker   main     false      1833865b   node service
acme-services/node-gateway    main     false      568a26e9   gateway node service
```

*Discovered* is what the forge listed. *Recorded* is what changed in the inventory, so a second
run with nothing changed records 0. *Inaccessible* is what a complete listing no longer shows;
those repositories are marked, never deleted.

## How it is built

Three open-source binaries. Whoever runs them can audit every line that touches their code and
their credentials:

| Binary | Role |
|---|---|
| `repoplane` | the CLI: a client of the API, and nothing else |
| `repoplane-api` | owns Postgres; serves `/v1` and the worker task queues; never calls a forge |
| `repoplane-worker` | leases tasks over outbound HTTPS and talks to the forges; holds the forge tokens; never opens a database |

Workers poll task queues the way Temporal workers do. So the API and the workers can run
together, all inside one network, or apart: a hosted API with workers inside the customer's
network, where tokens and source code never leave it. The C4 views and the rules are in
[`docs/architecture.md`](docs/architecture.md), and the decisions in [`docs/adr`](docs/adr).

| Forge | `--org` is | Notes |
|---|---|---|
| Forgejo / Gitea | an organisation | `--base-url` required |
| GitHub | an organisation | GitHub Enterprise Server with `--base-url` |
| GitLab | a group path, subgroups included | gitlab.com, or self-managed with `--base-url` |
| Azure DevOps | an organisation, all projects | no topics: `topics` is `null`, and `--topic` is refused |

## Try it

Needs Rust 1.93+ and Docker.

```sh
make up build                  # Postgres on :5432 and a Forgejo on :3000; the three binaries
export REPOPLANE_DATABASE_URL=postgres://repoplane:repoplane-not-a-secret@127.0.0.1:5432/repoplane
export REPOPLANE_ADMIN_TOKEN=dev-admin REPOPLANE_TOKEN=dev-admin REPOPLANE_WORKER_TOKEN=dev-worker

target/debug/repoplane-api migrate
target/debug/repoplane-api serve &
FORGEJO_TOKEN=… target/debug/repoplane-worker &

target/debug/repoplane connection add forgejo --name lab --org <your-org> \
  --auth token --token-env FORGEJO_TOKEN --base-url http://localhost:3000
target/debug/repoplane connection reconcile lab --wait
target/debug/repoplane repo list
```

## Secrets are named, never written

Every credential is configured by the *name* of an environment variable: the API's database
URL, admin and queue tokens ([`examples/repoplane-api.yaml`](examples/repoplane-api.yaml)); a
connection's forge token (`--token-env`, read only by the worker); the CLI's API token
([`examples/contexts.yaml`](examples/contexts.yaml)). No flag takes a secret, and one pasted
where a name belongs is refused.

## Development

```sh
make lint       # fmt, clippy, and the component boundaries (make boundaries)
make unit       # unit tests: no Docker, no network
make e2e        # end-to-end on a local Forgejo (Docker)
make e2e SANDBOX=gh|gl|ado    # on a real sandbox: source ../fleets/.env first
make e2e-all    # all four
```

The end-to-end suite runs the real binaries against the
[fleets](https://github.com/repoplane/fleets) sandboxes. It mutates each sandbox (a topic, a
pushed commit, an archive, a deletion on local), checks that reconcile records exactly that, and
puts the sandbox back with [forgelab](https://github.com/repoplane/forgelab) after every
scenario. Expected state comes from the fleets' `fleet.lock.json`, never from test code. It
expects the fleets checkout at `../fleets` (or `REPOPLANE_FLEETS_DIR`).

## License

[Apache-2.0](LICENSE)
