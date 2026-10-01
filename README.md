# repoplane

**An accurate inventory of your repository fleet, across Forgejo, GitHub, GitLab and Azure DevOps.**

Repoplane connects to an organisation on a forge and keeps a record of every repository in it
(name, default branch, topics, archived state), and reads the HEAD commit of the default
branches you ask about. It
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

$ repoplane repo refresh-heads --connection lab --topic node --wait
Refresh     hrf_06G9N0A2QPZ3M4X5Y6T7V8W9RS
Connection  lab
Status      succeeded

Targeted       3
Observed       3
Changed        3

$ repoplane repo list --topic node --archived false
REPOSITORY                    BRANCH   ARCHIVED   HEAD       TOPICS
acme-services/billing-api     main     false      22d00302   node service sonar
acme-services/ledger-worker   main     false      1833865b   node service
acme-services/node-gateway    main     false      568a26e9   gateway node service
```

Two operations, each doing one thing:

- **`connection reconcile`** records *what exists*. *Discovered* is what the forge listed.
  *Recorded* is what changed in the inventory, so a second run with nothing changed records 0.
  *Inaccessible* is what a complete listing no longer shows (after a direct lookup confirms it
  gone); those repositories are marked, never deleted. It reads no HEAD, so it costs one to three
  requests per organisation on GitHub, GitLab and Azure DevOps.
- **`repo refresh-heads`** reads *what each repository's default branch points at*, for a
  connection's repositories or only those a filter selects. A scan of the `node` repositories
  reads their HEADs, not the organisation's. *Changed* counts HEADs that moved. `repo list`
  shows the last HEAD observed, or `—` until one has been.

## How it is built

Three open-source binaries, orchestrated by [Temporal](https://temporal.io). Whoever runs them can
audit every line that touches their code and their credentials:

| Binary | Role |
|---|---|
| `repoplane` | the CLI: a client of the API, and nothing else |
| `repoplane-api` | owns Postgres, serves `/v1`, and runs the workflows and their Postgres steps as a Temporal worker; never calls a forge |
| `repoplane-worker` | runs the forge steps as a Temporal worker on its queue; holds the forge tokens; talks only to Temporal and forges (API and `git`, which it needs on its `PATH`); never opens a database |

A reconcile and a head refresh are each a Temporal workflow. They retry with backoff, survive a
process dying, and leave room for what comes next: waiting days for a merge, waves, signals.
Because workers only poll Temporal, they can run inside a customer's network against a hosted
API and Temporal, and tokens and source code never leave that network. The C4 views and the
rules are in [`docs/architecture.md`](docs/architecture.md), and the decisions in
[`docs/adr`](docs/adr).

| Forge | `--org` is | Notes |
|---|---|---|
| Forgejo / Gitea | an organisation | `--base-url` required |
| GitHub | an organisation | GitHub Enterprise Server with `--base-url` |
| GitLab | a group path, subgroups included | gitlab.com, or self-managed with `--base-url` |
| Azure DevOps | an organisation, all projects | no topics: `topics` is `null`, and `--topic` is refused |

## Try it

Needs Rust 1.93+ and Docker.

```sh
make up build                  # Postgres :5432, Temporal :7233 (UI :8233), Forgejo :3000; the three binaries
export REPOPLANE_DATABASE_URL=postgres://repoplane:repoplane-not-a-secret@127.0.0.1:5432/repoplane
export REPOPLANE_ADMIN_TOKEN=dev-admin REPOPLANE_TOKEN=dev-admin

target/debug/repoplane-api migrate
target/debug/repoplane-api serve &
FORGEJO_TOKEN=… target/debug/repoplane-worker &

target/debug/repoplane connection add forgejo --name lab --org <your-org> \
  --auth token --token-env FORGEJO_TOKEN --base-url http://localhost:3000
target/debug/repoplane connection reconcile lab --wait
target/debug/repoplane repo refresh-heads --connection lab --wait
target/debug/repoplane repo list
```

Each run is visible in the Temporal UI at <http://localhost:8233>, its workflow id being the run's
id (`rec_…`, `hrf_…`).

## Secrets are named, never written

Every credential is configured by the *name* of an environment variable: the API's database
URL, admin token and Temporal Cloud API key ([`examples/repoplane-api.yaml`](examples/repoplane-api.yaml)); a
connection's forge token (`--token-env`, read only by the worker); the CLI's API token
([`examples/contexts.yaml`](examples/contexts.yaml)). No flag takes a secret, and one pasted
where a name belongs is refused.

## Development

```sh
make lint       # fmt, clippy, and the component boundaries (make boundaries)
make unit       # unit tests: no Docker, no network
make db         # the run steps against a real Postgres (Docker)
make e2e        # end-to-end on a local Forgejo (Docker)
make e2e SANDBOX=gh|gl|ado    # on a real sandbox: source ../fleets/.env first
make e2e-all    # all four
```

The end-to-end suite runs the real binaries, with Postgres and a Temporal dev server in
containers, against the
[fleets](https://github.com/repoplane/fleets) sandboxes. It mutates each sandbox (a topic, a
pushed commit, an archive, a deletion on local), checks that reconcile records exactly that, and
puts the sandbox back with [forgelab](https://github.com/repoplane/forgelab) after every
scenario. Expected state comes from the fleets' `fleet.lock.json`, never from test code. It
expects the fleets checkout at `../fleets` (or `REPOPLANE_FLEETS_DIR`).

## License

[Apache-2.0](LICENSE)
