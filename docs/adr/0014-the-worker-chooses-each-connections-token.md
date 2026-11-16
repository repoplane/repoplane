# 14. The worker chooses each connection's token

**Status:** Accepted · 2026-11-16

## Context

Whoever can create a connection must not be able to choose which of a worker's secrets is sent,
nor where. A connection that could name any variable of the worker's environment could name its
Temporal API key, point at another host, and have the worker send the key there.

The people who add connections should not need to know how the workers' environments are laid
out either. A fleet's forge tokens are service credentials (a bot account, a group access token),
created by whoever runs the platform, not by each person using the CLI.

## Decision

A connection carries no credential, not even the name of one. Each worker holds the tokens for
the forges it serves, in its configuration (`tokens`, or `--token WHERE=VARIABLE`), keyed by
where each may be sent: `host`, `host:port`, `host/org` or `host:port/org`, each naming the
variable that holds the token.

A connection gets the token whose key matches its base URL and organisation most closely: a key
with an organisation also covers that GitLab group's subgroups, and a key without a port covers
every port of its host. A connection that no key matches fails at once, without a retry, saying
which queue lacks a token for which host.

The worker reads every listed variable when it starts, and refuses to start if one is unset, is
not a variable name, or holds its Temporal API key.

## Consequences

- A token only ever goes to the host its key names, whatever a connection says. That holds in
  the hybrid deployment too, where the API is not run by the forge's owner.
- Adding a connection to a new forge host or organisation takes two steps by two roles: the
  workers' operator adds the token, then anyone adds the connection. Adding it checks the
  first step: a worker on its queue lists the organisation with the token it holds, or the
  connection is refused, saying why (`--no-check` skips this, for workers not deployed yet).
- Connections on the same host and organisation share a token.
- When GitHub Apps arrive, the App key is held the same way (ADR 0003).
