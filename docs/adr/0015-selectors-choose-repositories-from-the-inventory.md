# 15. Selectors choose repositories from the inventory

**Status:** Accepted · 2026-11-17

## Context

Several things act on a subset of a connection's repositories: `repo list`, a head refresh, and
soon the check rules, which apply to the repositories they are meant for (the `node` services,
everything named `*-starter`). Each has so far had its own filter (`topic`, `archived`), and
they disagree where a forge cannot answer. `repo list --topic infra` without a connection
quietly left out every Azure DevOps repository, because Azure DevOps has no topics (ADR 0009),
and the listing looked like the complete answer.

## Decision

**One selector, one meaning, in the store.** A selector is a predicate over what the inventory
records about a repository. It never reads repository contents; that is what a rule does after
the selection.

| Field | Matches |
|---|---|
| `connection` | repositories of that connection; an operation that already names one (a head refresh) takes it from there |
| `topic` | repositories with the topic; a list means any of them |
| `name` | the repository's last path segment, case-insensitively, with `*` for any run of characters (`*-starter`, `s4a-roma-*-starter`) |
| `archived` | archived or not (disabled, on Azure DevOps) |

Fields are optional and combined with AND. There is no `or` and no `not`; a rule that needs one
is two rules. The canonical form is the structured selector, in the API and in a rule's YAML. The
CLI flags (`--topic`, `--name`, `--archived`) build the same value.

- **`name` is the last segment only.** A pattern containing `/` is refused, with a message
  saying so. A connection is one organisation, so a `acme/` prefix says nothing the
  connection does not; GitLab subgroups and Azure DevOps projects would need their own fields
  (`group`, `project`), not a path pattern, and refusing `/` leaves it free to mean something
  then. `*` is the only wildcard, so the store compiles a name to a `like`, escaping `%`, `_`
  and `\`.
- **Three answers, not two.** A field the forge cannot answer is *unknown*, not *no match*:
  `topic` on a repository whose `topics` is null. SQL's own logic gives this for free, since
  `$1 = any(null)` is null, and one that is false stays false whatever else is unknown. The
  store has the only implementation, as a `where` clause; a repository is *selected* when the
  clause is true and counted *unknown* when it is null.
- **Unknown is never selected, and always counted.** What uses a selector reports how many
  repositories it left out as unknown, per connection and with the reason ("12 repositories not
  selected: azuredevops has no topics"). A listing or a run never looks complete when it is not.
- **Resolved when requested.** A run (a head refresh, a scan) resolves its selector into repository
  ids when it is requested and keeps them, as a head refresh already does, so it does what was
  asked whatever a reconcile does meanwhile. A rule stores the selector, not the ids.

## Consequences

The `topic` and `archived` filters of `repo list` and `repo refresh-heads` become selectors; their
query parameters and flags keep working. The CLI's own "skipping <connection>: no repository
topics" line stays for `refresh-heads` across connections, where each connection is its own run
and the API refuses a topic on a forge without them; `repo list` reports counts instead.

A leading `*` cannot use an index, so a name selector scans the connection's rows: microseconds at
a few thousand repositories, not worth an index.

Not in v1, until a case asks: `or` and `not`, `?` and character classes, `**`, `group` and
`project` fields, matching on the HEAD or on files.
