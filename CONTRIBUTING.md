# Contributing

Thank you for wanting to help.

## The Contributor License Agreement

Repoplane is licensed under [Apache-2.0](LICENSE), and contributions are accepted under the
[Contributor License Agreement](CLA.md). On your first pull request the CLA Assistant bot asks you
to sign it: one click with your GitHub account, and it covers every later contribution. A pull
request is merged only once every author of its commits has signed.

You keep the copyright in your work. The agreement gives the maintainer the rights to keep the
project's licensing coherent, including offering Repoplane under other terms later, the way
Temporal does with its own CLA. If your employer has rights to what you write, make sure it allows
you to contribute (section 4).

## Before you open a pull request

- `make lint unit` passes. A change that touches a forge or a run also passes `make e2e`; the
  [Development](README.md#development) section explains the suites.
- Secrets are named, never written: no token in code, tests or fixtures.
