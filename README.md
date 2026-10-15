# ferramentum

A repository housing various useful Rust CLI utilities.

- `arca-tool`: build and publish containerized Rust crate artifacts
- `ice-tool`: manage cloud VM instances across `vast.ai`, `gcp`, `aws`, and `verda`
- `kai-tool`: Codex launch/resume with configurable credential rotation and source listings
- `ocular-tool`: OpenConnect/AnyConnect SSO CLI bridge
- `think-tool`: coordinate persistent agent sessions on complex projects

## Development checks

The workspace currently includes an Auc path dependency on a sibling
`caudex/capulus` checkout. If that checkout is unavailable, check Kai and Ice
independently with the maintained helper:

```sh
bash scripts/check-cli.sh kai-tool test
bash scripts/check-cli.sh ice-tool test
bash scripts/check-cli.sh kai-tool clippy --all-targets -- -D warnings
bash scripts/check-cli.sh ice-tool clippy --all-targets
bash scripts/check-cli.sh kai-tool fmt -- --check
bash scripts/check-cli.sh ice-tool fmt -- --check
```

The helper copies the selected package into a temporary workspace under `target`,
preserving the root dependency declarations, patches, profiles, and lockfile when
present. It removes the temporary source copy after the command and retains build
artifacts in `target/kai-standalone` or `target/ice-standalone`; `CARGO_TARGET_DIR`
can select an existing cache. Without a root lockfile, Cargo resolves compatible
dependencies. These commands validate the selected CLI, not the entire workspace.
They use local fixtures and do not rent cloud instances.

Kai's additional desktop checks are documented in its
[window-status guide](kai-tool/README.md#window-status) and
[GNOME integration guide](kai-tool/integrations/gnome/README.md).
