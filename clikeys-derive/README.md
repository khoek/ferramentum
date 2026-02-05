# clikeys-derive

Proc-macro derive for `clikeys::CliKeys` on config structs.

## Features

- **`#[derive(CliKeys)]`**: Implements `clikeys::CliKeys` for structs with named fields.
- **Field attributes**: `rename`, `help`, `ns`, and `skip` via `#[clikey(...)]`.
- **Constructor**: Generates `T::new_with_options(iter)` for parsing `KEY=VALUE` overrides.

## Example

```rust
use clikeys_derive::CliKeys;

#[derive(Debug, Default, CliKeys)]
pub struct TrainConfig {
    #[clikey(help = "number of training episodes")]
    pub episodes: usize,
}

let cfg = TrainConfig::new_with_options(["episodes=64"])?;
assert_eq!(cfg.episodes, 64);
```

## License

AGPL-3.0-only. See `LICENSE` for details.
