# clikeys

Namespace-aware `KEY=VALUE` overrides for nested configuration structs.
Pairs well with `clap` via a repeatable `-o/--option KEY=VALUE` flag.

## Features

- **Nested namespaces**: `backend.d_model=256` delegates into child structs.
- **Typed parsing**: Common scalars (`usize`, `bool`, `f64`, `String`, ...) with structured errors.
- **Help table**: Render grouped option help with defaults via `CliKeys::options_help()`.

## Example

```rust
use clikeys_derive::CliKeys;

#[derive(Debug, Default, CliKeys)]
pub struct BackendConfig {
    #[clikey(help = "model width")]
    pub d_model: usize,
}

#[derive(Debug, Default, CliKeys)]
pub struct AppConfig {
    pub backend: BackendConfig,
}

let cfg = AppConfig::new_with_options(["backend.d_model=512"])?;
assert_eq!(cfg.backend.d_model, 512);

println!("{}", <AppConfig as clikeys::CliKeys>::options_help());
```

## License

AGPL-3.0-only. See `LICENSE` for details.
