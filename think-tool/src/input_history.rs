use std::path::PathBuf;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::{io, state::ProjectPaths};

const INPUT_HISTORY_VERSION: u32 = 1;

#[derive(Debug, Default, Serialize, Deserialize)]
struct InputHistory {
    version: u32,
    #[serde(default)]
    entries: Vec<String>,
}

pub fn load(project: &ProjectPaths, name: &str) -> Result<Vec<String>> {
    let path = path(project, name);
    if !path.exists() {
        return Ok(Vec::new());
    }
    Ok(io::read_toml::<InputHistory>(&path)?.entries)
}

pub fn append(project: &ProjectPaths, name: &str, value: &str) -> Result<()> {
    let path = path(project, name);
    if let Some(parent) = path.parent() {
        io::ensure_dir(parent)?;
    }
    let mut history = if path.exists() {
        io::read_toml::<InputHistory>(&path)?
    } else {
        InputHistory {
            version: INPUT_HISTORY_VERSION,
            entries: Vec::new(),
        }
    };
    history.version = INPUT_HISTORY_VERSION;
    history.entries.push(value.to_owned());
    io::write_toml(&path, &history)
}

fn path(project: &ProjectPaths, name: &str) -> PathBuf {
    project
        .runtime_dir()
        .join("input-history")
        .join(format!("{name}.toml"))
}
