use std::fs::{self, File, OpenOptions};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use fs2::FileExt;

/// The lock name is part of the downstream credential protocol. Keep it adjacent to the
/// canonical auth file so every Kai process and every downstream Codex process derives the
/// same rendezvous path, even when the active auth.json is a symlink.
pub(crate) fn lock_path(auth_file: &Path) -> PathBuf {
    let canonical = canonical_auth_path(auth_file);
    let name = canonical
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("auth.json");
    canonical.with_file_name(format!("{name}.refresh.lock"))
}

pub(crate) struct Guard {
    _file: File,
}

pub(crate) fn acquire(auth_file: &Path) -> Result<Guard> {
    let path = lock_path(auth_file);
    let parent = path
        .parent()
        .context("credential lock path has no parent directory")?;
    fs::create_dir_all(parent).with_context(|| {
        format!(
            "could not create credential lock directory {}",
            parent.display()
        )
    })?;
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&path)
        .with_context(|| format!("could not open credential refresh lock {}", path.display()))?;
    file.lock_exclusive()
        .with_context(|| format!("could not lock credential refresh lock {}", path.display()))?;
    Ok(Guard { _file: file })
}

fn canonical_auth_path(path: &Path) -> PathBuf {
    if let Ok(canonical) = path.canonicalize() {
        return canonical;
    }
    let Some(parent) = path.parent() else {
        return path.to_owned();
    };
    parent
        .canonicalize()
        .map(|parent| parent.join(path.file_name().unwrap_or_default()))
        .unwrap_or_else(|_| path.to_owned())
}
