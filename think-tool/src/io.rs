use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tempfile::NamedTempFile;

pub fn ensure_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("Failed to create `{}`", path.display()))
}

pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("Path `{}` has no parent directory", path.display()))?;
    ensure_dir(parent)?;
    let mut temp = NamedTempFile::new_in(parent)
        .with_context(|| format!("Failed to create temp file in `{}`", parent.display()))?;
    temp.write_all(bytes)
        .with_context(|| format!("Failed to write `{}`", path.display()))?;
    temp.flush()
        .with_context(|| format!("Failed to flush `{}`", path.display()))?;
    temp.persist(path)
        .map_err(|err| err.error)
        .with_context(|| format!("Failed to replace `{}`", path.display()))?;
    Ok(())
}

pub fn write_text(path: &Path, text: &str) -> Result<()> {
    write_atomic(path, text.as_bytes())
}

pub fn write_text_if_missing(path: &Path, text: &str) -> Result<()> {
    if path.exists() {
        return Ok(());
    }
    write_text(path, text)
}

pub fn read_text(path: &Path) -> Result<String> {
    fs::read_to_string(path).with_context(|| format!("Failed to read `{}`", path.display()))
}

pub fn read_optional_text(path: &Path) -> Result<Option<String>> {
    match fs::read_to_string(path) {
        Ok(text) => Ok(Some(text)),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err).with_context(|| format!("Failed to read `{}`", path.display())),
    }
}

pub fn collect_existing_dir<T>(
    path: &Path,
    mut map_entry: impl FnMut(fs::DirEntry) -> Result<Option<T>>,
) -> Result<Vec<T>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    let mut values = Vec::new();
    for entry in
        fs::read_dir(path).with_context(|| format!("Failed to read `{}`", path.display()))?
    {
        if let Some(value) =
            map_entry(entry.with_context(|| format!("Failed to read `{}`", path.display()))?)?
        {
            values.push(value);
        }
    }
    Ok(values)
}

pub fn write_toml<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    write_text(
        path,
        &toml::to_string_pretty(value)
            .with_context(|| format!("Failed to serialize `{}`", path.display()))?,
    )
}

pub fn read_toml<T: DeserializeOwned>(path: &Path) -> Result<T> {
    toml::from_str(&read_text(path)?)
        .with_context(|| format!("Failed to parse `{}`", path.display()))
}

pub fn require_empty_or_missing_dir(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    if !path.is_dir() {
        bail!("`{}` exists and is not a directory.", path.display());
    }
    if fs::read_dir(path)
        .with_context(|| format!("Failed to read `{}`", path.display()))?
        .next()
        .transpose()
        .with_context(|| format!("Failed to read `{}`", path.display()))?
        .is_some()
    {
        bail!("`{}` already exists and is not empty.", path.display());
    }
    Ok(())
}

pub fn canonicalize_existing(path: &Path) -> Result<PathBuf> {
    path.canonicalize()
        .with_context(|| format!("Failed to canonicalize `{}`", path.display()))
}
