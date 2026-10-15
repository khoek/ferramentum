//! Codex's append-only session index contains the names shown by its resume picker.
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    fs::OpenOptions,
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom},
    os::unix::fs::{MetadataExt, OpenOptionsExt},
    path::{Path, PathBuf},
};

const MAX_BYTES: u64 = 8 * 1024 * 1024;

#[derive(Default)]
pub(super) struct Names {
    indexes: HashMap<PathBuf, Index>,
}

#[derive(Default)]
struct Index {
    stamp: Option<(u64, u64, u64, i64, i64)>,
    names: HashMap<String, String>,
}

#[derive(Deserialize)]
struct Entry {
    id: String,
    thread_name: String,
}

impl Names {
    pub(super) fn refresh<'a>(&mut self, rollouts: impl Iterator<Item = &'a PathBuf>) {
        let paths: HashSet<_> = rollouts.filter_map(|path| index_path(path)).collect();
        self.indexes.retain(|path, _| paths.contains(path));
        for path in paths {
            let index = self.indexes.entry(path.clone()).or_default();
            if index.refresh(&path).is_err() {
                *index = Index::default();
            }
        }
    }

    pub(super) fn lookup(&self, rollout: &Path, id: &str) -> Option<String> {
        self.indexes
            .get(&index_path(rollout)?)?
            .names
            .get(id)
            .cloned()
    }
}

fn index_path(rollout: &Path) -> Option<PathBuf> {
    let sessions = rollout.ancestors().find(|path| {
        path.file_name()
            .is_some_and(|name| name == "sessions" || name == "archived_sessions")
    })?;
    Some(sessions.parent()?.join("session_index.jsonl"))
}

impl Index {
    fn refresh(&mut self, path: &Path) -> io::Result<()> {
        let mut file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(path)?;
        let meta = file.metadata()?;
        // SAFETY: geteuid has no preconditions.
        if !meta.is_file() || meta.uid() != unsafe { libc::geteuid() } {
            return Err(io::Error::other("invalid session index"));
        }
        let stamp = (
            meta.dev(),
            meta.ino(),
            meta.len(),
            meta.mtime(),
            meta.mtime_nsec(),
        );
        if self.stamp == Some(stamp) {
            return Ok(());
        }
        let start = meta.len().saturating_sub(MAX_BYTES);
        file.seek(SeekFrom::Start(start))?;
        let mut reader = BufReader::new(file.take(MAX_BYTES));
        let mut line = Vec::new();
        if start > 0 {
            reader.read_until(b'\n', &mut line)?;
        }
        self.names.clear();
        loop {
            line.clear();
            if reader.read_until(b'\n', &mut line)? == 0 {
                break;
            }
            if !line.ends_with(b"\n") {
                break;
            }
            if let Ok(entry) = serde_json::from_slice::<Entry>(&line) {
                let name = entry.thread_name.trim();
                if name.is_empty() {
                    self.names.remove(&entry.id);
                } else {
                    self.names.insert(entry.id, name.to_owned());
                }
            }
        }
        self.stamp = Some(stamp);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, io::Write};
    #[test]
    fn newest_name_wins_and_custom_homes_and_partial_appends_work() {
        let root = tempfile::tempdir().unwrap();
        let rollout = root.path().join("sessions/2026/09/30/rollout-test.jsonl");
        let index = root.path().join("session_index.jsonl");
        fs::write(&index, "{\"id\":\"a\",\"thread_name\":\"Old\"}\n{\"id\":\"a\",\"thread_name\":\"New\"}\n{\"id\":\"a\",").unwrap();
        let mut names = Names::default();
        names.refresh([&rollout].into_iter());
        assert_eq!(names.lookup(&rollout, "a").as_deref(), Some("New"));
        fs::OpenOptions::new()
            .append(true)
            .open(&index)
            .unwrap()
            .write_all(b"\"thread_name\":\"Renamed\"}\n")
            .unwrap();
        names.refresh([&rollout].into_iter());
        assert_eq!(names.lookup(&rollout, "a").as_deref(), Some("Renamed"));
        fs::remove_file(index).unwrap();
        names.refresh([&rollout].into_iter());
        assert!(names.lookup(&rollout, "a").is_none());
    }
}
