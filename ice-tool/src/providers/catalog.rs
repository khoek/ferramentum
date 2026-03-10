use std::collections::{HashMap, HashSet};
use std::hash::Hash;
use std::path::PathBuf;

use anyhow::{Result, bail};

#[derive(Debug)]
pub(crate) struct RefreshCatalogOutcome {
    pub(crate) path: PathBuf,
    pub(crate) entry_count: usize,
    pub(crate) changed_entry_count: usize,
    pub(crate) warning_count: usize,
    pub(crate) warning_summary: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct MachineRegionKey {
    pub(crate) region: String,
    pub(crate) machine: String,
}

impl MachineRegionKey {
    pub(crate) fn new(region: impl Into<String>, machine: impl Into<String>) -> Self {
        Self {
            region: region.into(),
            machine: machine.into(),
        }
    }
}

pub(crate) trait MachineRegionEntry {
    fn machine(&self) -> &str;
    fn region(&self) -> &str;

    fn machine_region_key(&self) -> MachineRegionKey {
        MachineRegionKey::new(self.region(), self.machine())
    }
}

pub(crate) trait MachineRegionSkipEntry: MachineRegionEntry {
    fn skip_reason(&self) -> Option<&str>;
}

pub(crate) fn build_index_by_key<'a, T, K, F>(
    entries: &'a [T],
    mut key_fn: F,
    mut duplicate_error: impl FnMut(&K) -> String,
) -> Result<HashMap<K, &'a T>>
where
    K: Eq + Hash + Clone,
    F: FnMut(&T) -> K,
{
    let mut index = HashMap::with_capacity(entries.len());
    for entry in entries {
        let key = key_fn(entry);
        if index.insert(key.clone(), entry).is_some() {
            bail!("{}", duplicate_error(&key));
        }
    }
    Ok(index)
}

pub(crate) fn build_machine_region_index<'a, T>(
    entries: &'a [T],
    duplicate_error: impl FnMut(&MachineRegionKey) -> String,
) -> Result<HashMap<MachineRegionKey, &'a T>>
where
    T: MachineRegionEntry,
{
    build_index_by_key(entries, T::machine_region_key, duplicate_error)
}

pub(crate) fn changed_entry_count_by_key<T, K, F>(
    previous: &[T],
    current: &[T],
    mut key_fn: F,
) -> usize
where
    T: PartialEq,
    K: Eq + Hash,
    F: FnMut(&T) -> K,
{
    let previous_by_key = previous
        .iter()
        .map(|entry| (key_fn(entry), entry))
        .collect::<HashMap<_, _>>();
    let current_by_key = current
        .iter()
        .map(|entry| (key_fn(entry), entry))
        .collect::<HashMap<_, _>>();
    let updated_or_added = current_by_key
        .iter()
        .filter(|(key, entry)| match previous_by_key.get(*key) {
            Some(previous_entry) => *previous_entry != **entry,
            None => true,
        })
        .count();
    let deleted = previous_by_key
        .keys()
        .filter(|key| !current_by_key.contains_key(*key))
        .count();
    updated_or_added + deleted
}

pub(crate) fn machine_region_key_set<T>(entries: &[T]) -> HashSet<MachineRegionKey>
where
    T: MachineRegionEntry,
{
    entries.iter().map(T::machine_region_key).collect()
}

pub(crate) fn stale_machine_region_entries<'a, T>(
    entries: &'a [T],
    live_keys: &HashSet<MachineRegionKey>,
) -> Vec<&'a T>
where
    T: MachineRegionEntry,
{
    entries
        .iter()
        .filter(|entry| !live_keys.contains(&entry.machine_region_key()))
        .collect()
}

pub(crate) fn skipped_machine_region_key_set<T>(entries: &[T]) -> HashSet<MachineRegionKey>
where
    T: MachineRegionSkipEntry,
{
    entries
        .iter()
        .filter(|entry| entry.skip_reason().is_some())
        .map(T::machine_region_key)
        .collect()
}

pub(crate) fn machine_region_skip_reason<T>(
    entries: &[T],
    machine: &str,
    region: &str,
) -> Option<String>
where
    T: MachineRegionSkipEntry,
{
    entries
        .iter()
        .find(|entry| {
            entry.machine().eq_ignore_ascii_case(machine)
                && entry.region().eq_ignore_ascii_case(region)
                && entry
                    .skip_reason()
                    .is_some_and(|reason| !reason.trim().is_empty())
        })
        .and_then(|entry| entry.skip_reason().map(str::to_owned))
}
