use std::path::PathBuf;

use anyhow::Result;
use capulus::store::{load_toml_or_default, write_toml_file};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::config_store::ice_root_dir;
use crate::listing::{CachedListRow, ListedInstance};
use crate::model::Cloud;
use crate::providers::CloudInstance;
use crate::support::{PROVIDER_DIR_NAME, now_unix_secs};

pub(crate) trait CloudCacheModel {
    type Instance: CloudInstance<ListContext = Self::ListContext>;
    type ListContext: Default;
    type Entry: Clone + Serialize + DeserializeOwned;
    type Store: Default + Serialize + DeserializeOwned;

    const CLOUD: Cloud;

    fn entries(store: &Self::Store) -> &[Self::Entry];
    fn entries_mut(store: &mut Self::Store) -> &mut Vec<Self::Entry>;
    fn key_for_entry(entry: &Self::Entry) -> String;
    fn entry_from_instance(
        instance: &Self::Instance,
        observed_at_unix: u64,
        context: &Self::ListContext,
    ) -> Option<Self::Entry>;
    fn listed_from_entry(entry: &Self::Entry) -> Option<&ListedInstance>;
    fn observed_at_unix(entry: &Self::Entry) -> u64;
}

pub(crate) fn load_cache_store<M>() -> M::Store
where
    M: CloudCacheModel,
{
    load_cloud_cache_or_default(M::CLOUD)
}

pub(crate) fn load_cached_list_rows_for<M>() -> Vec<CachedListRow>
where
    M: CloudCacheModel,
{
    M::entries(&load_cache_store::<M>())
        .iter()
        .filter_map(|entry| {
            Some(CachedListRow {
                key: M::key_for_entry(entry),
                instance: M::listed_from_entry(entry)?.clone(),
                observed_at_unix: M::observed_at_unix(entry),
            })
        })
        .collect()
}

pub(crate) fn persist_instances<M>(instances: &[M::Instance])
where
    M: CloudCacheModel,
{
    let context = M::ListContext::default();
    persist_instances_with_context::<M>(instances, &context);
}

pub(crate) fn persist_instances_with_context<M>(instances: &[M::Instance], context: &M::ListContext)
where
    M: CloudCacheModel,
{
    let observed_at_unix = now_unix_secs();
    let entries = instances
        .iter()
        .filter_map(|instance| M::entry_from_instance(instance, observed_at_unix, context))
        .collect::<Vec<_>>();
    let mut store = M::Store::default();
    *M::entries_mut(&mut store) = entries;
    save_cloud_cache_best_effort(M::CLOUD, &store);
}

pub(crate) fn upsert_instance<M>(instance: &M::Instance)
where
    M: CloudCacheModel,
{
    let context = M::ListContext::default();
    upsert_instance_with_context::<M>(instance, &context);
}

pub(crate) fn upsert_instance_with_context<M>(instance: &M::Instance, context: &M::ListContext)
where
    M: CloudCacheModel,
{
    let Some(entry) = M::entry_from_instance(instance, now_unix_secs(), context) else {
        return;
    };
    let key = M::key_for_entry(&entry);
    let mut store = load_cache_store::<M>();
    if let Some(existing) = M::entries_mut(&mut store)
        .iter_mut()
        .find(|existing| M::key_for_entry(existing) == key)
    {
        *existing = entry;
    } else {
        M::entries_mut(&mut store).push(entry);
    }
    save_cloud_cache_best_effort(M::CLOUD, &store);
}

pub(crate) fn remove_instance<M>(instance: &M::Instance)
where
    M: CloudCacheModel,
{
    remove_key::<M>(&instance.cache_key());
}

pub(crate) fn remove_key<M>(key: &str)
where
    M: CloudCacheModel,
{
    let mut store = load_cache_store::<M>();
    M::entries_mut(&mut store).retain(|entry| M::key_for_entry(entry) != key);
    save_cloud_cache_best_effort(M::CLOUD, &store);
}

fn cloud_cache_slug(cloud: Cloud) -> &'static str {
    match cloud {
        Cloud::VastAi => "vast-ai",
        Cloud::Gcp => "gcp",
        Cloud::Aws => "aws",
        Cloud::Local => "local",
    }
}

fn cloud_cache_path(cloud: Cloud) -> Result<PathBuf> {
    let root = ice_root_dir()?;
    Ok(root
        .join(PROVIDER_DIR_NAME)
        .join(cloud_cache_slug(cloud))
        .join("instance-cache.toml"))
}

fn load_cloud_cache_or_default<T>(cloud: Cloud) -> T
where
    T: DeserializeOwned + Default,
{
    let Ok(path) = cloud_cache_path(cloud) else {
        return T::default();
    };
    load_toml_or_default(&path).unwrap_or_default()
}

fn save_cloud_cache_best_effort<T>(cloud: Cloud, value: &T)
where
    T: Serialize,
{
    let Ok(path) = cloud_cache_path(cloud) else {
        return;
    };
    let _ = write_toml_file(&path, value, None, None);
}
