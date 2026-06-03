use std::fmt;
use std::marker::PhantomData;
use std::str::FromStr;

use anyhow::{Result, bail};
use serde::de;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub enum RoleTag {}
pub enum ChannelTag {}
pub enum StepTag {}
pub enum AgentTag {}

pub type RoleSlug = Slug<RoleTag>;
pub type ChannelSlug = Slug<ChannelTag>;
pub type StepSlug = Slug<StepTag>;
pub type AgentId = Slug<AgentTag>;

pub struct Slug<K> {
    value: String,
    _kind: PhantomData<fn() -> K>,
}

impl<K> Slug<K> {
    pub fn parse(raw: impl Into<String>) -> Result<Self> {
        let value = raw.into();
        validate_slug(&value)?;
        Ok(Self {
            value,
            _kind: PhantomData,
        })
    }

    pub fn as_str(&self) -> &str {
        &self.value
    }
}

impl<K> Clone for Slug<K> {
    fn clone(&self) -> Self {
        Self {
            value: self.value.clone(),
            _kind: PhantomData,
        }
    }
}

impl<K> PartialEq for Slug<K> {
    fn eq(&self, other: &Self) -> bool {
        self.value == other.value
    }
}

impl<K> Eq for Slug<K> {}

impl<K> PartialOrd for Slug<K> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<K> Ord for Slug<K> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.value.cmp(&other.value)
    }
}

impl<K> std::hash::Hash for Slug<K> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.value.hash(state);
    }
}

impl<K> fmt::Debug for Slug<K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("Slug").field(&self.value).finish()
    }
}

impl<K> fmt::Display for Slug<K> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.value)
    }
}

impl<K> FromStr for Slug<K> {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self> {
        Self::parse(value)
    }
}

impl<K> Serialize for Slug<K> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.value)
    }
}

impl<'de, K> Deserialize<'de> for Slug<K> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::parse(String::deserialize(deserializer)?).map_err(de::Error::custom)
    }
}

fn validate_slug(value: &str) -> Result<()> {
    if value.is_empty() {
        bail!("Slug cannot be empty.");
    }
    if value.len() > 96 {
        bail!("Slug `{value}` is too long; use at most 96 characters.");
    }
    if value.starts_with('-') || value.ends_with('-') || value.contains("--") {
        bail!("Slug `{value}` must not start or end with `-`, or contain `--`.");
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        bail!("Slug `{value}` must contain only lowercase ASCII letters, digits, and `-`.");
    }
    Ok(())
}
