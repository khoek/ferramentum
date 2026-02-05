use std::{borrow::Cow, str::FromStr};

#[derive(Debug, thiserror::Error)]
pub enum NsError {
    #[error("unknown option: {0}")]
    UnknownKey(String),
    #[error("invalid value for {key}: {value} ({msg})")]
    ParseError {
        key: String,
        value: String,
        msg: String,
    },
}

#[derive(Debug, Clone)]
pub struct OptionMeta {
    /// Fully qualified key (e.g., "backend.d_model").
    pub key: Cow<'static, str>,
    /// Rust type name (informational).
    pub ty: Cow<'static, str>,
    /// Human description (if provided).
    pub help: Cow<'static, str>,
    /// Stringified default.
    pub default: String,
}

impl OptionMeta {
    pub fn with_default<S1, S2, S3>(key: S1, ty: S2, help: S3, default: String) -> Self
    where
        S1: Into<Cow<'static, str>>,
        S2: Into<Cow<'static, str>>,
        S3: Into<Cow<'static, str>>,
    {
        Self {
            key: key.into(),
            ty: ty.into(),
            help: help.into(),
            default,
        }
    }
}

pub trait CliKeys: Sized + Default {
    /// Return all options (fully qualified keys for this *type* in isolation).
    ///
    /// For nested fields, parents will prefix child keys (e.g., "backend.*").
    fn options_meta() -> Vec<OptionMeta>;

    /// Pretty-printed help table, grouped by first path segment.
    fn options_help() -> String {
        format_options_help(&Self::options_meta())
    }

    /// Apply a single `key=value` override inside this *local* namespace.
    ///
    /// Parents pass the remainder after stripping their prefix, or pass an
    /// already fully-qualified key if this is a root config.
    fn apply_kv(&mut self, key: &str, value: &str) -> Result<(), NsError>;
}

pub fn format_options_help(options: &[OptionMeta]) -> String {
    use itertools::Itertools;
    let mut lines = Vec::new();
    lines.push("Options (set with -o/--option KEY=VALUE; repeatable):".to_string());

    let mut grouped = options
        .iter()
        .into_group_map_by(|m| m.key.split('.').next().unwrap_or(&m.key));

    // Stable top-level group order.
    let mut groups: Vec<_> = grouped.keys().cloned().collect();
    groups.sort();

    for g in groups {
        let mut items = grouped.remove(g).unwrap_or_default();
        items.sort_by(|a, b| a.key.cmp(&b.key));

        lines.push(format!("\n{}.*", g));
        for m in items {
            // Right-pad key for alignment.
            let key = &m.key;
            let ty = &m.ty;
            let help = if m.help.is_empty() { "" } else { &m.help };
            lines.push(format!(
                "  {:<28}  default = {:<10}  ({}) {}",
                key, m.default, ty, help
            ));
        }
    }

    lines.join("\n")
}

// ------ value parsing helpers used by the derive macro ------

pub trait ParseFromStr: Sized {
    fn parse_str(s: &str) -> Result<Self, String>;
}

macro_rules! impl_parse_from_str {
    ($t:ty) => {
        impl ParseFromStr for $t {
            fn parse_str(s: &str) -> Result<Self, String> {
                <Self as FromStr>::from_str(s).map_err(|e| e.to_string())
            }
        }
    };
}

impl_parse_from_str!(usize);
impl_parse_from_str!(u32);
impl_parse_from_str!(u64);
impl_parse_from_str!(i32);
impl_parse_from_str!(i64);
impl_parse_from_str!(f32);
impl_parse_from_str!(f64);
impl_parse_from_str!(bool);

impl ParseFromStr for String {
    fn parse_str(s: &str) -> Result<Self, String> {
        Ok(s.to_string())
    }
}

// Prefix all keys in `meta` with `prefix.`.
pub fn prefix_meta(prefix: &str, mut meta: Vec<OptionMeta>) -> Vec<OptionMeta> {
    if prefix.is_empty() {
        return meta;
    }
    for m in &mut meta {
        m.key = Cow::Owned(format!("{prefix}.{}", m.key));
    }
    meta
}

// Exhaustive split_once for delegation.
pub fn split_once(s: &str, delim: char) -> Option<(&str, &str)> {
    let idx = s.find(delim)?;
    Some((&s[..idx], &s[idx + 1..]))
}
