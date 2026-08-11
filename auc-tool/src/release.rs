use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use capulus::managed::{CargoRegistry, ReleaseSource, ResolvedRelease, VersionTarget};
use semver::Version;
use serde::Deserialize;

const CRATE_API: &str = "https://crates.io/api/v1/crates/auc-tool";

#[derive(Clone)]
pub struct AucReleaseSource {
    client: reqwest::Client,
}

impl AucReleaseSource {
    pub fn new() -> Result<Self> {
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .user_agent(concat!("auc/", env!("CARGO_PKG_VERSION")))
                .build()
                .context("failed to construct the auc release HTTP client")?,
        })
    }

    async fn published_versions(&self) -> Result<Vec<PublishedVersion>> {
        let response: CrateResponse = self
            .client
            .get(CRATE_API)
            .send()
            .await
            .context("failed to query crates.io for auc-tool releases")?
            .error_for_status()
            .context("crates.io rejected the auc-tool release query")?
            .json()
            .await
            .context("crates.io returned malformed auc-tool release metadata")?;
        if response.versions.len() > 10_000 {
            bail!("crates.io returned an unreasonable auc-tool release list");
        }
        response
            .versions
            .into_iter()
            .map(|published| {
                Ok(PublishedVersion {
                    version: Version::parse(&published.num).with_context(|| {
                        format!(
                            "crates.io returned invalid auc-tool version {:?}",
                            published.num
                        )
                    })?,
                    yanked: published.yanked,
                })
            })
            .collect()
    }
}

impl ReleaseSource for AucReleaseSource {
    async fn resolve(&self, target: VersionTarget) -> Result<ResolvedRelease> {
        let versions = self.published_versions().await?;
        let version = match target {
            VersionTarget::Latest => versions
                .into_iter()
                .filter(|published| published.is_installable())
                .map(|published| published.version)
                .max()
                .ok_or_else(|| anyhow!("auc-tool has no non-yanked stable release on crates.io"))?,
            VersionTarget::Exact(value) => {
                let version = Version::parse(&value)
                    .with_context(|| format!("requested auc-tool version {value:?} is invalid"))?;
                if !version.pre.is_empty() || !version.build.is_empty() {
                    bail!("requested auc-tool release must be a stable semantic version");
                }
                versions
                    .into_iter()
                    .find(|published| published.version == version && published.is_installable())
                    .map(|published| published.version)
                    .ok_or_else(|| {
                        anyhow!("auc-tool {version} is not a published non-yanked release")
                    })?
            }
        };
        let release = ResolvedRelease {
            version,
            registry: CargoRegistry::CratesIo,
        };
        release.validate()?;
        Ok(release)
    }
}

#[derive(Deserialize)]
struct CrateResponse {
    versions: Vec<CratesIoVersion>,
}

#[derive(Deserialize)]
struct CratesIoVersion {
    num: String,
    yanked: bool,
}

struct PublishedVersion {
    version: Version,
    yanked: bool,
}

impl PublishedVersion {
    fn is_installable(&self) -> bool {
        !self.yanked && self.version.pre.is_empty() && self.version.build.is_empty()
    }
}
