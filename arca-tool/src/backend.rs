use anyhow::Result;

use crate::artifact::StoredArtifact;
use crate::runtime::ContainerRuntime;

pub(crate) mod rust;

pub(crate) trait GeneratorBackend {
    fn kind(&self) -> &'static str;
    fn build(&self, runtime: ContainerRuntime) -> Result<StoredArtifact>;
}
