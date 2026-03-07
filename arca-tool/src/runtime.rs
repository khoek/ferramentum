use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};
use libc::{getegid, geteuid};

use crate::command::{
    ensure_command_available, run_command_status, run_command_status_streaming,
    run_command_status_with_input,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContainerRuntime {
    Docker,
    Podman,
}

pub struct ContainerMount<'a> {
    pub source: &'a Path,
    pub target: &'a str,
    pub read_only: bool,
}

impl ContainerRuntime {
    pub fn detect() -> Result<Self> {
        if ensure_command_available("docker").is_ok() {
            Ok(Self::Docker)
        } else if ensure_command_available("podman").is_ok() {
            Ok(Self::Podman)
        } else {
            ensure_command_available("docker")?;
            unreachable!()
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Docker => "docker",
            Self::Podman => "podman",
        }
    }

    pub fn ensure_build_available(self) -> Result<()> {
        match self {
            Self::Docker => ensure_docker_buildx_available(),
            Self::Podman => Ok(()),
        }
    }

    pub fn build(self, tag: &str, context_dir: &Path, containerfile: &Path) -> Result<()> {
        self.ensure_build_available()?;
        let mut command = Command::new(self.name());
        command.args(self.build_args(tag, context_dir, containerfile));
        run_command_status_streaming(&mut command, self.build_context())
    }

    pub fn image_exists(self, tag: &str) -> Result<bool> {
        let mut command = Command::new(self.name());
        command.arg("image").arg("inspect").arg(tag);
        Ok(command.output()?.status.success())
    }

    pub fn save(self, tag: &str, archive_path: &Path) -> Result<()> {
        let mut command = Command::new(self.name());
        command.arg("save").arg("-o").arg(archive_path).arg(tag);
        run_command_status(&mut command, "save container image archive")
    }

    pub fn load(self, archive_path: &Path) -> Result<()> {
        let mut command = Command::new(self.name());
        command.arg("load").arg("-i").arg(archive_path);
        run_command_status_streaming(&mut command, "load container image archive")
    }

    pub fn tag(self, source: &str, target: &str) -> Result<()> {
        let mut command = Command::new(self.name());
        command.arg("tag").arg(source).arg(target);
        run_command_status(&mut command, "tag container image")
    }

    pub fn remove_image(self, target: &str) -> Result<()> {
        let mut command = Command::new(self.name());
        command.arg("image").arg("rm").arg("-f").arg(target);
        run_command_status(&mut command, "remove container image")
    }

    pub fn push(self, target: &str) -> Result<()> {
        let mut command = Command::new(self.name());
        command.arg("push").arg(target);
        run_command_status_streaming(&mut command, "push container image")
    }

    pub fn pull(self, target: &str) -> Result<()> {
        let mut command = Command::new(self.name());
        command.arg("pull").arg(target);
        run_command_status_streaming(&mut command, "pull container image")
    }

    pub fn run(
        self,
        image: &str,
        workdir: Option<&str>,
        envs: &[(String, String)],
        mounts: &[ContainerMount<'_>],
        command_args: &[String],
    ) -> Result<()> {
        let mut command = Command::new(self.name());
        command.arg("run").arg("--rm");
        for mount in mounts {
            let mut spec = format!("{}:{}", mount.source.display(), mount.target);
            if mount.read_only {
                spec.push_str(":ro");
            }
            command.arg("-v").arg(spec);
        }
        if let Some(workdir) = workdir {
            command.arg("-w").arg(workdir);
        }
        for (key, value) in envs {
            command.arg("-e").arg(format!("{key}={value}"));
        }
        command.arg("--user").arg(current_user_spec());
        command.arg(image);
        command.args(command_args);
        run_command_status_streaming(&mut command, "run container")
    }

    pub fn login_password_stdin(
        self,
        registry: &str,
        username: &str,
        password: &str,
    ) -> Result<()> {
        let mut command = Command::new(self.name());
        command
            .arg("login")
            .arg("-u")
            .arg(username)
            .arg("--password-stdin")
            .arg(registry);
        run_command_status_with_input(
            &mut command,
            "log in container runtime",
            password.as_bytes(),
        )
    }

    fn build_args(self, tag: &str, context_dir: &Path, containerfile: &Path) -> Vec<String> {
        match self {
            Self::Docker => vec![
                "buildx".to_owned(),
                "build".to_owned(),
                "--load".to_owned(),
                "-t".to_owned(),
                tag.to_owned(),
                "-f".to_owned(),
                containerfile.display().to_string(),
                context_dir.display().to_string(),
            ],
            Self::Podman => vec![
                "build".to_owned(),
                "-t".to_owned(),
                tag.to_owned(),
                "-f".to_owned(),
                containerfile.display().to_string(),
                context_dir.display().to_string(),
            ],
        }
    }

    fn build_context(self) -> &'static str {
        match self {
            Self::Docker => "build container image with Docker Buildx",
            Self::Podman => "build container image with Podman",
        }
    }
}

fn current_user_spec() -> String {
    unsafe { format!("{}:{}", geteuid(), getegid()) }
}

fn ensure_docker_buildx_available() -> Result<()> {
    let output = Command::new("docker")
        .args(["buildx", "version"])
        .output()
        .context("Failed to check whether Docker Buildx is installed")?;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let detail = if !stderr.is_empty() { stderr } else { stdout };
    if detail.is_empty() {
        bail!(
            "Docker Buildx is required. Install the `docker buildx` plugin; arca no longer supports Docker's legacy builder."
        );
    }
    bail!(
        "Docker Buildx is required. Install the `docker buildx` plugin; arca no longer supports Docker's legacy builder. Docker reported: {detail}"
    );
}

#[cfg(test)]
mod tests {
    use super::ContainerRuntime;
    use std::path::Path;

    #[test]
    fn docker_build_uses_buildx_with_load() {
        assert_eq!(
            ContainerRuntime::Docker.build_args(
                "arca:test",
                Path::new("/tmp/context"),
                Path::new("/tmp/Containerfile"),
            ),
            vec![
                "buildx",
                "build",
                "--load",
                "-t",
                "arca:test",
                "-f",
                "/tmp/Containerfile",
                "/tmp/context",
            ]
        );
    }

    #[test]
    fn podman_build_uses_native_build() {
        assert_eq!(
            ContainerRuntime::Podman.build_args(
                "arca:test",
                Path::new("/tmp/context"),
                Path::new("/tmp/Containerfile"),
            ),
            vec![
                "build",
                "-t",
                "arca:test",
                "-f",
                "/tmp/Containerfile",
                "/tmp/context",
            ]
        );
    }
}
