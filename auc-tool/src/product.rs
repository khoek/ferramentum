use std::path::PathBuf;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use capulus::managed::{
    AgentInfo, AgentServiceOptions, ApplicationSocketOptions, ManagedProduct,
    ManagedProductOptions, ManagedRedeployOptions, ManagementClient, ManagementClientOptions,
    ManagementRequest, ManagementResponse, ServiceHardening, SocketOptions, SystemBinary,
    UserBinary,
};
use semver::Version;

pub const APPLICATION_SOCKET_PATH: &str = "/run/auc/agent.sock";
pub const MANAGEMENT_SOCKET_PATH: &str = "/run/auc/capulus.sock";
pub const ACCESS_GROUP: &str = "auc";
const READINESS_RETRY: Duration = Duration::from_secs(1);

pub fn managed_product() -> Result<ManagedProduct> {
    ManagedProductOptions {
        product: "auc".to_string(),
        package: "auc-tool".to_string(),
        version: Version::parse(env!("CARGO_PKG_VERSION"))
            .context("auc package version is not semantic")?,
        system_binaries: vec![
            SystemBinary {
                cargo_name: "auc".to_string(),
                destination: PathBuf::from("/usr/local/bin/auc"),
            },
            SystemBinary {
                cargo_name: "auc-agent".to_string(),
                destination: PathBuf::from("/usr/local/bin/auc-agent"),
            },
        ],
        user_binary: UserBinary {
            cargo_name: "auc".to_string(),
        },
        agent_binary: PathBuf::from("/usr/local/bin/auc-agent"),
        service: AgentServiceOptions {
            description: "auc machine-local passkey authenticator".to_string(),
            executable: PathBuf::from("/usr/local/bin/auc-agent"),
            arguments: vec!["serve".to_string()],
            restart_delay: Duration::from_secs(2),
            network_required: false,
            state_directory_mode: 0o700,
            hardening: ServiceHardening::Strict {
                read_write_paths: vec![PathBuf::from("/var/lib/auc")],
                device_allow: vec![PathBuf::from("/dev/uhid")],
            },
        },
        application_socket: ApplicationSocketOptions::SystemdActivated(SocketOptions {
            path: PathBuf::from(APPLICATION_SOCKET_PATH),
            mode: 0o660,
            group: Some(ACCESS_GROUP.to_string()),
        }),
        management_socket: SocketOptions {
            path: PathBuf::from(MANAGEMENT_SOCKET_PATH),
            mode: 0o660,
            group: Some(ACCESS_GROUP.to_string()),
        },
        redeploy: ManagedRedeployOptions::default(),
    }
    .validate()
    .context("auc managed-product declaration is invalid")
}

pub fn application_agent_info() -> Result<AgentInfo> {
    use crate::application::{ApplicationClient, ApplicationRequest, ApplicationResponse};

    match ApplicationClient::new(APPLICATION_SOCKET_PATH).request(ApplicationRequest::Status)? {
        ApplicationResponse::Status(status) => {
            if !status.device_present {
                bail!("auc UHID device is not present");
            }
            Ok(AgentInfo {
                product: status.product,
                package: status.package,
                version: status.version,
                protocol_major: status.protocol_major,
            })
        }
        _ => Err(anyhow!(
            "auc application health request returned the wrong response"
        )),
    }
}

pub fn wait_until_healthy(expected_version: &Version, timeout: Duration) -> Result<()> {
    let product = managed_product()?;
    let mut options = ManagementClientOptions::new(MANAGEMENT_SOCKET_PATH);
    options.timeout = Duration::from_secs(2);
    let management = ManagementClient::new(options);
    let started = Instant::now();
    let mut last_error = None;
    while started.elapsed() < timeout {
        let result = management
            .request(ManagementRequest::Info)
            .map_err(anyhow::Error::from)
            .and_then(|response| match response {
                ManagementResponse::Info(info) => Ok(info),
                _ => Err(anyhow!(
                    "auc management health request returned the wrong response"
                )),
            })
            .and_then(|management| {
                let application = application_agent_info()?;
                for info in [&management, &application] {
                    if info.product != product.name()
                        || info.package != product.package()
                        || info.version != expected_version.to_string()
                        || info.protocol_major != capulus::managed::PROTOCOL_MAJOR
                    {
                        bail!("auc agent health identity or version does not match");
                    }
                }
                Ok(())
            });
        match result {
            Ok(()) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
        thread::sleep(READINESS_RETRY);
    }
    Err(last_error
        .unwrap_or_else(|| anyhow!("auc readiness deadline elapsed without a response"))
        .context("auc did not become healthy on both sockets"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_has_one_agent_and_two_distinct_sockets() {
        let product = managed_product().unwrap();
        assert_eq!(product.service_name(), "auc-agent.service");
        assert_eq!(product.application_socket_name(), "auc-agent.socket");
        assert_eq!(product.management_socket_name(), "auc-capulus.socket");
        assert_ne!(
            product.application_socket_path(),
            product.management_socket_path()
        );
        product
            .installation_manifest()
            .validate(product.name())
            .unwrap();
    }
}
