use std::fs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use capulus::managed::PeerCredentials;
use zbus::zvariant::OwnedObjectPath;

use super::protocol::PROTOCOL_MAJOR;
use super::{
    ApplicationHandler, ApplicationRequest, ApplicationResponse, ErrorCode, ProtocolError, Status,
};
use crate::authenticator::PresenceGate;
use crate::vault::Vault;

const LOGIN_SERVICE: &str = "org.freedesktop.login1";
const LOGIN_MANAGER_PATH: &str = "/org/freedesktop/login1";
const LOGIN_MANAGER_INTERFACE: &str = "org.freedesktop.login1.Manager";
const LOGIN_SESSION_INTERFACE: &str = "org.freedesktop.login1.Session";
const LOGIN_USER_INTERFACE: &str = "org.freedesktop.login1.User";

struct SessionProperties {
    active: bool,
    remote: bool,
    class: String,
    session_type: String,
    uid: u32,
}

impl SessionProperties {
    async fn load(connection: &zbus::Connection, path: &OwnedObjectPath) -> Result<Self> {
        let session = zbus::Proxy::new(
            connection,
            LOGIN_SERVICE,
            path.as_str(),
            LOGIN_SESSION_INTERFACE,
        )
        .await
        .context("failed to inspect the caller's logind session")?;
        let (uid, _): (u32, OwnedObjectPath) = session.get_property("User").await?;
        Ok(Self {
            active: session.get_property("Active").await?,
            remote: session.get_property("Remote").await?,
            class: session.get_property("Class").await?,
            session_type: session.get_property("Type").await?,
            uid,
        })
    }

    fn is_active_local_interactive(&self, uid: u32) -> bool {
        self.uid == uid
            && self.active
            && !self.remote
            && self.class == "user"
            && matches!(self.session_type.as_str(), "tty" | "x11" | "wayland")
    }
}

#[derive(Clone)]
pub struct LocalSessionAuthorizer {
    connection: zbus::Connection,
}

impl LocalSessionAuthorizer {
    pub async fn connect() -> Result<Self> {
        Ok(Self {
            connection: zbus::Connection::system()
                .await
                .context("failed to connect to the system bus for logind authorization")?,
        })
    }

    pub async fn authorize(&self, peer: PeerCredentials) -> Result<()> {
        validate_process_uid(peer)?;
        let manager = zbus::Proxy::new(
            &self.connection,
            LOGIN_SERVICE,
            LOGIN_MANAGER_PATH,
            LOGIN_MANAGER_INTERFACE,
        )
        .await
        .context("failed to create the logind manager proxy")?;
        match manager.call("GetSessionByPID", &peer.pid).await {
            Ok(path) => {
                if SessionProperties::load(&self.connection, &path)
                    .await?
                    .is_active_local_interactive(peer.uid)
                {
                    return Ok(());
                }
                bail!("caller is not an active local interactive logind user");
            }
            Err(error) if no_session_for_pid(&error) => {}
            Err(error) => {
                return Err(error).context("failed to resolve the caller's logind session");
            }
        }
        let user_path: OwnedObjectPath = manager
            .call("GetUserByPID", &peer.pid)
            .await
            .context("the caller does not belong to a logind user manager")?;
        let user = zbus::Proxy::new(
            &self.connection,
            LOGIN_SERVICE,
            user_path.as_str(),
            LOGIN_USER_INTERFACE,
        )
        .await
        .context("failed to inspect the caller's logind user")?;
        let uid: u32 = user.get_property("UID").await?;
        if uid != peer.uid {
            bail!("caller's logind user no longer matches its Unix socket credentials");
        }
        let sessions: Vec<(String, OwnedObjectPath)> = user.get_property("Sessions").await?;
        for (_, path) in sessions {
            if SessionProperties::load(&self.connection, &path)
                .await?
                .is_active_local_interactive(peer.uid)
            {
                return Ok(());
            }
        }
        bail!("caller is not an active local interactive logind user")
    }
}

fn no_session_for_pid(error: &zbus::Error) -> bool {
    matches!(
        error,
        zbus::Error::MethodError(name, _, _)
            if name.as_str() == "org.freedesktop.login1.NoSessionForPID"
    )
}

pub struct AucApplication {
    vault: Vault,
    presence: PresenceGate,
    device_present: Arc<AtomicBool>,
    authorizer: LocalSessionAuthorizer,
}

impl AucApplication {
    pub fn new(
        vault: Vault,
        presence: PresenceGate,
        device_present: Arc<AtomicBool>,
        authorizer: LocalSessionAuthorizer,
    ) -> Self {
        Self {
            vault,
            presence,
            device_present,
            authorizer,
        }
    }

    async fn require_local_session(&self, peer: PeerCredentials) -> Result<(), ProtocolError> {
        match crate::system::operator_is_authorized(peer.uid) {
            Ok(true) => {}
            Ok(false) => {
                return Err(ProtocolError::new(
                    ErrorCode::Unauthorized,
                    "request requires an authorized auc operator",
                ));
            }
            Err(error) => return Err(internal(error)),
        }
        self.authorizer.authorize(peer).await.map_err(|error| {
            eprintln!("auc rejected application peer: {error:#}");
            ProtocolError::new(
                ErrorCode::Unauthorized,
                "request requires an active local interactive login session",
            )
        })
    }

    fn status(&self) -> Result<ApplicationResponse, ProtocolError> {
        self.vault
            .credential_count()
            .map(|credential_count| {
                ApplicationResponse::Status(Status {
                    product: "auc".to_string(),
                    package: "auc-tool".to_string(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    protocol_major: PROTOCOL_MAJOR,
                    device_present: self.device_present.load(Ordering::Acquire),
                    pending_touch: self.presence.has_pending_touch(),
                    credential_count,
                })
            })
            .map_err(internal)
    }
}

impl ApplicationHandler for AucApplication {
    async fn handle(
        &self,
        peer: PeerCredentials,
        request: ApplicationRequest,
    ) -> Result<ApplicationResponse, ProtocolError> {
        match request {
            ApplicationRequest::Status => self.status(),
            ApplicationRequest::Touch => {
                self.require_local_session(peer).await?;
                self.presence
                    .touch()
                    .map(ApplicationResponse::Touch)
                    .map_err(|_| {
                        ProtocolError::new(
                            ErrorCode::Conflict,
                            "no auc operation is waiting for presence",
                        )
                    })
            }
            ApplicationRequest::ListCredentials => {
                self.require_local_session(peer).await?;
                self.vault
                    .credential_summaries()
                    .map(|credentials| ApplicationResponse::Credentials { credentials })
                    .map_err(internal)
            }
            ApplicationRequest::DeleteCredential { credential_id } => {
                self.require_local_session(peer).await?;
                let credential = decode_credential_id(&credential_id)?;
                match self
                    .vault
                    .delete_credential(&credential)
                    .map_err(internal)?
                {
                    true => Ok(ApplicationResponse::Deleted { credential_id }),
                    false => Err(ProtocolError::new(
                        ErrorCode::NotFound,
                        "credential was not found",
                    )),
                }
            }
            ApplicationRequest::Unknown => Err(ProtocolError::new(
                ErrorCode::BadRequest,
                "auc application method is not supported",
            )),
        }
    }
}

fn validate_process_uid(peer: PeerCredentials) -> Result<()> {
    if peer.pid == 0 {
        bail!("peer supplied an invalid kernel PID");
    }
    let status = fs::read_to_string(format!("/proc/{}/status", peer.pid))
        .context("failed to inspect the application peer process")?;
    let uid_line = status
        .lines()
        .find_map(|line| line.strip_prefix("Uid:"))
        .ok_or_else(|| anyhow!("peer process status has no UID record"))?;
    let uids = uid_line
        .split_whitespace()
        .map(str::parse::<u32>)
        .collect::<std::result::Result<Vec<_>, _>>()?;
    if uids.len() != 4 || uids.iter().any(|uid| *uid != peer.uid) {
        bail!("peer process UID no longer matches its Unix socket credentials");
    }
    Ok(())
}

fn decode_credential_id(value: &str) -> Result<Vec<u8>, ProtocolError> {
    if value.is_empty()
        || value.len() > 2048
        || !value.len().is_multiple_of(2)
        || !value.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ProtocolError::new(
            ErrorCode::BadRequest,
            "credential ID must be lowercase hexadecimal",
        ));
    }
    if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(ProtocolError::new(
            ErrorCode::BadRequest,
            "credential ID must be lowercase hexadecimal",
        ));
    }
    hex::decode(value).map_err(|_| {
        ProtocolError::new(
            ErrorCode::BadRequest,
            "credential ID must be lowercase hexadecimal",
        )
    })
}

fn internal(error: anyhow::Error) -> ProtocolError {
    eprintln!("auc application operation failed: {error:#}");
    ProtocolError::new(ErrorCode::Internal, "auc application operation failed")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn session(active: bool, remote: bool, class: &str, session_type: &str) -> SessionProperties {
        SessionProperties {
            active,
            remote,
            class: class.to_string(),
            session_type: session_type.to_string(),
            uid: 1000,
        }
    }

    #[test]
    fn credential_ids_are_canonical_and_bounded() {
        assert_eq!(
            decode_credential_id("deadbeef").unwrap(),
            [0xde, 0xad, 0xbe, 0xef]
        );
        assert!(decode_credential_id("").is_err());
        assert!(decode_credential_id("DEADBEEF").is_err());
        assert!(decode_credential_id("abc").is_err());
        assert!(decode_credential_id(&"aa".repeat(1025)).is_err());
    }

    #[test]
    fn only_active_local_interactive_user_sessions_supply_presence() {
        assert!(session(true, false, "user", "wayland").is_active_local_interactive(1000));
        assert!(session(true, false, "user", "x11").is_active_local_interactive(1000));
        assert!(session(true, false, "user", "tty").is_active_local_interactive(1000));
        assert!(!session(false, false, "user", "wayland").is_active_local_interactive(1000));
        assert!(!session(true, true, "user", "tty").is_active_local_interactive(1000));
        assert!(!session(true, false, "manager", "unspecified").is_active_local_interactive(1000));
        assert!(!session(true, false, "user", "wayland").is_active_local_interactive(1001));
    }
}
