use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow, bail};
use soft_fido2::{
    Authenticator, AuthenticatorCallbacks, AuthenticatorConfig, AuthenticatorOptions, Credential,
    CredentialBackupState, CredentialRef, CtapCommand, Error, UpResult, UvResult,
};

use crate::application::TouchReceipt;
use crate::vault::Vault;

const PRESENCE_TIMEOUT: Duration = Duration::from_secs(30);
// UUIDv5 of the canonical auc-tool repository URL in the UUID URL namespace.
const AUC_AAGUID: [u8; 16] = [
    0x5e, 0x16, 0x05, 0xda, 0x48, 0x1c, 0x5a, 0xb1, 0xa8, 0x8d, 0x5c, 0x33, 0x23, 0x32, 0xe0, 0xb7,
];

#[derive(Clone)]
pub struct PresenceGate {
    shared: Arc<PresenceShared>,
    timeout: Duration,
}

struct PresenceShared {
    state: Mutex<PresenceState>,
    changed: Condvar,
}

#[derive(Default)]
struct PresenceState {
    command: Option<ActiveCommand>,
    pending: Option<PendingPresence>,
}

struct ActiveCommand {
    channel: u32,
    cancelled: bool,
}

struct PendingPresence {
    channel: u32,
    operation: String,
    rp_id: String,
    deadline: Instant,
    outcome: Option<PresenceOutcome>,
}

#[derive(Clone, Copy)]
enum PresenceOutcome {
    Accepted,
    Cancelled,
}

impl PresenceGate {
    pub fn new() -> Self {
        Self::with_timeout(PRESENCE_TIMEOUT)
    }

    fn with_timeout(timeout: Duration) -> Self {
        Self {
            shared: Arc::new(PresenceShared {
                state: Mutex::new(PresenceState::default()),
                changed: Condvar::new(),
            }),
            timeout,
        }
    }

    pub fn begin_command(&self, channel: u32) -> Result<()> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("auc presence lock was poisoned"))?;
        if state.command.is_some() {
            bail!("another CTAP command is already active");
        }
        state.command = Some(ActiveCommand {
            channel,
            cancelled: false,
        });
        Ok(())
    }

    pub fn finish_command(&self, channel: u32) {
        if let Ok(mut state) = self.shared.state.lock()
            && state
                .command
                .as_ref()
                .is_some_and(|command| command.channel == channel)
        {
            state.command = None;
            state.pending = None;
            self.shared.changed.notify_all();
        }
    }

    pub fn cancel(&self, channel: u32) -> bool {
        let Ok(mut state) = self.shared.state.lock() else {
            return false;
        };
        let Some(command) = state
            .command
            .as_mut()
            .filter(|command| command.channel == channel)
        else {
            return false;
        };
        command.cancelled = true;
        if let Some(pending) = state
            .pending
            .as_mut()
            .filter(|pending| pending.channel == channel && pending.outcome.is_none())
        {
            pending.outcome = Some(PresenceOutcome::Cancelled);
        }
        self.shared.changed.notify_all();
        true
    }

    pub fn is_cancelled(&self, channel: u32) -> bool {
        self.shared
            .state
            .lock()
            .ok()
            .and_then(|state| {
                state
                    .command
                    .as_ref()
                    .filter(|command| command.channel == channel)
                    .map(|command| command.cancelled)
            })
            .unwrap_or(true)
    }

    pub fn is_waiting(&self, channel: u32) -> bool {
        self.shared
            .state
            .lock()
            .ok()
            .and_then(|state| {
                state
                    .pending
                    .as_ref()
                    .filter(|pending| pending.channel == channel)
                    .map(|pending| pending.outcome.is_none() && Instant::now() < pending.deadline)
            })
            .unwrap_or(false)
    }

    pub fn has_pending_touch(&self) -> bool {
        self.shared
            .state
            .lock()
            .ok()
            .and_then(|state| {
                state
                    .pending
                    .as_ref()
                    .map(|pending| pending.outcome.is_none() && Instant::now() < pending.deadline)
            })
            .unwrap_or(false)
    }

    pub fn touch(&self) -> Result<TouchReceipt> {
        let mut state = self
            .shared
            .state
            .lock()
            .map_err(|_| anyhow!("auc presence lock was poisoned"))?;
        let pending = state
            .pending
            .as_mut()
            .filter(|pending| pending.outcome.is_none() && Instant::now() < pending.deadline)
            .ok_or_else(|| anyhow!("no auc operation is waiting for presence"))?;
        pending.outcome = Some(PresenceOutcome::Accepted);
        let receipt = TouchReceipt {
            operation: pending.operation.clone(),
            rp_id: pending.rp_id.clone(),
        };
        self.shared.changed.notify_all();
        Ok(receipt)
    }

    fn request(&self, information: &str, rp_id: &str) -> soft_fido2::Result<UpResult> {
        let mut state = self.shared.state.lock().map_err(|_| Error::Other)?;
        let channel = match &state.command {
            Some(command) if !command.cancelled => command.channel,
            _ => return Ok(UpResult::Denied),
        };
        if state.pending.is_some() {
            return Ok(UpResult::Denied);
        }
        let deadline = Instant::now() + self.timeout;
        state.pending = Some(PendingPresence {
            channel,
            operation: operation_label(information).to_string(),
            rp_id: bounded_rp_id(rp_id),
            deadline,
            outcome: None,
        });
        loop {
            let now = Instant::now();
            let outcome = state
                .pending
                .as_ref()
                .filter(|pending| pending.channel == channel)
                .and_then(|pending| pending.outcome);
            match outcome {
                Some(PresenceOutcome::Accepted) => {
                    state.pending = None;
                    return Ok(UpResult::Accepted);
                }
                Some(PresenceOutcome::Cancelled) => {
                    state.pending = None;
                    return Ok(UpResult::Denied);
                }
                None if now >= deadline => {
                    state.pending = None;
                    return Ok(UpResult::Timeout);
                }
                None => {
                    let wait = deadline.saturating_duration_since(now);
                    let (next, _) = self
                        .shared
                        .changed
                        .wait_timeout(state, wait)
                        .map_err(|_| Error::Other)?;
                    state = next;
                }
            }
        }
    }
}

impl Default for PresenceGate {
    fn default() -> Self {
        Self::new()
    }
}

pub struct AuthenticatorEngine {
    authenticator: Authenticator<AucCallbacks>,
}

impl AuthenticatorEngine {
    pub fn new(vault: Vault, presence: PresenceGate) -> Result<Self> {
        let options = AuthenticatorOptions::new()
            .with_resident_keys(true)
            .with_user_presence(true)
            .with_user_verification(Some(false))
            .with_platform_device(false)
            .with_client_pin(Some(true))
            .with_pin_uv_auth_token(Some(true))
            .with_credential_management(Some(true))
            .with_biometric_enrollment(Some(false))
            .with_large_blobs(Some(false))
            .with_enterprise_attestation(Some(false));
        let config = AuthenticatorConfig::builder()
            .aaguid(AUC_AAGUID)
            .commands(vec![
                CtapCommand::MakeCredential,
                CtapCommand::GetAssertion,
                CtapCommand::GetInfo,
                CtapCommand::ClientPin,
                CtapCommand::GetNextAssertion,
                CtapCommand::CredentialManagement,
                CtapCommand::Selection,
            ])
            .options(options)
            .max_credentials(4096)
            .force_resident_keys(true)
            .constant_sign_count(true)
            .default_credential_backup_state(CredentialBackupState::Eligible)
            .algorithms(vec![-7])
            .device_name("auc software authenticator".to_string())
            .vendor_id(0x1209)
            .product_id(0xa0c0)
            .device_version(0x0001)
            .max_pin_retries(8)
            .build();
        let pin_storage = vault.pin_storage();
        let callbacks = AucCallbacks { vault, presence };
        Ok(Self {
            authenticator: Authenticator::with_config_and_pin_storage(
                callbacks,
                config,
                pin_storage,
            )
            .map_err(|_| anyhow!("failed to initialize soft-fido2 authenticator"))?,
        })
    }

    pub fn handle(&mut self, request: &[u8]) -> Result<Vec<u8>> {
        if !request
            .first()
            .is_some_and(|command| ALLOWED_COMMANDS.contains(command))
        {
            return Ok(vec![soft_fido2::StatusCode::InvalidCommand as u8]);
        }
        let mut response = Vec::new();
        self.authenticator
            .handle(request, &mut response)
            .map_err(|_| anyhow!("soft-fido2 command dispatch failed"))?;
        Ok(response)
    }
}

const ALLOWED_COMMANDS: &[u8] = &[0x01, 0x02, 0x04, 0x06, 0x08, 0x0a, 0x0b];

struct AucCallbacks {
    vault: Vault,
    presence: PresenceGate,
}

impl AuthenticatorCallbacks for AucCallbacks {
    fn request_up(
        &self,
        information: &str,
        _user_name: Option<&str>,
        rp_id: &str,
    ) -> soft_fido2::Result<UpResult> {
        self.presence.request(information, rp_id)
    }

    fn request_uv(
        &self,
        _information: &str,
        _user_name: Option<&str>,
        _rp_id: &str,
    ) -> soft_fido2::Result<UvResult> {
        Ok(UvResult::Denied)
    }

    fn write_credential(&self, credential: &CredentialRef<'_>) -> soft_fido2::Result<()> {
        self.vault.write_credential(credential).map_err(vault_error)
    }

    fn read_credential(&self, credential_id: &[u8]) -> soft_fido2::Result<Option<Credential>> {
        self.vault
            .read_credential(credential_id)
            .map_err(vault_error)
    }

    fn delete_credential(&self, credential_id: &[u8]) -> soft_fido2::Result<()> {
        self.vault
            .delete_credential(credential_id)
            .map(|_| ())
            .map_err(vault_error)
    }

    fn list_credentials(
        &self,
        rp_id: &str,
        user_id: Option<&[u8]>,
    ) -> soft_fido2::Result<Vec<Credential>> {
        self.vault
            .list_credentials(rp_id, user_id)
            .map_err(vault_error)
    }

    fn select_credential(
        &self,
        _rp_id: &str,
        credentials: &[Credential],
    ) -> soft_fido2::Result<usize> {
        if credentials.is_empty() {
            Err(Error::Other)
        } else {
            Ok(0)
        }
    }

    fn enumerate_rps(&self) -> soft_fido2::Result<Vec<(String, Option<String>, usize)>> {
        self.vault.enumerate_rps().map_err(vault_error)
    }

    fn credential_count(&self) -> soft_fido2::Result<usize> {
        self.vault.credential_count().map_err(vault_error)
    }

    fn get_timestamp_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0)
    }
}

fn vault_error(error: anyhow::Error) -> Error {
    eprintln!("auc vault operation failed: {error:#}");
    Error::Other
}

fn operation_label(information: &str) -> &'static str {
    let information = information.to_ascii_lowercase();
    if information.contains("registration") || information.contains("make credential") {
        "register passkey"
    } else if information.contains("authentication") || information.contains("assertion") {
        "authenticate"
    } else {
        "confirm authenticator operation"
    }
}

fn bounded_rp_id(rp_id: &str) -> String {
    rp_id.chars().take(253).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn early_touch_is_never_cached_and_touch_is_consumed_once() {
        let gate = PresenceGate::with_timeout(Duration::from_secs(1));
        assert!(gate.touch().is_err());
        gate.begin_command(7).unwrap();
        let callback = gate.clone();
        let thread = std::thread::spawn(move || callback.request("authentication", "example.test"));
        while !gate.has_pending_touch() {
            std::thread::yield_now();
        }
        let receipt = gate.touch().unwrap();
        assert_eq!(receipt.operation, "authenticate");
        assert_eq!(receipt.rp_id, "example.test");
        assert!(gate.touch().is_err());
        assert_eq!(thread.join().unwrap().unwrap(), UpResult::Accepted);
        gate.finish_command(7);
    }

    #[test]
    fn cancellation_wakes_presence_wait_immediately() {
        let gate = PresenceGate::with_timeout(Duration::from_secs(10));
        gate.begin_command(9).unwrap();
        let callback = gate.clone();
        let thread = std::thread::spawn(move || callback.request("registration", "example.test"));
        while !gate.has_pending_touch() {
            std::thread::yield_now();
        }
        assert!(gate.cancel(9));
        assert_eq!(thread.join().unwrap().unwrap(), UpResult::Denied);
        assert!(!gate.has_pending_touch());
        gate.finish_command(9);
    }

    #[test]
    fn operation_labels_do_not_echo_arbitrary_core_text() {
        assert_eq!(
            operation_label("Make Credential registration"),
            "register passkey"
        );
        assert_eq!(operation_label("get assertion"), "authenticate");
        assert_eq!(
            operation_label("attacker controlled detail"),
            "confirm authenticator operation"
        );
    }
}
