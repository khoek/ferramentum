use std::collections::{HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::ops::Deref;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::atomic::{Ordering, compiler_fence};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{Context, Result, anyhow, bail};
use chacha20poly1305::aead::{Aead, KeyInit, Payload};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use rand::RngExt as _;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use soft_fido2::{Credential, CredentialBackupState, CredentialRef, PinState, StatusCode};
use tempfile::Builder;
use zeroize::{Zeroize, Zeroizing};

use crate::application::CredentialSummary;

const VAULT_ROOT: &str = "/var/lib/auc";
const IDENTITY_FILE: &str = "identity.cbor";
const EVENTS_DIRECTORY: &str = "events";
const PIN_STATE_FILE: &str = "pin-state.cbor";
const IDENTITY_SCHEMA: u16 = 1;
const EVENT_SCHEMA: u16 = 1;
const PIN_SCHEMA: u16 = 1;
const MAX_IDENTITY_BYTES: u64 = 4096;
const MAX_EVENT_BYTES: u64 = 256 * 1024;
const MAX_PIN_BYTES: u64 = 16 * 1024;
const MAX_CREDENTIALS: usize = 4096;
const EVENT_DOMAIN: &[u8] = b"auc-credential-event-v1\0";
const EVENT_SIGNATURE_DOMAIN: &[u8] = b"auc-credential-event-signature-v1\0";
const PIN_DOMAIN: &[u8] = b"auc-device-local-pin-state-v1\0";

#[derive(Clone)]
pub struct Vault {
    inner: Arc<Mutex<VaultState>>,
}

impl Vault {
    pub fn open() -> Result<Self> {
        if !rustix::process::geteuid().is_root() {
            bail!("auc vault requires root");
        }
        Self::open_path(Path::new(VAULT_ROOT), 0, 0)
    }

    pub fn purge() -> Result<()> {
        if !rustix::process::geteuid().is_root() {
            bail!("auc vault purge requires root");
        }
        drop(Self::open()?);
        let root = Path::new(VAULT_ROOT);
        let metadata = fs::symlink_metadata(root)?;
        if !metadata.file_type().is_dir()
            || metadata.uid() != 0
            || metadata.gid() != 0
            || metadata.mode() & 0o7777 != 0o700
        {
            bail!("auc vault root failed ownership, type, or mode validation");
        }
        for entry in fs::read_dir(root)? {
            let entry = entry?;
            let name = entry.file_name();
            if !matches!(
                name.to_str(),
                Some(IDENTITY_FILE | EVENTS_DIRECTORY | PIN_STATE_FILE | "access-policy.json")
            ) {
                bail!(
                    "refusing to purge unexpected path in the auc vault: {}",
                    entry.path().display()
                );
            }
            let metadata = fs::symlink_metadata(entry.path())?;
            if name == EVENTS_DIRECTORY {
                if !metadata.file_type().is_dir()
                    || metadata.uid() != 0
                    || metadata.gid() != 0
                    || metadata.mode() & 0o7777 != 0o700
                {
                    bail!("auc events path changed before vault purge");
                }
            } else if !metadata.file_type().is_file()
                || metadata.uid() != 0
                || metadata.gid() != 0
                || metadata.mode() & 0o7777 != 0o600
            {
                bail!("auc private file changed before vault purge");
            }
        }
        fs::remove_dir_all(root).context("failed to remove the validated auc vault")?;
        File::open("/var/lib")?.sync_all()?;
        Ok(())
    }

    #[cfg(test)]
    fn open_for_test(path: &Path) -> Result<Self> {
        Self::open_path(
            path,
            rustix::process::geteuid().as_raw(),
            rustix::process::getegid().as_raw(),
        )
    }

    fn open_path(root: &Path, uid: u32, gid: u32) -> Result<Self> {
        ensure_secure_directory(root, uid, gid)?;
        remove_uncommitted_files(root, ".auc-identity-", uid, gid)?;
        remove_uncommitted_files(root, ".auc-pin-", uid, gid)?;
        let events = root.join(EVENTS_DIRECTORY);
        ensure_secure_directory(&events, uid, gid)?;
        remove_uncommitted_files(&events, ".auc-event-", uid, gid)?;
        let identity = load_or_create_identity(root, uid, gid)?;
        let mut state = VaultState {
            root: root.to_path_buf(),
            uid,
            gid,
            vault_key: LockedSecret::new(identity.vault_key)?,
            signing_seed: LockedSecret::new(identity.signing_key)?,
            device_id: identity.device_id,
            credentials: HashMap::new(),
            tombstones: HashSet::new(),
            event_ids: HashSet::new(),
            next_sequence: 1,
            last_hash: [0; 32],
            pin_version: 0,
        };
        state.load_events()?;
        state.pin_version = state.load_pin_state()?.version;
        Ok(Self {
            inner: Arc::new(Mutex::new(state)),
        })
    }

    pub fn device_unique_name(&self) -> Result<String> {
        Ok(format!("auc-{}", hex::encode(self.lock()?.device_id)))
    }

    pub fn write_credential(&self, credential: &CredentialRef<'_>) -> Result<()> {
        let mut credential = credential.to_owned();
        credential.sign_count = 0;
        credential.backup_state = CredentialBackupState::Eligible;
        self.lock()?.append(EventPayload::Upsert {
            credential: credential
                .to_bytes()
                .map_err(|_| anyhow!("soft-fido2 failed to encode a newly created credential"))?,
        })
    }

    pub fn read_credential(&self, credential_id: &[u8]) -> Result<Option<Credential>> {
        Ok(self.lock()?.credentials.get(credential_id).cloned())
    }

    pub fn delete_credential(&self, credential_id: &[u8]) -> Result<bool> {
        let mut state = self.lock()?;
        if !state.credentials.contains_key(credential_id) {
            return Ok(false);
        }
        state.append(EventPayload::Tombstone {
            credential_id: credential_id.to_vec(),
        })?;
        Ok(true)
    }

    pub fn list_credentials(&self, rp_id: &str, user_id: Option<&[u8]>) -> Result<Vec<Credential>> {
        let state = self.lock()?;
        let mut credentials = state
            .credentials
            .values()
            .filter(|credential| {
                credential.rp.id == rp_id
                    && user_id.is_none_or(|user_id| credential.user.id == user_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        credentials.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(credentials)
    }

    pub fn all_credentials(&self) -> Result<Vec<Credential>> {
        let state = self.lock()?;
        let mut credentials = state.credentials.values().cloned().collect::<Vec<_>>();
        credentials.sort_by(|left, right| {
            left.rp
                .id
                .cmp(&right.rp.id)
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(credentials)
    }

    pub fn credential_summaries(&self) -> Result<Vec<CredentialSummary>> {
        Ok(self
            .all_credentials()?
            .into_iter()
            .map(|credential| CredentialSummary {
                credential_id: hex::encode(&credential.id),
                rp_id: credential.rp.id,
                user_name: credential.user.name,
                discoverable: credential.discoverable,
                backup_eligible: credential.backup_state.is_eligible(),
                backed_up: credential.backup_state.is_backed_up(),
            })
            .collect())
    }

    pub fn credential_count(&self) -> Result<usize> {
        Ok(self.lock()?.credentials.len())
    }

    pub fn enumerate_rps(&self) -> Result<Vec<(String, Option<String>, usize)>> {
        let state = self.lock()?;
        let mut relying_parties = HashMap::<String, (Option<String>, usize)>::new();
        for credential in state.credentials.values() {
            let entry = relying_parties
                .entry(credential.rp.id.clone())
                .or_insert_with(|| (credential.rp.name.clone(), 0));
            entry.1 += 1;
        }
        let mut relying_parties = relying_parties
            .into_iter()
            .map(|(id, (name, count))| (id, name, count))
            .collect::<Vec<_>>();
        relying_parties.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(relying_parties)
    }

    pub fn pin_storage(&self) -> VaultPinStorage {
        VaultPinStorage {
            vault: self.clone(),
        }
    }

    fn lock(&self) -> Result<MutexGuard<'_, VaultState>> {
        self.inner
            .lock()
            .map_err(|_| anyhow!("auc vault lock was poisoned"))
    }
}

#[derive(Clone)]
pub struct VaultPinStorage {
    vault: Vault,
}

impl soft_fido2::PinStorageCallbacks for VaultPinStorage {
    fn load_pin_state(&self) -> std::result::Result<PinState, StatusCode> {
        self.vault
            .lock()
            .and_then(|state| state.load_pin_state())
            .map_err(|error| {
                eprintln!("auc PIN state load failed: {error:#}");
                StatusCode::Other
            })
    }

    fn save_pin_state(&self, state: &PinState) -> std::result::Result<(), StatusCode> {
        self.vault
            .lock()
            .and_then(|mut vault| vault.save_pin_state(state))
            .map_err(|error| {
                eprintln!("auc PIN state save failed: {error:#}");
                StatusCode::Other
            })
    }
}

struct VaultState {
    root: PathBuf,
    uid: u32,
    gid: u32,
    vault_key: LockedSecret,
    signing_seed: LockedSecret,
    device_id: [u8; 16],
    credentials: HashMap<Vec<u8>, Credential>,
    tombstones: HashSet<Vec<u8>>,
    event_ids: HashSet<[u8; 16]>,
    next_sequence: u64,
    last_hash: [u8; 32],
    pin_version: u64,
}

impl VaultState {
    fn cipher(&self) -> XChaCha20Poly1305 {
        XChaCha20Poly1305::new_from_slice(&*self.vault_key)
            .expect("a locked auc vault key always has 32 bytes")
    }

    fn signing_key(&self) -> SigningKey {
        SigningKey::from_bytes(&self.signing_seed)
    }

    fn load_events(&mut self) -> Result<()> {
        let mut paths = fs::read_dir(self.root.join(EVENTS_DIRECTORY))?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<std::io::Result<Vec<_>>>()?;
        paths.sort();
        for path in paths {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| anyhow!("auc event filename is not UTF-8"))?;
            if !name.ends_with(".cbor") {
                bail!("unexpected file in auc event store: {name}");
            }
            let bytes = read_private_file(&path, self.uid, self.gid, MAX_EVENT_BYTES)
                .with_context(|| format!("failed to read auc event {name}"))?;
            let envelope: EventEnvelope = ciborium::from_reader(bytes.as_slice())
                .with_context(|| format!("failed to decode auc event {name}"))?;
            let envelope_hash: [u8; 32] = Sha256::digest(&bytes).into();
            self.apply_envelope(name, &envelope, &envelope_hash)?;
            self.last_hash = envelope_hash;
            self.next_sequence = self
                .next_sequence
                .checked_add(1)
                .ok_or_else(|| anyhow!("auc event sequence overflow"))?;
        }
        Ok(())
    }

    fn apply_envelope(
        &mut self,
        filename: &str,
        envelope: &EventEnvelope,
        envelope_hash: &[u8; 32],
    ) -> Result<()> {
        envelope
            .validate(
                self.next_sequence,
                self.last_hash,
                &self.signing_key().verifying_key(),
            )
            .with_context(|| format!("auc event validation failed for {filename}"))?;
        let expected_name = event_filename(envelope.sequence, &envelope.event_id, envelope_hash);
        if filename != expected_name {
            bail!("auc event filename does not match its authenticated identity: {filename}");
        }
        if !self.event_ids.insert(envelope.event_id) {
            bail!("auc event contains a duplicate event ID: {filename}");
        }
        let associated_data = envelope.associated_data();
        let plaintext = Zeroizing::new(
            self.cipher()
                .decrypt(
                    &XNonce::from(envelope.nonce),
                    Payload {
                        msg: &envelope.ciphertext,
                        aad: &associated_data,
                    },
                )
                .map_err(|_| anyhow!("auc event AEAD authentication failed: {filename}"))?,
        );
        let payload = Zeroizing::new(
            ciborium::from_reader::<EventPayload, _>(plaintext.as_slice())
                .with_context(|| format!("auc event payload is malformed: {filename}"))?,
        );
        if payload.domain() != envelope.payload_domain {
            bail!("auc event payload domain mismatch: {filename}");
        }
        self.apply_payload(&payload)
            .with_context(|| format!("auc event payload is invalid: {filename}"))
    }

    fn apply_payload(&mut self, payload: &EventPayload) -> Result<()> {
        match payload {
            EventPayload::Upsert { credential } => {
                let credential = Credential::from_bytes(credential)
                    .map_err(|_| anyhow!("credential CBOR is invalid"))?;
                validate_credential(&credential)?;
                if self.tombstones.contains(&credential.id) {
                    bail!("credential tombstone cannot be resurrected");
                }
                if !self.credentials.contains_key(&credential.id)
                    && self.credentials.len() >= MAX_CREDENTIALS
                {
                    bail!("credential store exceeds its safety limit");
                }
                self.credentials.insert(credential.id.clone(), credential);
            }
            EventPayload::Tombstone { credential_id } => {
                validate_credential_id(credential_id)?;
                self.credentials.remove(credential_id.as_slice());
                self.tombstones.insert(credential_id.clone());
            }
        }
        Ok(())
    }

    fn append(&mut self, payload: EventPayload) -> Result<()> {
        let payload = Zeroizing::new(payload);
        self.apply_payload_validation(&payload)?;
        let mut payload_bytes = Zeroizing::new(Vec::new());
        ciborium::into_writer(&*payload, &mut *payload_bytes)?;
        if payload_bytes.len() > MAX_EVENT_BYTES as usize / 2 {
            bail!("auc credential event payload is too large");
        }
        let mut event_id = [0_u8; 16];
        let mut nonce = [0_u8; 24];
        let mut random = rand::rng();
        loop {
            random.fill(&mut event_id);
            if !self.event_ids.contains(&event_id) {
                break;
            }
        }
        random.fill(&mut nonce);
        let signing_key = self.signing_key();
        let mut envelope = EventEnvelope {
            schema: EVENT_SCHEMA,
            sequence: self.next_sequence,
            event_id,
            previous_hash: self.last_hash,
            signer: signing_key.verifying_key().to_bytes(),
            payload_domain: payload.domain(),
            nonce,
            ciphertext: Vec::new(),
            signature: Vec::new(),
        };
        let associated_data = envelope.associated_data();
        envelope.ciphertext = self
            .cipher()
            .encrypt(
                &XNonce::from(nonce),
                Payload {
                    msg: &payload_bytes,
                    aad: &associated_data,
                },
            )
            .map_err(|_| anyhow!("failed to encrypt auc credential event"))?;
        envelope.signature = signing_key
            .sign(&envelope.signature_message())
            .to_bytes()
            .to_vec();
        let mut bytes = Vec::new();
        ciborium::into_writer(&envelope, &mut bytes)?;
        if bytes.len() > MAX_EVENT_BYTES as usize {
            bail!("auc credential event envelope is too large");
        }
        let event_hash: [u8; 32] = Sha256::digest(&bytes).into();
        let destination = self.root.join(EVENTS_DIRECTORY).join(event_filename(
            self.next_sequence,
            &event_id,
            &event_hash,
        ));
        persist_new_private_file(&destination, &bytes, ".auc-event-", self.uid, self.gid)?;
        self.apply_payload(&payload)?;
        self.event_ids.insert(event_id);
        self.last_hash = event_hash;
        self.next_sequence = self
            .next_sequence
            .checked_add(1)
            .ok_or_else(|| anyhow!("auc event sequence overflow"))?;
        Ok(())
    }

    fn apply_payload_validation(&self, payload: &EventPayload) -> Result<()> {
        match payload {
            EventPayload::Upsert { credential } => {
                let credential = Credential::from_bytes(credential)
                    .map_err(|_| anyhow!("credential CBOR is invalid"))?;
                validate_credential(&credential)?;
                if self.tombstones.contains(&credential.id) {
                    bail!("credential tombstone cannot be resurrected");
                }
                if !self.credentials.contains_key(&credential.id)
                    && self.credentials.len() >= MAX_CREDENTIALS
                {
                    bail!("credential store exceeds its safety limit");
                }
                Ok(())
            }
            EventPayload::Tombstone { credential_id } => validate_credential_id(credential_id),
        }
    }

    fn load_pin_state(&self) -> Result<PinState> {
        let path = self.root.join(PIN_STATE_FILE);
        let bytes = match read_private_file(&path, self.uid, self.gid, MAX_PIN_BYTES) {
            Ok(bytes) => bytes,
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
            {
                return Ok(PinState::default());
            }
            Err(error) => return Err(error),
        };
        let envelope: PinEnvelope =
            ciborium::from_reader(bytes.as_slice()).context("failed to decode auc PIN envelope")?;
        if envelope.schema != PIN_SCHEMA {
            bail!("unsupported auc PIN state schema {}", envelope.schema);
        }
        let associated_data = pin_associated_data(envelope.version);
        let plaintext = Zeroizing::new(
            self.cipher()
                .decrypt(
                    &XNonce::from(envelope.nonce),
                    Payload {
                        msg: &envelope.ciphertext,
                        aad: &associated_data,
                    },
                )
                .map_err(|_| anyhow!("auc PIN state authentication failed"))?,
        );
        let state: PinState =
            ciborium::from_reader(plaintext.as_slice()).context("auc PIN state is malformed")?;
        if state.version != envelope.version {
            bail!("auc PIN state version does not match its authenticated envelope");
        }
        Ok(state)
    }

    fn save_pin_state(&mut self, state: &PinState) -> Result<()> {
        if state.version < self.pin_version {
            bail!("auc refused to roll PIN retry state backward");
        }
        let mut plaintext = Zeroizing::new(Vec::new());
        ciborium::into_writer(state, &mut *plaintext)?;
        let mut nonce = [0_u8; 24];
        rand::rng().fill(&mut nonce);
        let associated_data = pin_associated_data(state.version);
        let envelope = PinEnvelope {
            schema: PIN_SCHEMA,
            version: state.version,
            nonce,
            ciphertext: self
                .cipher()
                .encrypt(
                    &XNonce::from(nonce),
                    Payload {
                        msg: &plaintext,
                        aad: &associated_data,
                    },
                )
                .map_err(|_| anyhow!("failed to encrypt auc PIN state"))?,
        };
        let mut bytes = Vec::new();
        ciborium::into_writer(&envelope, &mut bytes)?;
        if bytes.len() > MAX_PIN_BYTES as usize {
            bail!("auc PIN state exceeds its safety limit");
        }
        replace_private_file(
            &self.root.join(PIN_STATE_FILE),
            &bytes,
            ".auc-pin-",
            self.uid,
            self.gid,
        )?;
        self.pin_version = state.version;
        Ok(())
    }
}

#[derive(Deserialize, Serialize, Zeroize)]
#[serde(deny_unknown_fields)]
struct IdentityFile {
    schema: u16,
    vault_key: [u8; 32],
    signing_key: [u8; 32],
    device_id: [u8; 16],
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
enum PayloadDomain {
    CredentialUpsert,
    CredentialTombstone,
}

impl PayloadDomain {
    fn as_byte(self) -> u8 {
        match self {
            Self::CredentialUpsert => 1,
            Self::CredentialTombstone => 2,
        }
    }
}

#[derive(Deserialize, Serialize, Zeroize)]
#[serde(rename_all = "kebab-case", tag = "kind", deny_unknown_fields)]
enum EventPayload {
    Upsert {
        #[serde(with = "serde_bytes")]
        credential: Vec<u8>,
    },
    Tombstone {
        #[serde(with = "serde_bytes")]
        credential_id: Vec<u8>,
    },
}

impl EventPayload {
    fn domain(&self) -> PayloadDomain {
        match self {
            Self::Upsert { .. } => PayloadDomain::CredentialUpsert,
            Self::Tombstone { .. } => PayloadDomain::CredentialTombstone,
        }
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct EventEnvelope {
    schema: u16,
    sequence: u64,
    event_id: [u8; 16],
    previous_hash: [u8; 32],
    signer: [u8; 32],
    payload_domain: PayloadDomain,
    nonce: [u8; 24],
    #[serde(with = "serde_bytes")]
    ciphertext: Vec<u8>,
    #[serde(with = "serde_bytes")]
    signature: Vec<u8>,
}

impl EventEnvelope {
    fn validate(
        &self,
        expected_sequence: u64,
        expected_previous_hash: [u8; 32],
        local_signer: &VerifyingKey,
    ) -> Result<()> {
        if self.schema != EVENT_SCHEMA {
            bail!("unsupported event schema {}", self.schema);
        }
        if self.sequence != expected_sequence || self.previous_hash != expected_previous_hash {
            bail!("event sequence or hash chain is discontinuous");
        }
        if self.signer != local_signer.to_bytes() {
            bail!("event is not signed by this local auc installation");
        }
        let signature: [u8; 64] = self
            .signature
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("event signature has the wrong length"))?;
        local_signer
            .verify(
                &self.signature_message(),
                &Signature::from_bytes(&signature),
            )
            .map_err(|_| anyhow!("event Ed25519 signature is invalid"))
    }

    fn associated_data(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(EVENT_DOMAIN.len() + 2 + 8 + 16 + 32 + 32 + 1);
        bytes.extend_from_slice(EVENT_DOMAIN);
        bytes.extend_from_slice(&self.schema.to_be_bytes());
        bytes.extend_from_slice(&self.sequence.to_be_bytes());
        bytes.extend_from_slice(&self.event_id);
        bytes.extend_from_slice(&self.previous_hash);
        bytes.extend_from_slice(&self.signer);
        bytes.push(self.payload_domain.as_byte());
        bytes
    }

    fn signature_message(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(
            EVENT_SIGNATURE_DOMAIN.len()
                + self.associated_data().len()
                + 24
                + 8
                + self.ciphertext.len(),
        );
        bytes.extend_from_slice(EVENT_SIGNATURE_DOMAIN);
        bytes.extend_from_slice(&self.associated_data());
        bytes.extend_from_slice(&self.nonce);
        bytes.extend_from_slice(&(self.ciphertext.len() as u64).to_be_bytes());
        bytes.extend_from_slice(&self.ciphertext);
        bytes
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PinEnvelope {
    schema: u16,
    version: u64,
    nonce: [u8; 24],
    #[serde(with = "serde_bytes")]
    ciphertext: Vec<u8>,
}

fn load_or_create_identity(root: &Path, uid: u32, gid: u32) -> Result<IdentitySecrets> {
    let path = root.join(IDENTITY_FILE);
    let identity = Zeroizing::new(
        match read_private_file(&path, uid, gid, MAX_IDENTITY_BYTES) {
            Ok(bytes) => {
                let bytes = Zeroizing::new(bytes);
                ciborium::from_reader(bytes.as_slice())
                    .context("failed to decode auc vault identity")?
            }
            Err(error)
                if error
                    .downcast_ref::<std::io::Error>()
                    .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
            {
                let mut vault_key = Zeroizing::new([0_u8; 32]);
                let mut signing_key = Zeroizing::new([0_u8; 32]);
                let mut device_id = [0_u8; 16];
                rand::rng().fill(&mut vault_key[..]);
                rand::rng().fill(&mut signing_key[..]);
                rand::rng().fill(&mut device_id);
                let identity = IdentityFile {
                    schema: IDENTITY_SCHEMA,
                    vault_key: *vault_key,
                    signing_key: *signing_key,
                    device_id,
                };
                let mut bytes = Zeroizing::new(Vec::new());
                ciborium::into_writer(&identity, &mut *bytes)?;
                persist_new_private_file(&path, &bytes, ".auc-identity-", uid, gid)?;
                identity
            }
            Err(error) => return Err(error),
        },
    );
    if identity.schema != IDENTITY_SCHEMA {
        bail!("unsupported auc identity schema {}", identity.schema);
    }
    Ok(IdentitySecrets {
        vault_key: Zeroizing::new(identity.vault_key),
        signing_key: Zeroizing::new(identity.signing_key),
        device_id: identity.device_id,
    })
}

struct IdentitySecrets {
    vault_key: Zeroizing<[u8; 32]>,
    signing_key: Zeroizing<[u8; 32]>,
    device_id: [u8; 16],
}

struct LockedSecret {
    mapping: NonNull<libc::c_void>,
    length: usize,
}

impl LockedSecret {
    fn new(secret: Zeroizing<[u8; 32]>) -> Result<Self> {
        use rustix::mm::{Advice, MapFlags, ProtFlags, madvise, mlock, mmap_anonymous, munmap};

        let length = rustix::param::page_size();
        // SAFETY: this creates one private anonymous page. Every failure path unmaps it, and the
        // successful path gives sole ownership to LockedSecret until Drop.
        let mapping = unsafe {
            mmap_anonymous(
                std::ptr::null_mut(),
                length,
                ProtFlags::READ | ProtFlags::WRITE,
                MapFlags::PRIVATE,
            )
        }
        .map_err(std::io::Error::from)
        .context("failed to allocate locked auc secret memory")?;
        let mapping = NonNull::new(mapping)
            .ok_or_else(|| anyhow!("anonymous secret mapping unexpectedly returned null"))?;
        // SAFETY: mapping owns a writable page of `length` bytes for the full operation.
        if let Err(error) = unsafe { mlock(mapping.as_ptr(), length) } {
            // SAFETY: no reference or value has yet been created in this mapping.
            let _ = unsafe { munmap(mapping.as_ptr(), length) };
            return Err(std::io::Error::from(error))
                .context("failed to lock auc vault secrets into RAM");
        }
        // SAFETY: the mapping is page-aligned, private, and remains valid for `length` bytes.
        if let Err(error) = unsafe { madvise(mapping.as_ptr(), length, Advice::LinuxDontDump) } {
            // SAFETY: the page contains no initialized secret yet.
            let _ = unsafe { rustix::mm::munlock(mapping.as_ptr(), length) };
            // SAFETY: no reference or initialized value remains in this mapping.
            let _ = unsafe { munmap(mapping.as_ptr(), length) };
            return Err(std::io::Error::from(error))
                .context("failed to exclude auc vault secrets from core dumps");
        }
        // SAFETY: the mapping is writable and at least one page, while the source is a live
        // 32-byte array. The regions cannot overlap.
        unsafe {
            std::ptr::copy_nonoverlapping(secret.as_ptr(), mapping.as_ptr().cast(), secret.len());
        }
        Ok(Self { mapping, length })
    }
}

impl Deref for LockedSecret {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        // SAFETY: LockedSecret initialized the first 32 bytes and keeps the mapping readable and
        // uniquely owned until Drop. No method mutates it after construction.
        unsafe { &*self.mapping.as_ptr().cast() }
    }
}

// SAFETY: LockedSecret owns an independent immutable mapping; shared access exposes only &[u8; 32].
unsafe impl Send for LockedSecret {}
// SAFETY: LockedSecret owns an independent immutable mapping; shared access exposes only &[u8; 32].
unsafe impl Sync for LockedSecret {}

impl Drop for LockedSecret {
    fn drop(&mut self) {
        // SAFETY: this is the final access to our uniquely owned writable mapping. Volatile writes
        // plus the compiler fence prevent removal or reordering of the erasure before munlock.
        unsafe {
            for index in 0..32 {
                std::ptr::write_volatile(self.mapping.as_ptr().cast::<u8>().add(index), 0);
            }
        }
        compiler_fence(Ordering::SeqCst);
        // SAFETY: all references are gone and both calls use the original mapping and length.
        let _ = unsafe { rustix::mm::munlock(self.mapping.as_ptr(), self.length) };
        // SAFETY: the mapping is no longer accessed after this call.
        let _ = unsafe { rustix::mm::munmap(self.mapping.as_ptr(), self.length) };
    }
}

fn validate_credential(credential: &Credential) -> Result<()> {
    validate_credential_id(&credential.id)?;
    if credential.rp.id.is_empty() || credential.rp.id.len() > 253 {
        bail!("credential RP ID is invalid");
    }
    if credential.user.id.is_empty() || credential.user.id.len() > 64 {
        bail!("credential user ID is invalid");
    }
    if credential.sign_count != 0
        || credential.alg != -7
        || credential.backup_state != CredentialBackupState::Eligible
    {
        bail!("credential violates auc counter, algorithm, or backup-state policy");
    }
    Ok(())
}

fn validate_credential_id(credential_id: &[u8]) -> Result<()> {
    if credential_id.is_empty() || credential_id.len() > 128 {
        bail!("credential ID length is invalid");
    }
    Ok(())
}

fn event_filename(sequence: u64, event_id: &[u8; 16], event_hash: &[u8; 32]) -> String {
    format!(
        "{sequence:020}-{}-{}.cbor",
        hex::encode(event_id),
        hex::encode(event_hash)
    )
}

fn pin_associated_data(version: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(PIN_DOMAIN.len() + 2 + 8);
    bytes.extend_from_slice(PIN_DOMAIN);
    bytes.extend_from_slice(&PIN_SCHEMA.to_be_bytes());
    bytes.extend_from_slice(&version.to_be_bytes());
    bytes
}

fn ensure_secure_directory(path: &Path, uid: u32, gid: u32) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => validate_private_directory(path, &metadata, uid, gid)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let parent = path
                .parent()
                .ok_or_else(|| anyhow!("auc vault directory has no parent"))?;
            let parent_metadata = fs::symlink_metadata(parent)?;
            validate_directory(parent, &parent_metadata, uid, gid)?;
            fs::DirBuilder::new().mode(0o700).create(path)?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
            sync_directory(parent)?;
        }
        Err(error) => return Err(error.into()),
    }
    validate_private_directory(path, &fs::symlink_metadata(path)?, uid, gid)
}

fn validate_directory(path: &Path, metadata: &fs::Metadata, uid: u32, gid: u32) -> Result<()> {
    if !metadata.file_type().is_dir() || metadata.uid() != uid || metadata.gid() != gid {
        bail!(
            "auc vault path is not a correctly owned real directory: {}",
            path.display()
        );
    }
    Ok(())
}

fn validate_private_directory(
    path: &Path,
    metadata: &fs::Metadata,
    uid: u32,
    gid: u32,
) -> Result<()> {
    validate_directory(path, metadata, uid, gid)?;
    if metadata.mode() & 0o7777 != 0o700 {
        bail!("auc vault directory mode is not 0700: {}", path.display());
    }
    Ok(())
}

fn read_private_file(path: &Path, uid: u32, gid: u32, limit: u64) -> Result<Vec<u8>> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_CLOEXEC);
    let file = options.open(path)?;
    let metadata = file.metadata()?;
    if !metadata.is_file()
        || metadata.uid() != uid
        || metadata.gid() != gid
        || metadata.mode() & 0o7777 != 0o600
        || metadata.len() > limit
    {
        bail!(
            "auc private file failed ownership, mode, type, or size validation: {}",
            path.display()
        );
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    use std::io::Read as _;
    file.take(limit + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > limit {
        bail!(
            "auc private file exceeds its size limit: {}",
            path.display()
        );
    }
    Ok(bytes)
}

fn persist_new_private_file(
    destination: &Path,
    bytes: &[u8],
    prefix: &str,
    uid: u32,
    gid: u32,
) -> Result<()> {
    let parent = destination
        .parent()
        .ok_or_else(|| anyhow!("auc private file has no parent"))?;
    validate_private_directory(parent, &fs::symlink_metadata(parent)?, uid, gid)?;
    let mut temporary = Builder::new().prefix(prefix).tempfile_in(parent)?;
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary
        .persist_noclobber(destination)
        .map_err(|error| error.error)?;
    sync_directory(parent)
}

fn replace_private_file(
    destination: &Path,
    bytes: &[u8],
    prefix: &str,
    uid: u32,
    gid: u32,
) -> Result<()> {
    let parent = destination
        .parent()
        .ok_or_else(|| anyhow!("auc private file has no parent"))?;
    validate_private_directory(parent, &fs::symlink_metadata(parent)?, uid, gid)?;
    if destination.exists() {
        read_private_file(destination, uid, gid, MAX_PIN_BYTES)?;
    }
    let mut temporary = Builder::new().prefix(prefix).tempfile_in(parent)?;
    temporary
        .as_file()
        .set_permissions(fs::Permissions::from_mode(0o600))?;
    temporary.write_all(bytes)?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(destination)
        .map_err(|error| error.error)?;
    sync_directory(parent)
}

fn remove_uncommitted_files(path: &Path, prefix: &str, uid: u32, gid: u32) -> Result<()> {
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let name = entry.file_name();
        if name.as_bytes().starts_with(prefix.as_bytes()) {
            let metadata = fs::symlink_metadata(entry.path())?;
            if !metadata.is_file()
                || metadata.uid() != uid
                || metadata.gid() != gid
                || metadata.mode() & 0o7777 != 0o600
            {
                bail!(
                    "unsafe uncommitted file in auc vault: {}",
                    entry.path().display()
                );
            }
            fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all().map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;
    use soft_fido2::{Extensions, RelyingParty, User};

    fn credential(id: u8) -> Credential {
        use soft_fido2::SoftwareCredentialKeyProvider;

        let provider = SoftwareCredentialKeyProvider;
        let generated = soft_fido2::CredentialKeyProvider::generate(&provider, -7).unwrap();
        Credential {
            id: vec![id; 32],
            rp: RelyingParty::new("example.test".to_string()),
            user: User::new(vec![id]),
            sign_count: 0,
            alg: -7,
            key: generated.key,
            created: 1,
            discoverable: true,
            backup_state: CredentialBackupState::Eligible,
            extensions: Extensions::default(),
        }
    }

    fn append_credential(vault: &Vault, credential: &Credential) {
        vault
            .lock()
            .unwrap()
            .append(EventPayload::Upsert {
                credential: credential.to_bytes().unwrap(),
            })
            .unwrap();
    }

    #[test]
    fn event_store_round_trips_and_tombstones_are_permanent() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("vault");
        let vault = Vault::open_for_test(&root).unwrap();
        append_credential(&vault, &credential(1));
        assert_eq!(vault.credential_count().unwrap(), 1);
        assert!(vault.delete_credential(&[1; 32]).unwrap());
        assert_eq!(vault.credential_count().unwrap(), 0);
        drop(vault);

        let vault = Vault::open_for_test(&root).unwrap();
        assert_eq!(vault.credential_count().unwrap(), 0);
        let error = vault
            .lock()
            .unwrap()
            .append(EventPayload::Upsert {
                credential: credential(1).to_bytes().unwrap(),
            })
            .unwrap_err();
        assert!(error.to_string().contains("tombstone"));
    }

    #[test]
    fn tampered_event_fails_closed_with_its_filename() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("vault");
        let vault = Vault::open_for_test(&root).unwrap();
        append_credential(&vault, &credential(2));
        drop(vault);
        let event = fs::read_dir(root.join(EVENTS_DIRECTORY))
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path();
        let mut bytes = fs::read(&event).unwrap();
        let index = bytes.len() - 1;
        bytes[index] ^= 1;
        fs::write(&event, bytes).unwrap();

        let error = Vault::open_for_test(&root).err().unwrap();
        assert!(
            error
                .to_string()
                .contains(event.file_name().unwrap().to_str().unwrap())
        );
    }

    #[test]
    fn pin_state_survives_restart_and_cannot_roll_back() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("vault");
        let vault = Vault::open_for_test(&root).unwrap();
        let mut state = PinState {
            retries: 3,
            uv_retries: 1,
            version: 7,
            force_pin_change: true,
            ..PinState::default()
        };
        vault.lock().unwrap().save_pin_state(&state).unwrap();
        drop(vault);

        let vault = Vault::open_for_test(&root).unwrap();
        let loaded = vault.lock().unwrap().load_pin_state().unwrap();
        assert_eq!(loaded.retries, 3);
        assert_eq!(loaded.uv_retries, 1);
        assert_eq!(loaded.version, 7);
        assert!(loaded.force_pin_change);

        state.version = 6;
        let error = vault.lock().unwrap().save_pin_state(&state).unwrap_err();
        assert!(error.to_string().contains("backward"));
    }

    #[test]
    fn tampered_pin_state_fails_closed() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("vault");
        let vault = Vault::open_for_test(&root).unwrap();
        let state = PinState {
            version: 1,
            ..PinState::default()
        };
        vault.lock().unwrap().save_pin_state(&state).unwrap();
        drop(vault);

        let path = root.join(PIN_STATE_FILE);
        let mut bytes = fs::read(&path).unwrap();
        let index = bytes.len() - 1;
        bytes[index] ^= 1;
        fs::write(path, bytes).unwrap();

        let error = Vault::open_for_test(&root).err().unwrap();
        assert!(format!("{error:#}").contains("PIN state authentication failed"));
    }

    #[test]
    fn insecure_vault_modes_fail_closed_without_being_repaired() {
        let directory = tempfile::tempdir().unwrap();
        let root = directory.path().join("vault");
        drop(Vault::open_for_test(&root).unwrap());

        fs::set_permissions(&root, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(Vault::open_for_test(&root).is_err());
        assert_eq!(fs::symlink_metadata(&root).unwrap().mode() & 0o7777, 0o755);

        fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        let identity = root.join(IDENTITY_FILE);
        fs::set_permissions(&identity, fs::Permissions::from_mode(0o644)).unwrap();
        assert!(Vault::open_for_test(&root).is_err());
    }
}
