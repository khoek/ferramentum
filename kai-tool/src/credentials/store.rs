use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use tempfile::NamedTempFile;

use super::auth::{Credential, profile_id, validate_email};
use super::paths::RuntimePaths;

const STATE_VERSION: u32 = 1;
const MAX_STATE_BYTES: u64 = 1024 * 1024;
const DELETE_PREFIX: &str = ".deleting-";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Profile {
    pub id: String,
    pub email: String,
    pub account_id: String,
    pub enrolled_at: u64,
}

impl Profile {
    pub fn from_credential(credential: &Credential) -> Self {
        Self {
            id: profile_id(&credential.facts.email),
            email: credential.facts.email.clone(),
            account_id: credential.facts.account_id.clone(),
            enrolled_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
    version: u32,
    codex_home: Option<PathBuf>,
    profiles: Vec<Profile>,
}

impl Default for State {
    fn default() -> Self {
        Self {
            version: STATE_VERSION,
            codex_home: None,
            profiles: Vec::new(),
        }
    }
}

pub struct Store {
    paths: RuntimePaths,
    state: State,
    primary_codex_home: bool,
}

impl Store {
    pub fn open(paths: RuntimePaths) -> Result<Self> {
        let managed_codex_home = paths.codex_home.clone();
        Self::open_for_home(paths, &managed_codex_home, true)
    }

    pub fn open_session(paths: RuntimePaths, managed_codex_home: &Path) -> Result<Self> {
        Self::open_for_home(paths, managed_codex_home, false)
    }

    fn open_for_home(
        paths: RuntimePaths,
        managed_codex_home: &Path,
        primary_codex_home: bool,
    ) -> Result<Self> {
        ensure_private_directory(&paths.credentials_home)?;
        ensure_private_directory(&paths.profiles_dir())?;

        let state = load_state(&paths.state_file())?;
        validate_state(&state)?;
        if let Some(managed_home) = &state.codex_home
            && !state.profiles.is_empty()
            && managed_home != managed_codex_home
        {
            bail!(
                concat!(
                    "this Kai credential vault manages CODEX_HOME {}, but the current CODEX_HOME ",
                    "is {}; use the original CODEX_HOME or a separate KAI_CREDENTIALS_HOME",
                ),
                managed_home.display(),
                managed_codex_home.display()
            );
        }

        let mut store = Self {
            paths,
            state,
            primary_codex_home,
        };
        store.recover_pending_deletions()?;
        Ok(store)
    }

    pub fn paths(&self) -> &RuntimePaths {
        &self.paths
    }

    pub fn uses_primary_codex_home(&self) -> bool {
        self.primary_codex_home
    }

    pub fn profiles(&self) -> &[Profile] {
        &self.state.profiles
    }

    pub fn find_profile(&self, email: &str) -> Option<&Profile> {
        self.state
            .profiles
            .iter()
            .find(|profile| profile.email.eq_ignore_ascii_case(email.trim()))
    }

    pub fn find_profile_by_account(&self, account_id: &str) -> Option<&Profile> {
        self.state
            .profiles
            .iter()
            .find(|profile| profile.account_id == account_id)
    }

    pub fn credential(&self, profile: &Profile) -> Result<Credential> {
        let credential = Credential::read(&self.profile_path(&profile.id))?;
        if credential.facts.account_id != profile.account_id
            || !credential.facts.email.eq_ignore_ascii_case(&profile.email)
        {
            bail!(
                "stored credential identity does not match profile metadata for {}",
                profile.email
            );
        }
        Ok(credential)
    }

    pub fn sync_profile(&self, profile: &Profile, credential: &Credential) -> Result<()> {
        if credential.facts.account_id != profile.account_id
            || !credential.facts.email.eq_ignore_ascii_case(&profile.email)
        {
            bail!(
                "refusing to overwrite profile {} with credentials for {}",
                profile.email,
                credential.facts.email
            );
        }
        atomic_write(&self.profile_path(&profile.id), credential.as_bytes())
            .with_context(|| format!("could not update credential for {}", profile.email))
    }

    pub fn insert_profile(&mut self, credential: &Credential) -> Result<Profile> {
        if self.find_profile(&credential.facts.email).is_some() {
            bail!("{} is already enrolled", credential.facts.email);
        }
        if let Some(profile) = self.find_profile_by_account(&credential.facts.account_id) {
            bail!(
                "this Codex account is already enrolled as {}",
                profile.email
            );
        }

        let profile = Profile::from_credential(credential);
        let path = self.profile_path(&profile.id);
        if path.exists() {
            bail!(
                "profile credential {} already exists but is not present in Kai credential state",
                path.display()
            );
        }
        atomic_write(&path, credential.as_bytes())?;

        self.state.profiles.push(profile.clone());
        self.state.codex_home = Some(self.paths.codex_home.clone());
        if let Err(err) = self.save_state() {
            let _ = fs::remove_file(&path);
            self.state.profiles.pop();
            return Err(err);
        }
        Ok(profile)
    }

    pub fn remove_profile(&mut self, profile_id: &str) -> Result<Profile> {
        let index = self
            .state
            .profiles
            .iter()
            .position(|profile| profile.id == profile_id)
            .context("profile disappeared while removing it")?;
        let profile = self.state.profiles[index].clone();
        let path = self.profile_path(profile_id);
        let pending = self
            .paths
            .profiles_dir()
            .join(format!("{DELETE_PREFIX}{profile_id}"));
        reject_symlink_if_present(&path)?;
        fs::rename(&path, &pending)
            .with_context(|| format!("could not stage credential removal for {}", profile.email))?;

        self.state.profiles.remove(index);
        if self.state.profiles.is_empty() {
            self.state.codex_home = None;
        }
        if let Err(err) = self.save_state() {
            self.state.profiles.insert(index, profile.clone());
            self.state.codex_home = Some(self.paths.codex_home.clone());
            let _ = fs::rename(&pending, &path);
            return Err(err);
        }

        fs::remove_file(&pending).with_context(|| {
            format!("could not finish removing credential for {}", profile.email)
        })?;
        sync_directory(&self.paths.profiles_dir())?;
        Ok(profile)
    }

    pub fn write_active(&self, credential: &Credential) -> Result<()> {
        ensure_private_directory(&self.paths.codex_home)?;
        reject_symlink_if_present(&self.paths.active_auth())?;
        atomic_write(&self.paths.active_auth(), credential.as_bytes())
            .context("could not activate Codex credential")
    }

    pub fn remove_active(&self) -> Result<()> {
        let path = self.paths.active_auth();
        match fs::symlink_metadata(&path) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                bail!("refusing to remove credential symlink {}", path.display())
            }
            Ok(metadata) if metadata.is_file() => {
                fs::remove_file(&path).with_context(|| {
                    format!("could not remove active credential {}", path.display())
                })?;
                path.parent()
                    .context("active credential path has no parent")
                    .and_then(sync_directory)
            }
            Ok(_) => bail!(
                "active credential is not a regular file: {}",
                path.display()
            ),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(err)
                .with_context(|| format!("could not inspect active credential {}", path.display())),
        }
    }

    fn profile_path(&self, profile_id: &str) -> PathBuf {
        self.paths.profiles_dir().join(format!("{profile_id}.json"))
    }

    fn save_state(&self) -> Result<()> {
        validate_state(&self.state)?;
        let mut encoded = serde_json::to_vec_pretty(&self.state)?;
        encoded.push(b'\n');
        atomic_write(&self.paths.state_file(), &encoded)
            .context("could not save Kai credential state")
    }

    fn recover_pending_deletions(&mut self) -> Result<()> {
        let known_ids = self
            .state
            .profiles
            .iter()
            .map(|profile| profile.id.as_str())
            .collect::<HashSet<_>>();
        for entry in fs::read_dir(self.paths.profiles_dir())? {
            let entry = entry?;
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(profile_id) = name.strip_prefix(DELETE_PREFIX) else {
                continue;
            };
            reject_symlink_if_present(&entry.path())?;
            if known_ids.contains(profile_id) {
                let destination = self.profile_path(profile_id);
                if destination.exists() {
                    bail!(
                        "both live and pending-delete credentials exist for profile {profile_id}"
                    );
                }
                fs::rename(entry.path(), destination)?;
            } else {
                fs::remove_file(entry.path())?;
            }
        }
        Ok(())
    }
}

pub fn ensure_codex_uses_file_credentials(paths: &RuntimePaths) -> Result<()> {
    let config_path = paths.codex_config();
    let Some(config) = paths.read_codex_config()? else {
        return Ok(());
    };
    match config
        .get("cli_auth_credentials_store")
        .and_then(toml::Value::as_str)
    {
        None | Some("file") => Ok(()),
        Some(mode @ ("keyring" | "auto")) => bail!(
            concat!(
                "Codex is configured with `cli_auth_credentials_store = \"{}\"`; ",
                "Kai credential rotation requires file-backed credentials. Set it to `file` in {}",
            ),
            mode,
            config_path.display()
        ),
        Some(mode) => bail!(
            "unsupported Codex `cli_auth_credentials_store` value `{mode}` in {}",
            config_path.display()
        ),
    }
}

pub fn atomic_write(path: &Path, contents: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .with_context(|| format!("path has no parent: {}", path.display()))?;
    let mut temporary = NamedTempFile::new_in(parent)
        .with_context(|| format!("could not create temporary file in {}", parent.display()))?;
    set_private_file_permissions(temporary.as_file())?;
    temporary.write_all(contents)?;
    temporary.as_file_mut().flush()?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(path)
        .map_err(|err| err.error)
        .with_context(|| format!("could not atomically replace {}", path.display()))?;
    sync_directory(parent)
}

fn load_state(path: &Path) -> Result<State> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(State::default()),
        Err(err) => {
            return Err(err).with_context(|| format!("could not inspect {}", path.display()));
        }
    };
    if metadata.file_type().is_symlink() {
        bail!(
            "refusing to read Kai credential state through symlink {}",
            path.display()
        );
    }
    if !metadata.is_file() {
        bail!(
            "Kai credential state is not a regular file: {}",
            path.display()
        );
    }
    if metadata.len() > MAX_STATE_BYTES {
        bail!(
            "Kai credential state file is unexpectedly large: {}",
            path.display()
        );
    }
    let file = File::open(path)?;
    let mut contents = Vec::with_capacity(metadata.len() as usize);
    set_private_file_permissions(&file)?;
    file.take(MAX_STATE_BYTES + 1).read_to_end(&mut contents)?;
    serde_json::from_slice(&contents)
        .with_context(|| format!("could not parse Kai credential state {}", path.display()))
}

fn validate_state(state: &State) -> Result<()> {
    if state.version != STATE_VERSION {
        bail!(
            "unsupported Kai credential state version {} (this build supports {})",
            state.version,
            STATE_VERSION
        );
    }
    let mut ids = HashSet::new();
    let mut emails = HashSet::new();
    let mut accounts = HashSet::new();
    for profile in &state.profiles {
        validate_email(&profile.email)?;
        if profile.id != profile_id(&profile.email) {
            bail!("invalid profile ID for {}", profile.email);
        }
        if !ids.insert(profile.id.as_str())
            || !emails.insert(profile.email.to_ascii_lowercase())
            || !accounts.insert(profile.account_id.as_str())
        {
            bail!("Kai credential state contains duplicate profile identities");
        }
    }
    if !state.profiles.is_empty() && state.codex_home.is_none() {
        bail!("Kai credential state has profiles but no managed CODEX_HOME");
    }
    Ok(())
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!(
                "refusing to use credential directory symlink {}",
                path.display()
            )
        }
        Ok(metadata) if !metadata.is_dir() => {
            bail!("credential path is not a directory: {}", path.display())
        }
        Ok(_) => {}
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path)
                .with_context(|| format!("could not create directory {}", path.display()))?;
        }
        Err(err) => {
            return Err(err).with_context(|| format!("could not inspect {}", path.display()));
        }
    }
    #[cfg(unix)]
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    Ok(())
}

fn reject_symlink_if_present(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            bail!("refusing to replace symlink {}", path.display())
        }
        Ok(metadata) if !metadata.is_file() => {
            bail!("path is not a regular file: {}", path.display())
        }
        Ok(_) => Ok(()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err).with_context(|| format!("could not inspect {}", path.display())),
    }
}

fn set_private_file_permissions(file: &File) -> Result<()> {
    #[cfg(unix)]
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    #[cfg(unix)]
    File::open(path)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::super::auth::tests::auth_json;
    use super::*;

    fn paths(root: &Path) -> RuntimePaths {
        RuntimePaths::new(root.join("credentials"), root.join("codex")).unwrap()
    }

    #[test]
    fn stores_credentials_privately_and_round_trips_state() {
        let root = tempdir().unwrap();
        let paths = paths(root.path());
        let credential = Credential::from_bytes(auth_json(
            "ada@example.com",
            "account-1",
            "pro",
            2_000_000_000,
            "refresh-1",
        ))
        .unwrap();
        {
            let mut store = Store::open(paths.clone()).unwrap();
            store.insert_profile(&credential).unwrap();
        }

        let store = Store::open(paths.clone()).unwrap();
        assert_eq!(store.profiles().len(), 1);
        assert_eq!(
            store.credential(&store.profiles()[0]).unwrap().facts,
            credential.facts
        );
        #[cfg(unix)]
        {
            assert_eq!(
                fs::metadata(paths.state_file())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
            assert_eq!(
                fs::metadata(paths.profiles_dir())
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
    }

    #[test]
    fn rejects_keyring_backed_codex_configuration() {
        let root = tempdir().unwrap();
        let paths = paths(root.path());
        fs::create_dir_all(&paths.codex_home).unwrap();
        fs::write(
            paths.codex_config(),
            "cli_auth_credentials_store = \"keyring\"\n",
        )
        .unwrap();

        let error = ensure_codex_uses_file_credentials(&paths).unwrap_err();
        assert!(error.to_string().contains("requires file-backed"));
    }
}
