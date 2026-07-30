use std::fs;
use std::path::Path;

use anyhow::{Context, Result, bail};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

const MAX_AUTH_BYTES: u64 = 1024 * 1024;
const OPENAI_AUTH_CLAIM: &str = "https://api.openai.com/auth";

#[derive(Debug)]
pub struct Credential {
    bytes: Zeroizing<Vec<u8>>,
    pub facts: CredentialFacts,
    account_is_fedramp: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CredentialFacts {
    pub email: String,
    pub account_id: String,
    pub plan: Option<String>,
    pub access_expires_at: Option<i64>,
    pub last_refresh: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AuthDocument<'a> {
    auth_mode: Option<&'a str>,
    tokens: Option<AuthTokens<'a>>,
    last_refresh: Option<&'a str>,
}

#[derive(Debug, Deserialize)]
struct AuthTokens<'a> {
    id_token: &'a str,
    access_token: &'a str,
    account_id: Option<&'a str>,
    refresh_token: &'a str,
}

impl Credential {
    pub fn read(path: &Path) -> Result<Self> {
        let metadata = fs::symlink_metadata(path)
            .with_context(|| format!("could not inspect credential file {}", path.display()))?;
        if metadata.file_type().is_symlink() {
            bail!(
                "refusing to read credential symlink {}; use a regular 0600 file",
                path.display()
            );
        }
        if !metadata.is_file() {
            bail!("credential path is not a regular file: {}", path.display());
        }
        if metadata.len() > MAX_AUTH_BYTES {
            bail!(
                "credential file {} is unexpectedly large ({} bytes)",
                path.display(),
                metadata.len()
            );
        }
        Self::from_bytes(
            fs::read(path)
                .with_context(|| format!("could not read credential file {}", path.display()))?,
        )
        .with_context(|| format!("invalid Codex credential in {}", path.display()))
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        if bytes.len() as u64 > MAX_AUTH_BYTES {
            bail!("credential document exceeds {MAX_AUTH_BYTES} bytes");
        }
        let bytes = Zeroizing::new(bytes);
        let (facts, account_is_fedramp) = {
            let document: AuthDocument<'_> =
                serde_json::from_slice(&bytes).context("credential document is not valid JSON")?;
            if document.auth_mode != Some("chatgpt") {
                bail!(
                    "Kai only manages Codex ChatGPT credentials with `auth_mode` set to `chatgpt`"
                );
            }
            let tokens = document
                .tokens
                .context("credential document has no ChatGPT token set")?;
            if tokens.refresh_token.trim().is_empty() {
                bail!("credential document has an empty refresh token");
            }

            let id_claims =
                jwt_claims(tokens.id_token).context("could not decode the Codex ID token")?;
            let email = id_claims
                .get("email")
                .and_then(Value::as_str)
                .filter(|email| !email.trim().is_empty())
                .context("Codex ID token has no email claim")?
                .trim()
                .to_owned();
            validate_email(&email)?;

            let openai_auth = id_claims.get(OPENAI_AUTH_CLAIM).and_then(Value::as_object);
            let account_id = tokens
                .account_id
                .filter(|id| !id.trim().is_empty())
                .or_else(|| {
                    openai_auth?
                        .get("chatgpt_account_id")
                        .and_then(Value::as_str)
                        .filter(|id| !id.trim().is_empty())
                })
                .context("Codex credential has no ChatGPT account ID")?
                .to_owned();
            let plan = openai_auth
                .and_then(|claims| claims.get("chatgpt_plan_type"))
                .and_then(Value::as_str)
                .filter(|plan| !plan.trim().is_empty())
                .map(str::to_owned);
            let account_is_fedramp = openai_auth
                .and_then(|claims| claims.get("chatgpt_account_is_fedramp"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let access_expires_at = jwt_claims(tokens.access_token)
                .ok()
                .and_then(|claims| claims.get("exp").and_then(Value::as_i64));

            (
                CredentialFacts {
                    email,
                    account_id,
                    plan,
                    access_expires_at,
                    last_refresh: document.last_refresh.map(str::to_owned),
                },
                account_is_fedramp,
            )
        };

        Ok(Self {
            bytes,
            facts,
            account_is_fedramp,
        })
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) fn access_token(&self) -> Result<Zeroizing<String>> {
        let document: AuthDocument<'_> =
            serde_json::from_slice(&self.bytes).context("credential document is not valid JSON")?;
        let token = document
            .tokens
            .context("credential document has no ChatGPT token set")?
            .access_token
            .trim();
        if token.is_empty() {
            bail!("credential document has an empty access token");
        }
        Ok(Zeroizing::new(token.to_owned()))
    }

    pub(super) fn account_is_fedramp(&self) -> bool {
        self.account_is_fedramp
    }

    pub fn matches_email(&self, email: &str) -> bool {
        self.facts.email.eq_ignore_ascii_case(email.trim())
    }
}

pub fn validate_email(email: &str) -> Result<()> {
    let trimmed = email.trim();
    let Some((local, domain)) = trimmed.split_once('@') else {
        bail!("`{trimmed}` is not an email address");
    };
    if local.is_empty()
        || domain.is_empty()
        || domain.contains('@')
        || trimmed.chars().any(char::is_whitespace)
    {
        bail!("`{trimmed}` is not an email address");
    }
    Ok(())
}

pub fn profile_id(email: &str) -> String {
    let digest = Sha256::digest(email.trim().to_ascii_lowercase().as_bytes());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn jwt_claims(token: &str) -> Result<Value> {
    let mut segments = token.split('.');
    let _header = segments.next().context("JWT has no header")?;
    let payload = segments.next().context("JWT has no payload")?;
    let _signature = segments.next().context("JWT has no signature")?;
    if segments.next().is_some() {
        bail!("JWT has too many segments");
    }
    let decoded = Zeroizing::new(
        URL_SAFE_NO_PAD
            .decode(payload)
            .context("JWT payload is not valid base64url")?,
    );
    serde_json::from_slice(&decoded).context("JWT payload is not valid JSON")
}

#[cfg(test)]
pub(crate) mod tests {
    use base64::Engine;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use serde_json::json;

    use super::*;

    pub(crate) fn auth_json(
        email: &str,
        account_id: &str,
        plan: &str,
        access_exp: i64,
        refresh_token: &str,
    ) -> Vec<u8> {
        let jwt = |claims: Value| {
            let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"none","typ":"JWT"}"#);
            let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&claims).unwrap());
            format!("{header}.{payload}.signature")
        };
        serde_json::to_vec_pretty(&json!({
            "auth_mode": "chatgpt",
            "tokens": {
                "id_token": jwt(json!({
                    "email": email,
                    "exp": access_exp + 3600,
                    OPENAI_AUTH_CLAIM: {
                        "chatgpt_account_id": account_id,
                        "chatgpt_plan_type": plan
                    }
                })),
                "access_token": jwt(json!({"exp": access_exp})),
                "account_id": account_id,
                "refresh_token": refresh_token
            },
            "last_refresh": "2026-07-29T00:00:00Z"
        }))
        .unwrap()
    }

    #[test]
    fn parses_chatgpt_credential_without_exposing_tokens() {
        let credential = Credential::from_bytes(auth_json(
            "Ada@Example.com",
            "account-1",
            "pro",
            2_000_000_000,
            "secret-refresh",
        ))
        .unwrap();

        assert_eq!(credential.facts.email, "Ada@Example.com");
        assert_eq!(credential.facts.account_id, "account-1");
        assert_eq!(credential.facts.plan.as_deref(), Some("pro"));
        assert_eq!(credential.facts.access_expires_at, Some(2_000_000_000));
    }

    #[test]
    fn rejects_non_chatgpt_credentials() {
        let error = Credential::from_bytes(br#"{"auth_mode":"apikey"}"#.to_vec()).unwrap_err();
        assert!(error.to_string().contains("only manages Codex ChatGPT"));
    }

    #[test]
    fn profile_ids_are_case_insensitive_and_path_safe() {
        assert_eq!(profile_id("Ada@Example.com"), profile_id("ada@example.com"));
        assert_eq!(profile_id("ada@example.com").len(), 64);
    }
}
