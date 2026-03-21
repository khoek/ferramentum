use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use serde::{Deserialize, Serialize};

#[derive(Debug, Parser)]
#[command(
    name = "ocular",
    version,
    about = "SSO helper for OpenConnect (AnyConnect)"
)]
pub struct Args {
    #[arg(long, help = "VPN server address (e.g. vpn.example.com/group)")]
    pub server: Option<String>,

    #[arg(long, help = "Force interactive prompts", default_value_t = false)]
    pub interactive: bool,

    #[arg(
        long,
        help = "Proxy server (http:// or socks5://) used for auth and OpenConnect"
    )]
    pub proxy: Option<String>,

    #[arg(
        long,
        default_value = "",
        help = "Override usergroup (path) from --server"
    )]
    pub usergroup: String,

    #[arg(long, default_value = "", help = "Authentication group selection")]
    pub authgroup: String,

    #[arg(
        long,
        value_enum,
        num_args = 0..=1,
        default_missing_value = "shell",
        help = "Authenticate only and print connection details"
    )]
    pub authenticate: Option<AuthenticateOutputFormat>,

    #[arg(
        long,
        default_value = "4.7.00136",
        help = "AnyConnect version used during authentication and passed to openconnect"
    )]
    pub ac_version: String,

    #[arg(long, value_enum, default_value_t = LogLevel::Info)]
    pub log_level: LogLevel,

    #[arg(
        long,
        value_enum,
        default_value_t = RoutesMode::Add,
        help = "How vpnc-script installs routes: add preserves existing routes, replace matches legacy behavior"
    )]
    pub routes: RoutesMode,

    #[arg(
        long = "only-tunnel",
        value_delimiter = ',',
        value_parser = parse_tunnel_route_target,
        help = "Route only these IPs, CIDRs, or domain names through the tunnel; repeat or comma-separate"
    )]
    pub tunnel_routes: Vec<String>,

    #[arg(long, help = "Path to a Chrome/Chromium executable")]
    pub chrome_path: Option<PathBuf>,

    #[arg(
        long,
        value_parser = parse_timeout_seconds,
        default_value = "600",
        help = "Browser auth timeout (seconds)"
    )]
    pub browser_timeout: Duration,

    #[arg(long, default_value = "", help = "Command to run after disconnecting")]
    pub on_disconnect: String,

    #[arg(
        trailing_var_arg = true,
        help = "Arguments passed to openconnect (after --)"
    )]
    pub openconnect_args: Vec<String>,

    #[arg(long, hide = true)]
    pub internal_openconnect_payload: Option<PathBuf>,
}

fn parse_timeout_seconds(s: &str) -> Result<Duration, String> {
    let secs: u64 = s
        .parse()
        .map_err(|_| "timeout must be an integer number of seconds".to_string())?;
    Ok(Duration::from_secs(secs))
}

fn parse_tunnel_route_target(s: &str) -> Result<String, String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err("only-tunnel target cannot be empty".to_string());
    }
    Ok(trimmed.to_string())
}

pub fn normalize_tunnel_route_targets(values: &[String]) -> Result<Vec<String>, String> {
    let mut normalized = Vec::new();
    for value in values {
        for target in value.split(',') {
            let target = parse_tunnel_route_target(target)?;
            if !normalized.contains(&target) {
                normalized.push(target);
            }
        }
    }
    Ok(normalized)
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum AuthenticateOutputFormat {
    Shell,
    Json,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RoutesMode {
    #[default]
    Add,
    Replace,
}

#[derive(Debug, Serialize)]
pub struct AuthDetails {
    pub host: String,
    pub cookie: String,
    pub fingerprint: String,
}

#[cfg(test)]
mod tests {
    #[test]
    fn normalize_tunnel_route_targets_splits_commas_and_deduplicates() {
        let normalized = super::normalize_tunnel_route_targets(&[
            " 140.247.39.160 ".to_string(),
            "example.com,10.0.0.0/8".to_string(),
            "example.com".to_string(),
        ])
        .expect("normalize tunnel routes");

        assert_eq!(
            normalized,
            vec![
                "140.247.39.160".to_string(),
                "example.com".to_string(),
                "10.0.0.0/8".to_string(),
            ]
        );
    }
}
