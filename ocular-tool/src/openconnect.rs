use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::net::{IpAddr, ToSocketAddrs};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use indicatif::ProgressBar;
use ipnet::{IpNet, Ipv4Net, Ipv6Net};
use openconnect_core::config::{ConfigBuilder, EntrypointBuilder, LogLevel as CoreLogLevel};
use openconnect_core::events::EventHandlers;
use openconnect_core::protocols::get_anyconnect_protocol;
use openconnect_core::result::OpenconnectError;
use openconnect_core::{Connectable, Status, VpnClient};
use serde::{Deserialize, Serialize};
use sha1::{Digest, Sha1};
use sha2::Sha256;
#[cfg(not(target_os = "windows"))]
use tempfile::{Builder as TempDirBuilder, TempDir};
#[cfg(not(target_os = "windows"))]
use which::which;

use crate::anyconnect::AuthComplete;
use crate::cli::{LogLevel, RoutesMode};
use crate::error::AppError;
use crate::shell;

const PRIVILEGED_HANDOFF_FILE_EXTENSION: &str = "toml";
#[cfg(not(target_os = "windows"))]
const VPNSCRIPT_HOOKS_DIR_MARKER: &str = "HOOKS_DIR=/etc/vpnc";

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenConnectResult {
    pub exit_code: i32,
    pub auth_failed: bool,
    pub expires_at_epoch: Option<i64>,
    pub expires_at_text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PrivilegedConnectPayload {
    host_url: String,
    session_token: String,
    server_cert_hash: String,
    proxy: Option<String>,
    version: String,
    args: Vec<String>,
    #[serde(default)]
    routes: RoutesMode,
    #[serde(default)]
    selective_routes: SelectiveRoutes,
    #[serde(default)]
    interactive: bool,
    result_path: PathBuf,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct SelectiveRoutes {
    #[serde(default)]
    ipv4: Vec<Ipv4Net>,
    #[serde(default)]
    ipv6: Vec<Ipv6Net>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PeerCertHashes {
    sha1: String,
    sha256: String,
}

impl SelectiveRoutes {
    fn resolve(raw_targets: &[String]) -> Result<Self, AppError> {
        let mut ipv4 = BTreeSet::new();
        let mut ipv6 = BTreeSet::new();

        for target in raw_targets {
            match resolve_tunnel_route_target(target)? {
                ResolvedTunnelRouteTarget::Ipv4(net) => {
                    ipv4.insert(net);
                }
                ResolvedTunnelRouteTarget::Ipv6(net) => {
                    ipv6.insert(net);
                }
                ResolvedTunnelRouteTarget::Mixed {
                    ipv4: nets4,
                    ipv6: nets6,
                } => {
                    ipv4.extend(nets4);
                    ipv6.extend(nets6);
                }
            }
        }

        Ok(Self {
            ipv4: ipv4.into_iter().collect(),
            ipv6: ipv6.into_iter().collect(),
        })
    }

    fn is_empty(&self) -> bool {
        self.ipv4.is_empty() && self.ipv6.is_empty()
    }
}

enum ResolvedTunnelRouteTarget {
    Ipv4(Ipv4Net),
    Ipv6(Ipv6Net),
    Mixed {
        ipv4: BTreeSet<Ipv4Net>,
        ipv6: BTreeSet<Ipv6Net>,
    },
}

fn resolve_tunnel_route_target(target: &str) -> Result<ResolvedTunnelRouteTarget, AppError> {
    if let Some(route) = parse_literal_tunnel_route(target)? {
        return Ok(route);
    }

    let mut ipv4 = BTreeSet::new();
    let mut ipv6 = BTreeSet::new();
    let resolved: Vec<IpAddr> = (target, 0)
        .to_socket_addrs()
        .map_err(|err| {
            AppError::Config(format!(
                "failed to resolve only-tunnel target '{target}': {err}"
            ))
        })?
        .map(|addr| addr.ip())
        .collect();

    if resolved.is_empty() {
        return Err(AppError::Config(format!(
            "only-tunnel target '{target}' resolved to no addresses"
        )));
    }

    for addr in resolved {
        insert_host_route(addr, &mut ipv4, &mut ipv6);
    }

    tracing::debug!(
        target,
        ipv4 = ?ipv4,
        ipv6 = ?ipv6,
        "Resolved selective tunnel route target"
    );

    Ok(ResolvedTunnelRouteTarget::Mixed { ipv4, ipv6 })
}

fn parse_literal_tunnel_route(target: &str) -> Result<Option<ResolvedTunnelRouteTarget>, AppError> {
    if target.contains('/') {
        return target
            .parse::<IpNet>()
            .map(truncate_resolved_route)
            .map(Some)
            .map_err(|err| {
                AppError::Config(format!("invalid only-tunnel target '{target}': {err}"))
            });
    }

    Ok(target.parse::<IpAddr>().ok().map(host_route))
}

fn truncate_resolved_route(net: IpNet) -> ResolvedTunnelRouteTarget {
    match net {
        IpNet::V4(net) => ResolvedTunnelRouteTarget::Ipv4(net.trunc()),
        IpNet::V6(net) => ResolvedTunnelRouteTarget::Ipv6(net.trunc()),
    }
}

fn host_route(addr: IpAddr) -> ResolvedTunnelRouteTarget {
    match addr {
        IpAddr::V4(addr) => ResolvedTunnelRouteTarget::Ipv4(
            Ipv4Net::new(addr, 32).expect("32-bit IPv4 host route should always be valid"),
        ),
        IpAddr::V6(addr) => ResolvedTunnelRouteTarget::Ipv6(
            Ipv6Net::new(addr, 128).expect("128-bit IPv6 host route should always be valid"),
        ),
    }
}

fn insert_host_route(addr: IpAddr, ipv4: &mut BTreeSet<Ipv4Net>, ipv6: &mut BTreeSet<Ipv6Net>) {
    match host_route(addr) {
        ResolvedTunnelRouteTarget::Ipv4(net) => {
            ipv4.insert(net);
        }
        ResolvedTunnelRouteTarget::Ipv6(net) => {
            ipv6.insert(net);
        }
        ResolvedTunnelRouteTarget::Mixed { .. } => {
            unreachable!("host route cannot resolve to multiple routes")
        }
    }
}

pub fn run_openconnect(
    host_url: &str,
    auth: &AuthComplete,
    proxy: Option<&str>,
    version: &str,
    args: &[String],
    on_disconnect: Option<&str>,
    interactive: bool,
    log_level: LogLevel,
    routes: RoutesMode,
    tunnel_routes: &[String],
) -> Result<OpenConnectResult, AppError> {
    let selective_routes = SelectiveRoutes::resolve(tunnel_routes)?;
    if !selective_routes.is_empty() {
        tracing::info!(
            ipv4_routes = selective_routes.ipv4.len(),
            ipv6_routes = selective_routes.ipv6.len(),
            "Using selective tunnel routes"
        );
        tracing::debug!(targets = ?tunnel_routes, ?selective_routes, "Resolved selective tunnel routes");
    }

    #[cfg(unix)]
    {
        if unsafe { libc::geteuid() } != 0 {
            return run_openconnect_via_elevated_child(
                host_url,
                auth,
                proxy,
                version,
                args,
                on_disconnect,
                interactive,
                log_level,
                routes,
                &selective_routes,
            );
        }
    }

    run_openconnect_local(
        host_url,
        auth,
        proxy,
        version,
        args,
        on_disconnect,
        interactive,
        log_level,
        routes,
        &selective_routes,
    )
}

pub fn run_privileged_payload(payload_path: &Path, log_level: LogLevel) -> Result<i32, AppError> {
    let payload = read_toml_file::<PrivilegedConnectPayload>(payload_path)?.ok_or_else(|| {
        AppError::Config("privileged connection payload file was empty".to_string())
    })?;
    cleanup_temp_file(payload_path);

    let auth = AuthComplete {
        auth_id: "cached".to_string(),
        auth_message: String::new(),
        session_token: payload.session_token,
        server_cert_hash: payload.server_cert_hash,
    };

    let result = run_openconnect_local(
        &payload.host_url,
        &auth,
        payload.proxy.as_deref(),
        &payload.version,
        &payload.args,
        None,
        payload.interactive,
        log_level,
        payload.routes,
        &payload.selective_routes,
    )?;

    if let Err(err) = write_toml_file(&payload.result_path, &result, 0o666) {
        tracing::warn!(
            file = %payload.result_path.display(),
            %err,
            "Failed to write privileged connect result; falling back to child exit code only"
        );
        return Ok(result.exit_code);
    }

    Ok(result.exit_code)
}

pub fn preauthorize_privileged_runner() -> Result<(), AppError> {
    #[cfg(unix)]
    {
        if unsafe { libc::geteuid() } == 0 {
            return Ok(());
        }

        let (program, prefix_args) = privileged_command_prefix()?;
        let status = match program.as_str() {
            "sudo" => {
                let mut cmd = Command::new("sudo");
                cmd.args(&prefix_args);
                cmd.arg("-v");
                cmd.status()?
            }
            "doas" => {
                let mut cmd = Command::new("doas");
                cmd.args(&prefix_args);
                cmd.arg("true");
                cmd.status()?
            }
            _ => return Ok(()),
        };

        if !status.success() {
            return Err(AppError::NeedRoot);
        }
    }

    Ok(())
}

fn run_openconnect_local(
    host_url: &str,
    auth: &AuthComplete,
    proxy: Option<&str>,
    _version: &str,
    args: &[String],
    on_disconnect: Option<&str>,
    interactive: bool,
    log_level: LogLevel,
    routes: RoutesMode,
    selective_routes: &SelectiveRoutes,
) -> Result<OpenConnectResult, AppError> {
    #[cfg(target_os = "windows")]
    if !selective_routes.is_empty() {
        return Err(AppError::Config(
            "selective tunnel routes are not supported on Windows yet".to_string(),
        ));
    }

    if !args.is_empty() {
        tracing::warn!(
            "--openconnect-args passthrough is not supported with libopenconnect; ignoring: {:?}",
            args
        );
    }

    tracing::info!(host = host_url, "OpenConnect connecting...");

    let mut builder = ConfigBuilder::default();
    builder.loglevel(to_core_log_level(log_level));
    if let Some(proxy) = proxy {
        builder.http_proxy(proxy);
    }
    let _vpnc_capture = configure_vpnc_script(&mut builder, routes, selective_routes)?;

    let config = builder
        .build()
        .map_err(|e| AppError::OpenConnectCore(e.to_string()))?;

    let expected_hashes = parse_expected_hashes(&auth.server_cert_hash);
    let lifecycle = Arc::new(Mutex::new(Lifecycle::new(interactive)));
    let client_slot: Arc<Mutex<Option<Arc<VpnClient>>>> = Arc::new(Mutex::new(None));

    let event_handlers = {
        let lifecycle_for_status = Arc::clone(&lifecycle);
        let expected_hashes_for_cert = expected_hashes.clone();
        let client_slot_for_cert = Arc::clone(&client_slot);
        EventHandlers::default()
            .with_handle_connection_state_change(move |status| {
                handle_status_event(&lifecycle_for_status, status)
            })
            .with_handle_peer_cert_invalid(move |actual_hash| {
                handle_invalid_cert(
                    &expected_hashes_for_cert,
                    &client_slot_for_cert,
                    actual_hash,
                )
            })
    };

    let client = VpnClient::new(config, event_handlers)
        .map_err(|e| AppError::OpenConnectCore(e.to_string()))?;
    if let Ok(mut slot) = client_slot.lock() {
        *slot = Some(Arc::clone(&client));
    }

    let mut entry = EntrypointBuilder::new();
    let entrypoint = entry
        .server(host_url)
        .protocol(get_anyconnect_protocol())
        .cookie(&auth.session_token)
        .enable_udp(true)
        .accept_insecure_cert(false)
        .build()
        .map_err(|e| AppError::OpenConnectCore(e.to_string()))?;

    if let Err(err) = client.init_connection(entrypoint) {
        let auth_failed = is_probably_auth_failure(&err);
        tracing::warn!(error = %err, "OpenConnect failed to initialize connection");
        if let Some(cmd) = on_disconnect.filter(|s| !s.trim().is_empty()) {
            handle_disconnect(cmd);
        }
        return Ok(OpenConnectResult {
            exit_code: 1,
            auth_failed,
            ..OpenConnectResult::default()
        });
    }

    if !expected_hashes.is_empty() && !cert_hash_matches_any(&client, &expected_hashes) {
        let actual_hash = client.get_peer_cert_hash();
        tracing::warn!(
            expected = ?expected_hashes,
            actual = actual_hash,
            "Connected certificate does not match pinned hash; disconnecting"
        );
        client.disconnect();
        if let Some(cmd) = on_disconnect.filter(|s| !s.trim().is_empty()) {
            handle_disconnect(cmd);
        }
        return Ok(OpenConnectResult {
            exit_code: 1,
            auth_failed: false,
            ..OpenConnectResult::default()
        });
    }

    let mut result = OpenConnectResult::default();
    if let Err(err) = client.run_loop() {
        tracing::warn!(error = %err, "OpenConnect main loop exited with error");
        result.exit_code = 1;
        result.auth_failed = is_probably_auth_failure(&err);
    }

    if let Ok(state) = lifecycle.lock()
        && result.exit_code == 0
    {
        if let Some(err) = state.last_error.as_ref() {
            result.exit_code = 1;
            result.auth_failed |= is_probably_auth_failure(err);
        } else if !state.connected_once {
            result.exit_code = 1;
        }
    }

    if let Some(cmd) = on_disconnect.filter(|s| !s.trim().is_empty()) {
        handle_disconnect(cmd);
    }

    Ok(result)
}

#[cfg(unix)]
fn run_openconnect_via_elevated_child(
    host_url: &str,
    auth: &AuthComplete,
    proxy: Option<&str>,
    version: &str,
    args: &[String],
    on_disconnect: Option<&str>,
    interactive: bool,
    log_level: LogLevel,
    routes: RoutesMode,
    selective_routes: &SelectiveRoutes,
) -> Result<OpenConnectResult, AppError> {
    let (program, prefix_args) = privileged_command_prefix()?;
    let payload_path =
        create_secure_temp_file("payload", PRIVILEGED_HANDOFF_FILE_EXTENSION, 0o600)?;
    let result_path = create_secure_temp_file("result", PRIVILEGED_HANDOFF_FILE_EXTENSION, 0o666)?;

    let payload = PrivilegedConnectPayload {
        host_url: host_url.to_string(),
        session_token: auth.session_token.clone(),
        server_cert_hash: auth.server_cert_hash.clone(),
        proxy: proxy.map(|s| s.to_string()),
        version: version.to_string(),
        args: args.to_vec(),
        routes,
        selective_routes: selective_routes.clone(),
        interactive,
        result_path: result_path.clone(),
    };
    write_toml_file(&payload_path, &payload, 0o600)?;

    let exe = std::env::current_exe()?;
    tracing::info!(runner = %program, "Delegating VPN connect to privileged helper");

    let mut cmd = Command::new(&program);
    capulus::configure_privileged_child_command(&mut cmd, &program);
    cmd.args(&prefix_args);
    cmd.arg(exe);
    cmd.arg("--internal-openconnect-payload");
    cmd.arg(&payload_path);
    cmd.arg("--log-level");
    cmd.arg(log_level_as_cli_arg(log_level));
    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        cmd.env("RUST_LOG", rust_log);
    }

    let status = cmd.status()?;

    let mut result = match read_toml_file::<OpenConnectResult>(&result_path) {
        Ok(Some(result)) => result,
        Ok(None) => OpenConnectResult {
            exit_code: exit_code(status),
            auth_failed: !status.success(),
            ..OpenConnectResult::default()
        },
        Err(err) => {
            tracing::warn!(
                file = %result_path.display(),
                %err,
                "Failed to read privileged connect result; using child exit code fallback"
            );
            OpenConnectResult {
                exit_code: exit_code(status),
                auth_failed: !status.success(),
                ..OpenConnectResult::default()
            }
        }
    };

    if result.exit_code == 0 && !status.success() {
        result.exit_code = exit_code(status);
    }

    cleanup_temp_file(&payload_path);
    cleanup_temp_file(&result_path);

    if let Some(cmd) = on_disconnect.filter(|s| !s.trim().is_empty()) {
        handle_disconnect(cmd);
    }

    Ok(result)
}

#[cfg(unix)]
fn privileged_command_prefix() -> Result<(String, Vec<String>), AppError> {
    if which("doas").is_ok() {
        return Ok(("doas".to_string(), Vec::new()));
    }
    if which("sudo").is_ok() {
        return Ok(("sudo".to_string(), Vec::new()));
    }
    Err(AppError::NeedRoot)
}

fn log_level_as_cli_arg(level: LogLevel) -> &'static str {
    match level {
        LogLevel::Error => "error",
        LogLevel::Warn => "warn",
        LogLevel::Info => "info",
        LogLevel::Debug => "debug",
        LogLevel::Trace => "trace",
    }
}

fn exit_code(status: std::process::ExitStatus) -> i32 {
    match status.code() {
        Some(code) => code,
        None => {
            #[cfg(unix)]
            {
                use std::os::unix::process::ExitStatusExt;
                status.signal().map(|s| 128 + s).unwrap_or(1)
            }
            #[cfg(not(unix))]
            {
                1
            }
        }
    }
}

fn create_secure_temp_file(prefix: &str, extension: &str, mode: u32) -> Result<PathBuf, AppError> {
    capulus::temp::create_secure_temp_file(prefix, extension, mode)
        .map_err(|error| AppError::Io(io::Error::other(error.to_string())))
}

fn write_toml_file<T: Serialize>(path: &Path, value: &T, mode: u32) -> Result<(), AppError> {
    capulus::temp::write_toml_file(path, value, mode)
        .map_err(|error| AppError::Config(error.to_string()))
}

fn read_toml_file<T: serde::de::DeserializeOwned>(path: &Path) -> Result<Option<T>, AppError> {
    capulus::temp::read_toml_file(path).map_err(|error| AppError::Config(error.to_string()))
}

fn cleanup_temp_file(path: &Path) {
    if let Err(err) = capulus::temp::cleanup_temp_file(path) {
        tracing::debug!(file = %path.display(), %err, "Failed to remove temporary file");
    }
}

fn to_core_log_level(level: LogLevel) -> CoreLogLevel {
    match level {
        LogLevel::Error | LogLevel::Warn | LogLevel::Info => CoreLogLevel::Err,
        LogLevel::Debug => CoreLogLevel::Debug,
        LogLevel::Trace => CoreLogLevel::Trace,
    }
}

#[cfg(not(target_os = "windows"))]
const OCULAR_ADD_ROUTES_HOOK: &str = r#"if [ -z "$IPROUTE" ]; then
	echo "ocular: --routes add requires iproute2; falling back to stock vpnc-script routing" >&2
	return 0
fi

OCULAR_ROUTE_STATE_FILE="/var/run/vpnc/ocular-routes.$VPNPID"
OCULAR_ROUTE_PROTO=186

unset INTERNAL_IP4_NETMASK INTERNAL_IP4_NETMASKLEN INTERNAL_IP4_NETADDR INTERNAL_IP6_NETMASK

ocular_log() {
	echo "ocular: $*" >&2
}

ocular_default_metric() {
	FAMILY="$1"
	if [ "$FAMILY" = "6" ]; then
		BEST="$(
			$IPROUTE -6 route show default |
				awk -v tun="$TUNDEV" '
					$0 !~ ("(^| )dev " tun "($| )") {
						if (match($0, /metric [0-9]+/)) {
							print substr($0, RSTART + 7, RLENGTH - 7)
						} else {
							print 0
						}
					}
				' |
				sort -n |
				head -n 1
		)"
	else
		BEST="$(
			$IPROUTE route show default |
				awk -v tun="$TUNDEV" '
					$0 !~ ("(^| )dev " tun "($| )") {
						if (match($0, /metric [0-9]+/)) {
							print substr($0, RSTART + 7, RLENGTH - 7)
						} else {
							print 0
						}
					}
				' |
				sort -n |
				head -n 1
		)"
	fi

	case "$BEST" in
	"")
		echo 50
		;;
	0)
		echo 0
		;;
	*)
		expr "$BEST" - 1
		;;
	esac
}

ocular_record_route() {
	umask 077
	printf '%s\n' "$*" >>"$OCULAR_ROUTE_STATE_FILE"
}

ocular_delete_recorded_routes() {
	FAMILY="$1"
	MATCH_KIND="$2"
	MATCH_VALUE="$3"
	[ -f "$OCULAR_ROUTE_STATE_FILE" ] || return 0

	TMP="${OCULAR_ROUTE_STATE_FILE}.tmp.$$"
	umask 077
	: >"$TMP" || return 1

	while IFS= read -r line; do
		[ -n "$line" ] || continue
		LINE_FAMILY=${line%% *}
		LINE_SPEC=${line#* }

		MATCHED=0
		if [ "$LINE_FAMILY" = "$FAMILY" ]; then
			case "$MATCH_KIND" in
			exact)
				[ "$LINE_SPEC" = "$MATCH_VALUE" ] && MATCHED=1
				;;
			prefix)
				case "$LINE_SPEC" in
				"$MATCH_VALUE"*) MATCHED=1 ;;
				esac
				;;
			esac
		fi

		if [ "$MATCHED" = 1 ]; then
			if [ "$FAMILY" = "6" ]; then
				$IPROUTE -6 route del $LINE_SPEC 2>/dev/null
			else
				$IPROUTE route del $LINE_SPEC 2>/dev/null
			fi
		else
			printf '%s\n' "$line" >>"$TMP"
		fi
	done <"$OCULAR_ROUTE_STATE_FILE"

	mv "$TMP" "$OCULAR_ROUTE_STATE_FILE"
	[ -s "$OCULAR_ROUTE_STATE_FILE" ] || rm -f -- "$OCULAR_ROUTE_STATE_FILE"
	if [ "$FAMILY" = "6" ]; then
		$IPROUTE -6 route flush cache 2>/dev/null
	fi
}

ocular_try_add_route() {
	FAMILY="$1"
	shift
	if [ "$FAMILY" = "6" ]; then
		OUTPUT="$($IPROUTE -6 route add "$@" 2>&1)"
	else
		OUTPUT="$($IPROUTE route add "$@" 2>&1)"
	fi
	STATUS=$?
	if [ $STATUS -eq 0 ]; then
		ocular_record_route "$FAMILY $*"
		if [ "$FAMILY" = "6" ]; then
			$IPROUTE -6 route flush cache 2>/dev/null
		fi
		return 0
	fi

	case "$OUTPUT" in
	*"File exists"*)
		ocular_log "preserving existing route: $*"
		return 1
		;;
	*)
		ocular_log "failed to add route '$*': $OUTPUT"
		return $STATUS
		;;
	esac
}

ocular_has_non_tunnel_exact_route() {
	FAMILY="$1"
	TARGET="$2"
	if [ "$FAMILY" = "6" ]; then
		$IPROUTE -6 route show exact "$TARGET" |
			awk -v tun="$TUNDEV" 'NF && $0 !~ ("(^| )dev " tun "($| )") { found = 1 } END { exit(found ? 0 : 1) }'
	else
		$IPROUTE route show exact "$TARGET" |
			awk -v tun="$TUNDEV" 'NF && $0 !~ ("(^| )dev " tun "($| )") { found = 1 } END { exit(found ? 0 : 1) }'
	fi
}

ocular_set_ipv4_default_route() {
	METRIC="$(ocular_default_metric 4)"
	ocular_try_add_route 4 default dev "$TUNDEV" metric "$METRIC" proto "$OCULAR_ROUTE_PROTO"
}

ocular_set_ipv4_network_route() {
	NETWORK="$1"
	NETMASKLEN="$3"
	NETDEV="$4"
	NETGW="$5"
	PREFIX="$NETWORK/$NETMASKLEN"

	if ocular_has_non_tunnel_exact_route 4 "$PREFIX"; then
		ocular_log "preserving existing route: $PREFIX"
		return 0
	fi

	if [ -n "$NETGW" ]; then
		ocular_try_add_route 4 "$PREFIX" dev "$NETDEV" via "$NETGW" proto "$OCULAR_ROUTE_PROTO"
	else
		ocular_try_add_route 4 "$PREFIX" dev "$NETDEV" proto "$OCULAR_ROUTE_PROTO"
	fi
}

ocular_reset_ipv4_default_route() {
	rm -f -- "$DEFAULT_ROUTE_FILE"
	ocular_delete_recorded_routes 4 prefix "default dev $TUNDEV "
}

ocular_del_ipv4_network_route() {
	NETWORK="$1"
	NETMASKLEN="$3"
	NETDEV="$4"
	NETGW="$5"
	PREFIX="$NETWORK/$NETMASKLEN"

	if [ -n "$NETGW" ]; then
		ocular_delete_recorded_routes 4 exact "$PREFIX dev $NETDEV via $NETGW proto $OCULAR_ROUTE_PROTO"
	else
		ocular_delete_recorded_routes 4 exact "$PREFIX dev $NETDEV proto $OCULAR_ROUTE_PROTO"
	fi
}

set_ipv4_default_route() {
	ocular_set_ipv4_default_route "$@"
}

set_default_route() {
	ocular_set_ipv4_default_route "$@"
}

set_ipv4_network_route() {
	ocular_set_ipv4_network_route "$@"
}

set_network_route() {
	ocular_set_ipv4_network_route "$@"
}

reset_ipv4_default_route() {
	ocular_reset_ipv4_default_route "$@"
}

reset_default_route() {
	ocular_reset_ipv4_default_route "$@"
}

del_ipv4_network_route() {
	ocular_del_ipv4_network_route "$@"
}

del_network_route() {
	ocular_del_ipv4_network_route "$@"
}

set_ipv6_default_route() {
	METRIC="$(ocular_default_metric 6)"
	ocular_try_add_route 6 default dev "$TUNDEV" metric "$METRIC" proto "$OCULAR_ROUTE_PROTO"
}

set_ipv6_network_route() {
	NETWORK="$1"
	NETMASKLEN="$2"
	NETDEV="$3"
	NETGW="$4"
	PREFIX="$NETWORK/$NETMASKLEN"

	if ocular_has_non_tunnel_exact_route 6 "$PREFIX"; then
		ocular_log "preserving existing route: $PREFIX"
		return 0
	fi

	if [ -n "$NETGW" ]; then
		ocular_try_add_route 6 "$PREFIX" dev "$NETDEV" via "$NETGW" proto "$OCULAR_ROUTE_PROTO"
	else
		ocular_try_add_route 6 "$PREFIX" dev "$NETDEV" proto "$OCULAR_ROUTE_PROTO"
	fi
}

reset_ipv6_default_route() {
	rm -f -- "$DEFAULT_ROUTE_FILE_IPV6"
	ocular_delete_recorded_routes 6 prefix "default dev $TUNDEV "
}

del_ipv6_network_route() {
	NETWORK="$1"
	NETMASKLEN="$2"
	NETDEV="$3"
	NETGW="$4"
	PREFIX="$NETWORK/$NETMASKLEN"

	if [ -n "$NETGW" ]; then
		ocular_delete_recorded_routes 6 exact "$PREFIX dev $NETDEV via $NETGW proto $OCULAR_ROUTE_PROTO"
	else
		ocular_delete_recorded_routes 6 exact "$PREFIX dev $NETDEV proto $OCULAR_ROUTE_PROTO"
	fi
}
"#;

#[cfg(not(target_os = "windows"))]
fn build_selective_routes_hook(routes: &SelectiveRoutes) -> String {
    let mut hook = String::from(
        "unset INTERNAL_IP4_NETMASK INTERNAL_IP4_NETMASKLEN INTERNAL_IP4_NETADDR INTERNAL_IP6_NETMASK\n\
unset INTERNAL_IP4_DNS INTERNAL_IP4_NBNS INTERNAL_IP6_DNS CISCO_DEF_DOMAIN CISCO_SPLIT_DNS\n\
CISCO_SPLIT_EXC=0\n\
export CISCO_SPLIT_EXC\n\
CISCO_IPV6_SPLIT_EXC=0\n\
export CISCO_IPV6_SPLIT_EXC\n",
    );

    writeln!(&mut hook, "CISCO_SPLIT_INC={}", routes.ipv4.len())
        .expect("write selective IPv4 route count");
    hook.push_str("export CISCO_SPLIT_INC\n");
    for (index, net) in routes.ipv4.iter().enumerate() {
        let prefix_len = net.prefix_len().to_string();
        writeln!(
            &mut hook,
            "CISCO_SPLIT_INC_{index}_ADDR={}",
            shell::sh_quote(&net.network().to_string())
        )
        .expect("write selective IPv4 route address");
        writeln!(
            &mut hook,
            "CISCO_SPLIT_INC_{index}_MASK={}",
            shell::sh_quote(&net.netmask().to_string())
        )
        .expect("write selective IPv4 route netmask");
        writeln!(
            &mut hook,
            "CISCO_SPLIT_INC_{index}_MASKLEN={}",
            shell::sh_quote(&prefix_len)
        )
        .expect("write selective IPv4 route prefix length");
        writeln!(
            &mut hook,
            "export CISCO_SPLIT_INC_{index}_ADDR CISCO_SPLIT_INC_{index}_MASK CISCO_SPLIT_INC_{index}_MASKLEN"
        )
        .expect("write selective IPv4 export");
    }

    writeln!(&mut hook, "CISCO_IPV6_SPLIT_INC={}", routes.ipv6.len())
        .expect("write selective IPv6 route count");
    hook.push_str("export CISCO_IPV6_SPLIT_INC\n");
    for (index, net) in routes.ipv6.iter().enumerate() {
        let prefix_len = net.prefix_len().to_string();
        writeln!(
            &mut hook,
            "CISCO_IPV6_SPLIT_INC_{index}_ADDR={}",
            shell::sh_quote(&net.network().to_string())
        )
        .expect("write selective IPv6 route address");
        writeln!(
            &mut hook,
            "CISCO_IPV6_SPLIT_INC_{index}_MASKLEN={}",
            shell::sh_quote(&prefix_len)
        )
        .expect("write selective IPv6 route prefix length");
        writeln!(
            &mut hook,
            "export CISCO_IPV6_SPLIT_INC_{index}_ADDR CISCO_IPV6_SPLIT_INC_{index}_MASKLEN"
        )
        .expect("write selective IPv6 export");
    }

    hook
}

#[cfg(not(target_os = "windows"))]
fn build_vpnc_hook_content(
    routes_mode: RoutesMode,
    selective_routes: &SelectiveRoutes,
) -> Option<String> {
    let mut parts = Vec::new();
    if !selective_routes.is_empty() {
        parts.push(build_selective_routes_hook(selective_routes));
    }
    if routes_mode == RoutesMode::Add {
        parts.push(OCULAR_ADD_ROUTES_HOOK.to_string());
    }

    (!parts.is_empty()).then(|| parts.join("\n\n"))
}

#[cfg(not(target_os = "windows"))]
#[derive(Debug)]
struct VpncScriptCapture {
    _temp_dir: TempDir,
    wrapper_path: PathBuf,
    log_path: PathBuf,
}

#[cfg(target_os = "windows")]
#[derive(Debug)]
struct VpncScriptCapture;

#[cfg(not(target_os = "windows"))]
impl VpncScriptCapture {
    fn new(
        vpnc_script: &str,
        routes: RoutesMode,
        selective_routes: &SelectiveRoutes,
    ) -> Result<Self, AppError> {
        let temp_dir = TempDirBuilder::new().prefix("ocular-vpnc-").tempdir()?;
        let wrapper_path = temp_dir.path().join("vpnc-wrapper.sh");
        let log_path = temp_dir.path().join("vpnc-script.log");
        let script_path = match build_vpnc_hook_content(routes, selective_routes) {
            Some(hook_content) => {
                prepare_patched_vpnc_script(temp_dir.path(), vpnc_script, &hook_content)?
            }
            None => PathBuf::from(vpnc_script),
        };

        let log_path_str = log_path.to_string_lossy().to_string();
        let script_body = format!(
            "#!/bin/sh\nexec {} \"$@\" >>{} 2>&1\n",
            shell::sh_quote(&script_path.to_string_lossy()),
            shell::sh_quote(&log_path_str),
        );
        write_file_with_mode(&log_path, "", 0o600)?;
        write_file_with_mode(&wrapper_path, &script_body, 0o700)?;

        Ok(Self {
            _temp_dir: temp_dir,
            wrapper_path,
            log_path,
        })
    }

    fn wrapper_path_str(&self) -> String {
        self.wrapper_path.to_string_lossy().to_string()
    }

    fn emit_debug_logs(&self) {
        if !tracing::enabled!(tracing::Level::DEBUG) {
            return;
        }

        let raw = match fs::read_to_string(&self.log_path) {
            Ok(raw) => raw,
            Err(err) if err.kind() == io::ErrorKind::NotFound => return,
            Err(err) => {
                tracing::debug!(
                    file = %self.log_path.display(),
                    %err,
                    "Failed to read vpnc-script log capture"
                );
                return;
            }
        };

        for line in raw.lines().map(str::trim).filter(|line| !line.is_empty()) {
            tracing::debug!(vpnc_script = %line);
        }
    }
}

#[cfg(not(target_os = "windows"))]
impl Drop for VpncScriptCapture {
    fn drop(&mut self) {
        self.emit_debug_logs();
    }
}

#[cfg(not(target_os = "windows"))]
fn configure_vpnc_script(
    builder: &mut ConfigBuilder,
    routes: RoutesMode,
    selective_routes: &SelectiveRoutes,
) -> Result<Option<VpncScriptCapture>, AppError> {
    let requires_patch = !selective_routes.is_empty();
    if let Some(vpnc_script) = discover_vpnc_script() {
        match VpncScriptCapture::new(&vpnc_script, routes, selective_routes) {
            Ok(capture) => {
                tracing::debug!(
                    script = %vpnc_script,
                    ?routes,
                    selective = !selective_routes.is_empty(),
                    "Using vpnc-script wrapper"
                );
                let wrapper = capture.wrapper_path_str();
                builder.vpncscript(&wrapper);
                Ok(Some(capture))
            }
            Err(err) => {
                if requires_patch {
                    return Err(err);
                }
                tracing::warn!(
                    %err,
                    script = %vpnc_script,
                    "Failed to prepare vpnc-script wrapper; using script directly"
                );
                builder.vpncscript(&vpnc_script);
                Ok(None)
            }
        }
    } else {
        if requires_patch {
            return Err(AppError::Config(
                "vpnc-script is required for selective tunnel routes but was not found".to_string(),
            ));
        }
        tracing::warn!(
            "vpnc-script was not found in PATH/common locations; set OCULAR_VPNC_SCRIPT to avoid './vpnc-script' failures"
        );
        Ok(None)
    }
}

#[cfg(target_os = "windows")]
fn configure_vpnc_script(
    _builder: &mut ConfigBuilder,
    _routes: RoutesMode,
    _selective_routes: &SelectiveRoutes,
) -> Result<Option<VpncScriptCapture>, AppError> {
    Ok(None)
}

#[cfg(not(target_os = "windows"))]
fn prepare_patched_vpnc_script(
    temp_dir: &Path,
    vpnc_script: &str,
    hook_content: &str,
) -> Result<PathBuf, AppError> {
    let hooks_root = temp_dir.join("hooks");
    fs::create_dir_all(hooks_root.join("connect.d"))?;
    fs::create_dir_all(hooks_root.join("disconnect.d"))?;
    let hook_path = "ocular-routes.sh";
    write_file_with_mode(
        &hooks_root.join("connect.d").join(hook_path),
        hook_content,
        0o600,
    )?;
    write_file_with_mode(
        &hooks_root.join("disconnect.d").join(hook_path),
        hook_content,
        0o600,
    )?;

    let original = fs::read_to_string(vpnc_script)?;
    let patched = original.replacen(
        VPNSCRIPT_HOOKS_DIR_MARKER,
        &format!(
            "HOOKS_DIR={}",
            shell::sh_quote(&hooks_root.to_string_lossy()),
        ),
        1,
    );
    if patched == original {
        return Err(AppError::Config(format!(
            "could not patch HOOKS_DIR in {}",
            vpnc_script
        )));
    }

    let patched_script = temp_dir.join("vpnc-script");
    write_file_with_mode(&patched_script, &patched, 0o700)?;
    Ok(patched_script)
}

#[cfg(not(target_os = "windows"))]
fn write_file_with_mode(path: &Path, content: &str, mode: u32) -> Result<(), AppError> {
    fs::write(path, content)?;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))?;
    Ok(())
}

#[cfg(not(target_os = "windows"))]
fn discover_vpnc_script() -> Option<String> {
    if let Ok(path) = std::env::var("OCULAR_VPNC_SCRIPT") {
        let trimmed = path.trim();
        if !trimmed.is_empty() && Path::new(trimmed).is_file() {
            return Some(trimmed.to_string());
        }
    }

    if let Ok(path) = which("vpnc-script") {
        return Some(path.to_string_lossy().to_string());
    }

    let candidates = [
        "/etc/vpnc/vpnc-script",
        "/usr/share/vpnc-scripts/vpnc-script",
        "/usr/local/etc/vpnc/vpnc-script",
        "/opt/homebrew/etc/vpnc/vpnc-script",
    ];

    candidates.iter().find_map(|candidate| {
        Path::new(candidate)
            .is_file()
            .then(|| (*candidate).to_string())
    })
}

fn handle_disconnect(command: &str) {
    tracing::info!(command, "Running command on disconnect");
    #[cfg(unix)]
    let status = Command::new("sh").arg("-c").arg(command).status();
    #[cfg(windows)]
    let status = Command::new("cmd").arg("/C").arg(command).status();
    match status {
        Ok(st) => tracing::debug!(code = ?st.code(), "Disconnect command exited"),
        Err(err) => tracing::warn!(%err, "Disconnect command failed"),
    }
}

#[derive(Debug)]
struct LiveConnectionLine {
    pb: ProgressBar,
}

impl LiveConnectionLine {
    fn new() -> Self {
        let pb = ProgressBar::new_spinner();
        pb.set_style(capulus::ui::spinner_style("{spinner:.cyan} {msg}"));
        pb.enable_steady_tick(Duration::from_millis(120));
        pb.set_message("connecting...".to_string());
        Self { pb }
    }

    fn set_connecting(&self) {
        self.pb.set_message("connecting...".to_string());
    }

    fn set_connected(&self) {
        self.pb.set_message("\x1b[32mconnected\x1b[0m".to_string());
    }

    fn set_reconnecting(&self) {
        self.pb
            .set_message("disconnected, reconnecting...".to_string());
    }

    fn set_disconnecting(&self) {
        self.pb.set_message("disconnecting...".to_string());
    }

    fn finish(&self) {
        self.pb.finish_and_clear();
    }
}

#[derive(Debug)]
struct Lifecycle {
    connected_once: bool,
    connecting_announced: bool,
    last_error: Option<OpenconnectError>,
    live_line: Option<LiveConnectionLine>,
}

impl Lifecycle {
    fn new(interactive: bool) -> Self {
        Self {
            connected_once: false,
            connecting_announced: false,
            last_error: None,
            live_line: interactive.then(LiveConnectionLine::new),
        }
    }

    fn finish_live_line(&self) {
        if let Some(line) = self.live_line.as_ref() {
            line.finish();
        }
    }
}

impl Drop for Lifecycle {
    fn drop(&mut self) {
        self.finish_live_line();
    }
}

fn handle_status_event(state: &Arc<Mutex<Lifecycle>>, status: Status) {
    let mut state = state.lock().unwrap_or_else(|e| e.into_inner());
    match status {
        Status::Initialized => {}
        Status::Connecting(stage) => {
            if !state.connecting_announced {
                tracing::info!("OpenConnect connecting...");
                state.connecting_announced = true;
            }
            if let Some(line) = state.live_line.as_ref() {
                line.set_connecting();
            }
            tracing::debug!(stage = %stage, "OpenConnect stage");
        }
        Status::Connected => {
            if state.connected_once {
                tracing::info!("OpenConnect reconnected");
            } else {
                tracing::info!("OpenConnect connected!");
                state.connected_once = true;
            }
            if let Some(line) = state.live_line.as_ref() {
                line.set_connected();
            }
            state.connecting_announced = false;
            state.last_error = None;
        }
        Status::Disconnecting => {
            if let Some(line) = state.live_line.as_ref() {
                line.set_disconnecting();
            }
            tracing::info!("OpenConnect disconnecting...");
        }
        Status::Disconnected => {
            if let Some(line) = state.live_line.as_ref() {
                line.set_reconnecting();
            }
            tracing::warn!("OpenConnect disconnected");
            state.connecting_announced = false;
        }
        Status::Error(err) => {
            state.last_error = Some(err.clone());
            tracing::warn!(error = %err, "OpenConnect error");
        }
    }
}

fn parse_expected_hashes(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn normalize_cert_hash(hash: &str) -> String {
    hash.trim().to_ascii_lowercase()
}

fn is_hex_hash(hash: &str, len: usize) -> bool {
    hash.len() == len && hash.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn peer_cert_hashes(client: &VpnClient) -> Option<PeerCertHashes> {
    let cert_der = match client.get_peer_cert_der() {
        Ok(cert_der) if !cert_der.is_empty() => cert_der,
        Ok(_) => {
            tracing::debug!("Peer certificate DER was empty");
            return None;
        }
        Err(err) => {
            tracing::debug!(error = %err, "Failed to read peer certificate DER");
            return None;
        }
    };

    Some(PeerCertHashes {
        sha1: format!("{:x}", Sha1::digest(&cert_der)),
        sha256: format!("{:x}", Sha256::digest(&cert_der)),
    })
}

fn cert_hash_matches(expected: &str, actual_hashes: &PeerCertHashes) -> bool {
    let expected = expected.trim();

    if let Some((prefix, value)) = expected.split_once(':') {
        if prefix.eq_ignore_ascii_case("sha1") {
            return actual_hashes.sha1 == normalize_cert_hash(value);
        }
        if prefix.eq_ignore_ascii_case("sha256") {
            return actual_hashes.sha256 == normalize_cert_hash(value);
        }
        return false;
    }

    if is_hex_hash(expected, 40) {
        return actual_hashes.sha1 == normalize_cert_hash(expected);
    }

    if is_hex_hash(expected, 64) {
        return actual_hashes.sha256 == normalize_cert_hash(expected);
    }

    false
}

fn handle_invalid_cert(
    expected_hashes: &[String],
    client_slot: &Arc<Mutex<Option<Arc<VpnClient>>>>,
    actual_hash: &str,
) -> bool {
    if expected_hashes.is_empty() {
        tracing::warn!("No pinned hash available; rejecting untrusted certificate");
        return false;
    }

    if let Ok(slot) = client_slot.lock()
        && let Some(client) = slot.as_ref()
        && cert_hash_matches_any(client, expected_hashes)
    {
        tracing::info!(
            hash = actual_hash,
            "Accepted VPN server certificate by pinned hash"
        );
        true
    } else {
        tracing::warn!(
            expected = ?expected_hashes,
            actual = actual_hash,
            "Rejected VPN server certificate (hash mismatch)"
        );
        false
    }
}

fn cert_hash_matches_any(client: &VpnClient, expected_hashes: &[String]) -> bool {
    let Some(actual_hashes) = peer_cert_hashes(client) else {
        return false;
    };
    expected_hashes
        .iter()
        .any(|expected| cert_hash_matches(expected, &actual_hashes))
}

fn is_probably_auth_failure(err: &OpenconnectError) -> bool {
    matches!(
        err,
        OpenconnectError::SetCookieError(_)
            | OpenconnectError::ObtainCookieError(_)
            | OpenconnectError::MakeCstpError(_)
            | OpenconnectError::MainLoopError(_)
    )
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use ipnet::{Ipv4Net, Ipv6Net};

    use super::{
        OpenConnectResult, PRIVILEGED_HANDOFF_FILE_EXTENSION, RoutesMode, SelectiveRoutes,
        build_selective_routes_hook, build_vpnc_hook_content, create_secure_temp_file,
        read_toml_file, write_toml_file,
    };

    struct TempFile(PathBuf);

    impl Drop for TempFile {
        fn drop(&mut self) {
            super::cleanup_temp_file(&self.0);
        }
    }

    #[test]
    fn privileged_handoff_files_use_toml_extension() {
        let path = TempFile(
            create_secure_temp_file("payload-test", PRIVILEGED_HANDOFF_FILE_EXTENSION, 0o600)
                .expect("create temp file"),
        );
        assert_eq!(
            path.0.extension().and_then(|ext| ext.to_str()),
            Some("toml")
        );
    }

    #[test]
    fn privileged_handoff_round_trips_toml() {
        let path = TempFile(
            create_secure_temp_file("result-test", PRIVILEGED_HANDOFF_FILE_EXTENSION, 0o600)
                .expect("create temp file"),
        );
        let expected = OpenConnectResult {
            exit_code: 7,
            auth_failed: true,
            expires_at_epoch: Some(123),
            expires_at_text: Some("soon".to_string()),
        };

        write_toml_file(&path.0, &expected, 0o600).expect("write toml");
        let raw = std::fs::read_to_string(&path.0).expect("read raw toml");
        assert!(raw.contains("exit_code = 7"));
        assert_eq!(
            read_toml_file::<OpenConnectResult>(&path.0).expect("read toml"),
            Some(expected)
        );
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn additive_vpnc_script_redirects_hooks_and_writes_hook_files() {
        let temp = tempfile::tempdir().expect("tempdir");
        let source = temp.path().join("source-vpnc-script");
        fs::write(&source, "#!/bin/sh\nHOOKS_DIR=/etc/vpnc\n").expect("write source");
        let hook_content = build_vpnc_hook_content(RoutesMode::Add, &SelectiveRoutes::default())
            .expect("hook content");

        let patched = super::prepare_patched_vpnc_script(
            temp.path(),
            source.to_str().expect("utf-8 path"),
            &hook_content,
        )
        .expect("patch script");
        let hooks_root = temp.path().join("hooks");
        let expected_hooks_dir = format!(
            "HOOKS_DIR={}",
            crate::shell::sh_quote(&hooks_root.to_string_lossy()),
        );
        let patched_raw = fs::read_to_string(&patched).expect("read patched script");
        let hook_name = "ocular-routes.sh";
        let connect_hook = fs::read_to_string(hooks_root.join("connect.d").join(hook_name))
            .expect("read connect hook");

        assert!(patched_raw.contains(&expected_hooks_dir));
        assert!(connect_hook.contains("set_ipv4_default_route()"));
        assert!(connect_hook.contains("set_default_route()"));
        assert!(connect_hook.contains("set_ipv4_network_route()"));
        assert!(connect_hook.contains("set_network_route()"));
        assert!(connect_hook.contains("reset_ipv4_default_route()"));
        assert!(connect_hook.contains("reset_default_route()"));
        assert!(connect_hook.contains("del_ipv4_network_route()"));
        assert!(connect_hook.contains("del_network_route()"));
        assert!(hooks_root.join("connect.d").join(hook_name).is_file());
        assert!(hooks_root.join("disconnect.d").join(hook_name).is_file());
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn selective_routes_hook_rewrites_split_include_env() {
        let routes = SelectiveRoutes {
            ipv4: vec![
                Ipv4Net::new("140.247.39.160".parse().expect("ipv4"), 32).expect("host net"),
                Ipv4Net::new("10.7.0.12".parse().expect("ipv4"), 24)
                    .expect("prefix net")
                    .trunc(),
            ],
            ipv6: vec![
                Ipv6Net::new("2001:db8::1".parse().expect("ipv6"), 64)
                    .expect("prefix net")
                    .trunc(),
            ],
        };
        let hook = build_selective_routes_hook(&routes);

        assert!(hook.contains("unset INTERNAL_IP4_NETMASK INTERNAL_IP4_NETMASKLEN INTERNAL_IP4_NETADDR INTERNAL_IP6_NETMASK"));
        assert!(hook.contains("unset INTERNAL_IP4_DNS INTERNAL_IP4_NBNS INTERNAL_IP6_DNS CISCO_DEF_DOMAIN CISCO_SPLIT_DNS"));
        assert!(hook.contains("CISCO_SPLIT_EXC=0"));
        assert!(hook.contains("CISCO_SPLIT_INC=2"));
        assert!(hook.contains(&format!(
            "CISCO_SPLIT_INC_0_ADDR={}",
            crate::shell::sh_quote("140.247.39.160")
        )));
        assert!(hook.contains(&format!(
            "CISCO_SPLIT_INC_1_ADDR={}",
            crate::shell::sh_quote("10.7.0.0")
        )));
        assert!(hook.contains(&format!(
            "CISCO_SPLIT_INC_1_MASK={}",
            crate::shell::sh_quote("255.255.255.0")
        )));
        assert!(hook.contains(&format!(
            "CISCO_SPLIT_INC_1_MASKLEN={}",
            crate::shell::sh_quote("24")
        )));
        assert!(hook.contains("CISCO_IPV6_SPLIT_INC=1"));
        assert!(hook.contains(&format!(
            "CISCO_IPV6_SPLIT_INC_0_ADDR={}",
            crate::shell::sh_quote("2001:db8::")
        )));
        assert!(hook.contains(&format!(
            "CISCO_IPV6_SPLIT_INC_0_MASKLEN={}",
            crate::shell::sh_quote("64")
        )));
    }

    #[test]
    fn selective_routes_resolve_literals_and_truncate_prefixes() {
        let resolved = SelectiveRoutes::resolve(&[
            "140.247.39.160".to_string(),
            "10.7.0.12/24".to_string(),
            "2001:db8::1234/64".to_string(),
        ])
        .expect("resolve tunnel routes");

        assert_eq!(
            resolved.ipv4,
            vec![
                Ipv4Net::new("10.7.0.0".parse().expect("ipv4"), 24).expect("prefix net"),
                Ipv4Net::new("140.247.39.160".parse().expect("ipv4"), 32).expect("host net"),
            ]
        );
        assert_eq!(
            resolved.ipv6,
            vec![Ipv6Net::new("2001:db8::".parse().expect("ipv6"), 64).expect("prefix net")]
        );
    }

    #[test]
    fn cert_hash_matches_bare_sha1_fingerprint() {
        let actual_hashes = super::PeerCertHashes {
            sha1: "eaae34364443a941bb84402423a9fc7cf8cad01b".to_string(),
            sha256: "347b48cb944664463954474d440c305bdeb126b469ed5ed2099e68ec7a2bd077".to_string(),
        };
        assert_eq!(
            super::cert_hash_matches("EAAE34364443A941BB84402423A9FC7CF8CAD01B", &actual_hashes),
            true
        );
    }

    #[test]
    fn cert_hash_matches_prefixed_sha256_fingerprint() {
        let actual_hashes = super::PeerCertHashes {
            sha1: "eaae34364443a941bb84402423a9fc7cf8cad01b".to_string(),
            sha256: "347b48cb944664463954474d440c305bdeb126b469ed5ed2099e68ec7a2bd077".to_string(),
        };
        assert_eq!(
            super::cert_hash_matches(
                "SHA256:347B48CB944664463954474D440C305BDEB126B469ED5ED2099E68EC7A2BD077",
                &actual_hashes
            ),
            true
        );
    }

    #[test]
    fn cert_hash_does_not_treat_pin_sha256_as_certificate_hash() {
        let actual_hashes = super::PeerCertHashes {
            sha1: "eaae34364443a941bb84402423a9fc7cf8cad01b".to_string(),
            sha256: "347b48cb944664463954474d440c305bdeb126b469ed5ed2099e68ec7a2bd077".to_string(),
        };
        assert_eq!(
            super::cert_hash_matches(
                "pin-sha256:21qYUjnJqVC9nG3Lt9YkFPejMjUbqK62WwH4FB77v5c=",
                &actual_hashes
            ),
            false
        );
    }
}
