#![cfg(unix)]

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Command, Output};

use serde_json::{Value, json};
use tempfile::TempDir;

struct Fixture {
    root: TempDir,
}

impl Fixture {
    fn config(&self, contents: &str) {
        let dir = self.root.path().join("config/ice");
        fs::create_dir_all(&dir).unwrap();
        fs::write(dir.join("config.toml"), contents).unwrap();
    }

    fn gcp_creation(&self) {
        let dir = self.root.path().join(".ice/provider/gcp");
        fs::create_dir_all(&dir).unwrap();
        fs::write(
            dir.join("machine-catalog.toml"),
            r#"
refreshed_at_unix = 1790000000
[[entries]]
machine = "test-cpu"
zone = "test-a"
region = "test"
vcpus = 4
billable_vcpus = 4.0
ram_mb = 16000
gpus = []
hourly_usd = 0.1
"#,
        )
        .unwrap();
        fs::write(
            dir.join("machine-pricing-map.toml"),
            r#"
[[entries]]
machine = "test-cpu"
region = "test"
[[entries.components]]
sku_id = "test-sku"
quantity_source = "per-machine"
"#,
        )
        .unwrap();
        fs::write(
            dir.join("sku-pricing-cache.toml"),
            r#"
refreshed_at_unix = 1790000000
[[entries]]
sku_id = "test-sku"
description = "Fixture compute"
rate_unit = "per-hour"
usd_per_unit = 0.2
"#,
        )
        .unwrap();
        self.script("gcloud", r#"#!/bin/sh
[ "$1" != --version ] || exit 0
[ "$CLOUDSDK_CORE_DISABLE_PROMPTS" = 1 ] || exit 71
printf '%s\n' "$*" >> "$ICE_TEST_ACTIONS"
row() {
  name=$(/bin/cat "$ICE_TEST_STATE.name")
  state=$(/bin/cat "$ICE_TEST_STATE")
  printf '{"name":"%s","status":"%s","zone":"zones/test-a","metadata":{"items":[{"key":"ice-workload-kind","value":"shell"}]}}' "$name" "$state"
}
case "$1:$2:$3" in
  compute:instances:list)
    if [ -f "$ICE_TEST_STATE.name" ]; then printf '['; row; printf ']'; else printf '[]'; fi ;;
  compute:instances:create)
    printf '%s' "$4" > "$ICE_TEST_STATE.name"
    printf '%s' "${ICE_TEST_CREATED_STATE:-RUNNING}" > "$ICE_TEST_STATE"
    printf 'provider creation output\n' ;;
  compute:instances:describe) row ;;
  compute:instances:stop) printf TERMINATED > "$ICE_TEST_STATE" ;;
  compute:instances:start) printf RUNNING > "$ICE_TEST_STATE" ;;
  compute:instances:delete) /bin/rm "$ICE_TEST_STATE.name" ;;
  compute:ssh:*|compute:scp:*)
    case "$*" in *BatchMode=yes*) ;; *) exit 72 ;; esac
    if read -r answer; then exit 73; fi
    [ "$ICE_TEST_TRANSFER_FAIL" != 1 ] || exit 74
    printf 'transport output\n' ;;
  *) printf 'unexpected command: %s\n' "$*" >&2; exit 75 ;;
esac
"#);
    }

    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        fs::create_dir(root.path().join("bin")).unwrap();
        fs::create_dir(root.path().join("config")).unwrap();
        fs::create_dir(root.path().join("runtime")).unwrap();
        Self { root }
    }

    fn provider(&self, rows: Value, failure: bool) {
        fs::write(self.root.path().join("response.json"), rows.to_string()).unwrap();
        let script = if failure {
            "#!/bin/sh\nprintf '%s\\n' 'provider unavailable' >&2\nexit 7\n".to_owned()
        } else {
            "#!/bin/sh\nprintf '%s\\n' 'provider diagnostic' >&2\n/bin/cat \"$ICE_TEST_RESPONSE\"\n"
                .to_owned()
        };
        self.script("gcloud", &script);
    }

    fn script(&self, name: &str, script: &str) {
        let path = self.root.path().join("bin").join(name);
        fs::write(&path, script).unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).unwrap();
    }

    fn command(&self) -> Command {
        let mut command = Command::new(env!("CARGO_BIN_EXE_ice"));
        command
            .env("HOME", self.root.path())
            .env("PATH", self.root.path().join("bin"))
            .env("XDG_CONFIG_HOME", self.root.path().join("config"))
            .env("XDG_RUNTIME_DIR", self.root.path().join("runtime"))
            .env("ICE_TEST_RESPONSE", self.root.path().join("response.json"))
            .env("ICE_TEST_STATE", self.root.path().join("state"))
            .env("ICE_TEST_ACTIONS", self.root.path().join("actions"))
            .env_remove("VAST_API_KEY")
            .env_remove("VERDA_CLIENT_ID")
            .env_remove("VERDA_CLIENT_SECRET")
            .env_remove("AWS_ACCESS_KEY_ID")
            .env_remove("AWS_SECRET_ACCESS_KEY")
            .env_remove("AWS_SESSION_TOKEN");
        command
    }

    fn run(&self, args: &[&str]) -> Output {
        self.command().args(args).output().unwrap()
    }

    fn unpack(&self, logs: &[u8], exit_code: Option<i32>) -> PathBuf {
        let dir = self
            .root
            .path()
            .join("config/ice/provider/local/unpack/ice-example");
        fs::create_dir_all(dir.join("rootfs")).unwrap();
        fs::write(
            dir.join("instance.toml"),
            concat!(
                "id = 'ice-example'\nname = 'ice-example'\nsource = 'example:tag'\n",
                "created_at = '2026-01-01T00:00:00Z'\n",
            ),
        )
        .unwrap();
        fs::write(dir.join("stdio.log"), logs).unwrap();
        if let Some(code) = exit_code {
            fs::write(dir.join("exit-code"), code.to_string()).unwrap();
        }
        dir
    }

    fn runtime(&self) {
        self.script("docker", r#"#!/bin/sh
case "$1" in
  version) exit 0 ;;
  ps) if [ ! -f "$ICE_TEST_STATE.deleted" ]; then printf 'test-id\n'; fi ;;
  inspect)
    state=running
    if [ -f "$ICE_TEST_STATE" ]; then state=$(/bin/cat "$ICE_TEST_STATE"); fi
    printf '[{"Id":"test-id","Name":"/ice-example","State":{"Status":"%s"},"Config":{"Image":"example:tag","Labels":{"ice-managed":"true","ice-cloud":"local","ice-workload-kind":"container","ice-workload-container":"us-central1-docker.pkg.dev/test/repo/image:tag","ice-workload-registry":"gar"}}}]\n' "$state"
    ;;
  stop) printf stopped > "$ICE_TEST_STATE"; printf 'provider stop output\n' ;;
  start) printf running > "$ICE_TEST_STATE"; printf 'provider start output\n' ;;
  rm) printf deleted > "$ICE_TEST_STATE.deleted" ;;
  logs) printf 'stdout 🦀\n'; printf 'stderr 終' >&2 ;;
  login) /bin/cat >/dev/null; printf 'registry login output\n' ;;
  *) printf 'provider operation output\n' ;;
esac
printf '%s\n' "$1" >> "$ICE_TEST_ACTIONS"
"#);
    }
}

fn bounded_output(mut command: Command) -> Output {
    use std::process::Stdio;
    use std::time::{Duration, Instant};
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let start = Instant::now();
    while child.try_wait().unwrap().is_none() {
        if start.elapsed() > Duration::from_secs(12) {
            let _ = child.kill();
            let output = child.wait_with_output().unwrap();
            panic!("command blocked: {output:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    child.wait_with_output().unwrap()
}

#[test]
fn agent_mode_rejects_prompts_even_with_terminal_stdin() {
    use std::os::fd::FromRawFd;
    let fixture = Fixture::new();
    let mut master = -1;
    let mut slave = -1;
    // SAFETY: openpty initializes these descriptors; each is adopted exactly once.
    assert_eq!(
        unsafe {
            libc::openpty(
                &mut master,
                &mut slave,
                std::ptr::null_mut(),
                std::ptr::null(),
                std::ptr::null(),
            )
        },
        0
    );
    let _master = unsafe { fs::File::from_raw_fd(master) };
    let slave = unsafe { fs::File::from_raw_fd(slave) };
    for args in [
        vec![
            "--non-interactive",
            "--json",
            "create",
            "--cloud",
            "vast.ai",
            "--ssh",
            "--max-price-per-hr",
            "1",
        ],
        vec![
            "--non-interactive",
            "--json",
            "create",
            "--cloud",
            "vast.ai",
            "--custom",
        ],
        vec!["--non-interactive", "--json", "login", "--cloud", "vast.ai"],
        vec!["--non-interactive", "--json", "login", "--cloud", "verda"],
        vec![
            "--non-interactive",
            "--json",
            "login",
            "--cloud",
            "verda",
            "--force",
        ],
        vec![
            "--non-interactive",
            "--json",
            "shell",
            "--cloud",
            "vast.ai",
            "123",
        ],
    ] {
        let mut command = fixture.command();
        command.args(args).stdin(slave.try_clone().unwrap());
        let value = failed(bounded_output(command));
        assert!(matches!(
            value["error"]["code"].as_str(),
            Some(
                "confirmation_required"
                    | "invalid_arguments"
                    | "authentication_required"
                    | "interaction_required"
            )
        ));
    }
    assert!(!fixture.root.path().join("config/ice/config.toml").exists());
}

#[test]
fn creation_defaults_are_visible_overridable_and_never_persisted() {
    let fixture = Fixture::new();
    let config = "[default.vast_ai]\nmin_cpus = 8\nmin_ram_gb = 32.0\nmax_price_per_hr = 0.6\ngpu_count = 2\nmin_download_mbps = 100.0\n";
    fixture.config(config);
    let value = failed(fixture.run(&[
        "create",
        "--cloud",
        "vast.ai",
        "--ssh",
        "--gpu-count",
        "1",
        "--min-cpus",
        "4",
        "--disk-gb",
        "80",
        "--dry-run",
        "--json",
        "--non-interactive",
    ]));
    assert_eq!(value["error"]["code"], "authentication_required");
    let filters = &value["error"]["details"]["selection"]["filters"];
    assert_eq!(
        filters["min_cpus"],
        json!({"value":4,"source":"command_line"})
    );
    assert_eq!(
        filters["min_ram_gb"],
        json!({"value":32.0,"source":"saved_configuration"})
    );
    assert_eq!(filters["gpu_count"]["value"], 1);
    assert_eq!(filters["disk_gb"]["value"], 80);
    assert_eq!(
        fs::read_to_string(fixture.root.path().join("config/ice/config.toml")).unwrap(),
        config
    );

    let value = failed(fixture.run(&[
        "create",
        "--cloud",
        "vast.ai",
        "--ssh",
        "--no-defaults",
        "--max-price-per-hr",
        "0.4",
        "--dry-run",
        "--json",
    ]));
    let selection = &value["error"]["details"]["selection"];
    assert_eq!(selection["saved_filters_ignored"], true);
    assert_eq!(selection["price_scope"], "compute_and_allocated_storage");
    assert!(selection["filters"]["min_cpus"]["value"].is_null());
    assert!(selection["filters"]["gpu_count"]["value"].is_null());
    assert_eq!(selection["filters"]["disk_gb"]["value"], 32);
    assert_eq!(
        fs::read_to_string(fixture.root.path().join("config/ice/config.toml")).unwrap(),
        config
    );
}

#[test]
fn agent_creation_reports_missing_conflicting_and_unsupported_inputs() {
    let fixture = Fixture::new();
    let value = failed(fixture.run(&["create", "--cloud", "vast.ai", "--ssh", "--yes", "--json"]));
    assert_eq!(value["error"]["code"], "missing_configuration");
    assert_eq!(
        value["error"]["details"]["required_flags"],
        json!(["--max-price-per-hr"])
    );
    let value = failed(fixture.run(&[
        "create",
        "--cloud",
        "vast.ai",
        "--ssh",
        "--min-ram-gb",
        "NaN",
        "--disk-gb",
        "0",
        "--max-price-per-hr",
        "0",
        "--json",
    ]));
    assert_eq!(value["error"]["code"], "invalid_arguments");
    assert_eq!(
        value["error"]["details"]["filters"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    let value = failed(fixture.run(&[
        "create",
        "--cloud",
        "vast.ai",
        "--ssh",
        "--gpu-count",
        "0",
        "--min-gpu-memory-gb",
        "24",
        "--max-price-per-hr",
        "1",
        "--json",
    ]));
    assert_eq!(value["error"]["code"], "invalid_arguments");
    let value = failed(fixture.run(&[
        "create",
        "--cloud",
        "gcp",
        "--ssh",
        "--min-download-mbps",
        "500",
        "--max-price-per-hr",
        "1",
        "--json",
    ]));
    assert_eq!(value["error"]["code"], "unsupported_filter");
    assert_eq!(
        value["error"]["details"]["filters"],
        json!(["min_download_mbps"])
    );
    assert!(!fixture.root.path().join("config/ice/config.toml").exists());
}

#[test]
fn new_filter_defaults_round_trip_through_config() {
    let fixture = Fixture::new();
    for (key, value) in [
        ("gpu_count", "0"),
        ("min_gpu_memory_gb", "24"),
        ("disk_gb", "80"),
        ("min_download_mbps", "500"),
        ("min_upload_mbps", "100"),
    ] {
        let key = format!("default.vast_ai.{key}");
        parsed(fixture.run(&["config", "set", &format!("{key}={value}"), "--json"]));
        let actual = parsed(fixture.run(&["config", "get", &key, "--json"]));
        assert_eq!(
            actual["result"]["value"].as_f64(),
            value.parse::<f64>().ok()
        );
        parsed(fixture.run(&["config", "unset", &key, "--json"]));
        let actual = parsed(fixture.run(&["config", "get", &key, "--json"]));
        assert!(actual["result"]["value"].is_null());
    }
}

#[test]
fn invalid_saved_filter_cannot_silently_turn_into_an_unrestricted_search() {
    let fixture = Fixture::new();
    fixture.config("[default.vast_ai]\nmin_ram_gb = nan\nmax_price_per_hr = 0.6\n");
    let value = failed(fixture.run(&[
        "create",
        "--cloud",
        "vast.ai",
        "--ssh",
        "--dry-run",
        "--json",
    ]));
    assert_eq!(value["error"]["code"], "invalid_arguments");
    assert_eq!(value["error"]["details"]["source"], "saved_configuration");
    let value = failed(fixture.run(&[
        "create",
        "--cloud",
        "vast.ai",
        "--ssh",
        "--min-ram-gb",
        "16",
        "--dry-run",
        "--json",
    ]));
    assert_eq!(value["error"]["code"], "authentication_required");
    fixture.config("[default.vast_ai]\nallowed_gpus = ['RTX 4090']\nmax_price_per_hr = 0.6\n");
    let value = failed(fixture.run(&[
        "create",
        "--cloud",
        "vast.ai",
        "--ssh",
        "--gpu-count",
        "0",
        "--no-gpu",
        "--dry-run",
        "--json",
    ]));
    assert_eq!(value["error"]["code"], "authentication_required");
    let filters = &value["error"]["details"]["selection"]["filters"];
    assert_eq!(filters["gpu_count"]["value"], 0);
    assert_eq!(filters["allowed_gpus"]["value"], json!([]));
}

#[test]
fn gcp_service_account_applies_to_resource_commands_and_returned_connection() {
    let fixture = Fixture::new();
    fixture.config("[auth.gcp]\nservice_account_json = '/fixture/key with spaces.json'\n");
    fixture.script("gcloud", r#"#!/bin/sh
[ "$1" != --version ] || exit 0
[ "$CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE" = '/fixture/key with spaces.json' ] || exit 80
case "$2" in
  instances) printf '[{"name":"ice-example","status":"RUNNING","zone":"zones/test-a","metadata":{"items":[{"key":"ice-workload-kind","value":"shell"}]}}]' ;;
  ssh) exit 0 ;;
  *) exit 81 ;;
esac
"#);
    let value = parsed(fixture.run(&[
        "shell",
        "--cloud",
        "gcp",
        "ice-example",
        "--print-creds",
        "--non-interactive",
        "--json",
    ]));
    let command = value["result"]["connect_command"].as_str().unwrap();
    assert!(command.starts_with("env "));
    assert!(
        command.contains("CLOUDSDK_AUTH_CREDENTIAL_FILE_OVERRIDE=/fixture/key with spaces.json")
    );
    assert!(command.contains("--quiet"));
}

#[test]
fn unattended_gcp_workflow_verifies_price_creates_connects_transfers_and_cleans_up() {
    let fixture = Fixture::new();
    fixture.gcp_creation();
    let base = [
        "--non-interactive",
        "--json",
        "create",
        "--cloud",
        "gcp",
        "--ssh",
        "--max-price-per-hr",
        "0.3",
        "--disk-gb",
        "80",
        "--hours",
        "0.25",
    ];
    let preview = parsed(fixture.run(&[base.as_slice(), &["--dry-run"]].concat()));
    assert_eq!(preview["result"]["cost"]["hourly_usd"], 0.2);
    assert_eq!(preview["result"]["allocated_disk_gb"], 80);
    assert!(!fixture.root.path().join("state.name").exists());
    let created = parsed(fixture.run(&[base.as_slice(), &["--yes"]].concat()));
    let id = created["result"]["instance"]["id"].as_str().unwrap();
    let connection = parsed(fixture.run(&[
        "--non-interactive",
        "--json",
        "shell",
        "--cloud",
        "gcp",
        id,
        "--print-creds",
    ]));
    assert!(
        connection["result"]["connect_command"]
            .as_str()
            .unwrap()
            .contains("BatchMode=yes")
    );
    assert!(
        connection["result"]["connect_command"]
            .as_str()
            .unwrap()
            .contains("--quiet")
    );
    let file = fixture.root.path().join("payload");
    fs::write(&file, "payload").unwrap();
    for args in [
        vec![
            "push",
            "--cloud",
            "gcp",
            id,
            file.to_str().unwrap(),
            "/tmp/payload",
            "--json",
        ],
        vec![
            "pull",
            "--cloud",
            "gcp",
            id,
            "/tmp/payload",
            file.to_str().unwrap(),
            "--json",
        ],
    ] {
        parsed(fixture.run(&args));
        let mut command = fixture.command();
        command.args(&args).env("ICE_TEST_TRANSFER_FAIL", "1");
        failed(bounded_output(command));
    }
    parsed(fixture.run(&["stop", "--cloud", "gcp", id, "--json"]));
    let stopped = failed(fixture.run(&["shell", "--cloud", "gcp", id, "--print-creds", "--json"]));
    assert_eq!(stopped["error"]["code"], "instance_stopped");
    parsed(fixture.run(&["start", "--cloud", "gcp", id, "--json"]));
    parsed(fixture.run(&["delete", "--cloud", "gcp", id, "--json"]));
    let actions = fs::read_to_string(fixture.root.path().join("actions")).unwrap();
    assert_eq!(
        actions
            .lines()
            .filter(|line| line.starts_with("compute instances create "))
            .count(),
        1
    );
    assert!(!fixture.root.path().join("config/ice/config.toml").exists());
}

#[test]
fn unattended_gcp_creation_rejects_repriced_offer_and_reports_timeout_resource() {
    let fixture = Fixture::new();
    fixture.gcp_creation();
    let rejected = failed(fixture.run(&[
        "create",
        "--cloud",
        "gcp",
        "--ssh",
        "--max-price-per-hr",
        "0.15",
        "--yes",
        "--json",
    ]));
    assert_eq!(rejected["error"]["code"], "no_matching_offers");
    assert!(!fixture.root.path().join("state.name").exists());
    let mut command = fixture.command();
    command
        .args([
            "create",
            "--cloud",
            "gcp",
            "--ssh",
            "--max-price-per-hr",
            "0.3",
            "--yes",
            "--startup-timeout",
            "1s",
            "--json",
        ])
        .env("ICE_TEST_CREATED_STATE", "PROVISIONING");
    let failed = failed(bounded_output(command));
    assert_eq!(failed["error"]["code"], "startup_timeout");
    let recovery = &failed["error"]["details"]["recovery"];
    assert_eq!(recovery["stage"], "waiting_for_startup");
    assert_eq!(
        recovery["instance_id"].as_str().unwrap(),
        fs::read_to_string(fixture.root.path().join("state.name")).unwrap()
    );
    assert_eq!(recovery["zone"], "test-a");
}

fn parsed(output: Output) -> Value {
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap_or_else(|error| {
        panic!(
            "stdout is not a single JSON document: {error}: {}",
            String::from_utf8_lossy(&output.stdout)
        )
    })
}

fn failed(output: Output) -> Value {
    assert!(!output.status.success());
    let value: Value = serde_json::from_slice(&output.stdout)
        .unwrap_or_else(|error| panic!("invalid error JSON: {error}: {:?}", output));
    assert!(value.get("result").is_none());
    assert!(value["error"]["message"].is_string());
    value
}

fn records(bytes: &[u8]) -> Vec<Value> {
    serde_json::Deserializer::from_slice(bytes)
        .into_iter::<Value>()
        .map(Result::unwrap)
        .collect()
}

#[test]
fn list_projects_provider_records_without_secrets_or_display_formatting() {
    let fixture = Fixture::new();
    fixture.provider(
        json!([{
            "name": "ice-example", "status": "RUNNING", "zone": "zones/us-central1-a",
            "machineType": "machineTypes/g2-standard-4",
            "metadata": {"items": [{"key": "startup-script", "value": "secret-startup-token"}]},
            "credentials": "secret-provider-token"
        }]),
        false,
    );
    let value = parsed(fixture.run(&["list", "--cloud", "gcp", "--json"]));
    assert_eq!(value["schema_version"], 2);
    assert_eq!(value["command"], "list");
    assert_eq!(value["cloud"], "gcp");
    assert_eq!(value["result"]["instances"][0]["id"], "ice-example");
    assert_eq!(value["result"]["instances"][0]["state"], "RUNNING");
    assert_eq!(value["result"]["instances"][0]["zone"], "us-central1-a");
    assert!(value["result"]["instances"][0]["created_at"].is_null());
    assert!(!value.to_string().contains("secret-"));
    assert!(!value.to_string().contains("color"));
}

#[test]
fn empty_list_is_a_success_but_provider_failure_has_no_success_output() {
    let fixture = Fixture::new();
    fixture.provider(json!([]), false);
    let empty = parsed(fixture.run(&["list", "--cloud", "gcp", "--json"]));
    assert_eq!(empty["result"]["instances"], json!([]));
    fixture.provider(json!([]), true);
    let failure = fixture.run(&["list", "--cloud", "gcp", "--json"]);
    let failure = failed(failure);
    assert_eq!(failure["error"]["code"], "command_failed");
}

#[test]
fn json_list_does_not_present_stale_cached_instances_as_current() {
    let fixture = Fixture::new();
    fixture.provider(
        json!([{"name":"ice-old", "status":"RUNNING", "zone":"zones/test-a"}]),
        false,
    );
    parsed(fixture.run(&["list", "--cloud", "gcp", "--json"]));
    fixture.provider(json!([]), true);
    let failure = fixture.run(&["list", "--cloud", "gcp", "--json"]);
    failed(failure);
}

#[test]
fn default_list_still_uses_human_output() {
    let fixture = Fixture::new();
    fixture.provider(json!([]), false);
    let output = fixture.run(&["list", "--cloud", "gcp"]);
    assert!(output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("No `ice`-managed instances"));
}

#[test]
fn connection_json_remains_parseable_when_readiness_probe_prints_output() {
    let fixture = Fixture::new();
    fixture.provider(
        json!([{
            "name": "ice-example", "status": "RUNNING", "zone": "zones/us-central1-a",
            "machineType": "machineTypes/g2-standard-4",
            "metadata": {"items": [{"key": "ice-workload-kind", "value": "shell"}]}
        }]),
        false,
    );
    let value = parsed(fixture.run(&[
        "shell",
        "--cloud",
        "gcp",
        "ice-example",
        "--print-creds",
        "--json",
    ]));
    assert_eq!(value["command"], "shell");
    assert_eq!(value["result"]["instance"]["id"], "ice-example");
    assert!(
        value["result"]["connect_command"]
            .as_str()
            .unwrap()
            .contains("gcloud")
    );
    assert!(
        value["result"]["connect_command"]
            .as_str()
            .unwrap()
            .contains("ice-example")
    );
}

#[test]
fn local_preview_is_json_without_a_container_runtime() {
    let fixture = Fixture::new();
    let value = parsed(fixture.run(&[
        "create",
        "--cloud",
        "local",
        "--unpack",
        "example-image:tag",
        "--hours",
        "0.25",
        "--dry-run",
        "--json",
    ]));
    assert_eq!(value["result"]["dry_run"], true);
    assert_eq!(value["result"]["requested_hours"], 0.25);
    assert_eq!(value["result"]["workload"]["kind"], "unpack");
    assert!(value["result"]["container_runtime"].is_null());
}

#[test]
fn json_cannot_start_an_interactive_shell() {
    let fixture = Fixture::new();
    let shell = fixture.run(&["shell", "--cloud", "vast.ai", "123", "--json"]);
    assert!(
        failed(shell)["error"]["message"]
            .as_str()
            .unwrap()
            .contains("--print-creds")
    );
}

#[test]
fn global_flag_works_at_every_subcommand_depth_and_config_is_typed_and_redacted() {
    let fixture = Fixture::new();
    let set = parsed(fixture.run(&["--json", "config", "set", "default.gcp.min_cpus=12"]));
    assert_eq!(set["result"]["value"], 12);
    assert!(set["cloud"].is_null());
    let get = parsed(fixture.run(&["config", "--json", "get", "default.gcp.min_cpus"]));
    assert_eq!(get["result"]["value"], 12);
    for key in [
        "auth.vast_ai.api_key",
        "auth.aws.access_key_id",
        "auth.aws.secret_access_key",
    ] {
        let pair = format!("{key}=secret-test-credential");
        let result = parsed(fixture.run(&["config", "set", &pair, "--json"]));
        assert_eq!(result["result"]["value"], "<redacted>");
        assert!(!result.to_string().contains("secret-test-credential"));
    }
    parsed(fixture.run(&["config", "set", "default.runtime_hours=0.123456", "--json"]));
    let result = parsed(fixture.run(&["config", "list", "--json"]));
    assert_eq!(
        result["result"]["values"]["default.runtime_hours"],
        0.123456
    );
    assert!(result["result"]["values"]["default.aws.min_cpus"].is_null());
    assert!(!result.to_string().contains("secret-test-credential"));
    let unset = parsed(fixture.run(&["--json", "config", "unset", "default.gcp.min_cpus"]));
    assert!(unset["result"]["value"].is_null());
    let get = parsed(fixture.run(&["--json", "config", "get", "default.gcp.min_cpus"]));
    assert!(get["result"]["value"].is_null());
}

#[test]
fn argument_and_command_errors_are_json_and_keep_nonzero_exit_status() {
    let fixture = Fixture::new();
    let invalid = failed(fixture.run(&["--json", "start"]));
    assert_eq!(invalid["error"]["code"], "invalid_arguments");
    let invalid = failed(fixture.run(&[
        "config",
        "set",
        "--json",
        "auth.vast_ai.api_key=secret-value",
        "extra",
    ]));
    assert!(!invalid.to_string().contains("secret-value"));
    let unknown = failed(fixture.run(&["config", "get", "unknown.key", "--json"]));
    assert_eq!(unknown["command"], "config get");
    assert_eq!(unknown["error"]["code"], "command_failed");
    failed(fixture.run(&["refresh-catalog", "--cloud", "local", "--json"]));
}

#[test]
fn malformed_config_does_not_echo_credentials_in_json_error() {
    let fixture = Fixture::new();
    let dir = fixture.root.path().join("config/ice");
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("config.toml"),
        "[auth.vast_ai]\napi_key = 'secret-token' trailing-garbage",
    )
    .unwrap();
    let error = failed(fixture.run(&["config", "list", "--json"]));
    assert!(!error.to_string().contains("secret-token"));
}

#[test]
fn login_captures_provider_output_and_returns_only_readiness_metadata() {
    let fixture = Fixture::new();
    fixture.script("aws", "#!/bin/sh\nprintf 'raw-provider-identity\\n'\n");
    let value = parsed(fixture.run(&["login", "--cloud", "aws", "--json"]));
    assert_eq!(value["result"]["status"], "ready");
    assert_eq!(value["result"]["method"], "auto_detected");
    assert!(!value.to_string().contains("raw-provider"));
    fixture.runtime();
    let local = parsed(fixture.run(&["--json", "login", "--cloud", "local"]));
    assert_eq!(local["result"]["status"], "ready");
}

#[test]
fn lifecycle_reports_actual_state_and_distinguishes_noop() {
    let fixture = Fixture::new();
    fixture.runtime();
    let args = |command| [command, "--cloud", "local", "ice-example", "--json"];
    let unchanged = parsed(fixture.run(&args("start")));
    assert_eq!(unchanged["result"]["request"], "not_needed");
    let reads = fs::read_to_string(fixture.root.path().join("actions")).unwrap();
    assert_eq!(reads.lines().filter(|line| *line == "ps").count(), 1);
    assert_eq!(reads.lines().filter(|line| *line == "inspect").count(), 1);
    let stopped = parsed(fixture.run(&args("stop")));
    assert_eq!(stopped["result"]["request"], "acknowledged");
    assert_eq!(stopped["result"]["instance"]["state"], "stopped");
    let stopped = parsed(fixture.run(&args("stop")));
    assert_eq!(stopped["result"]["request"], "not_needed");
    let started = parsed(fixture.run(&args("start")));
    assert_eq!(started["result"]["request"], "acknowledged");
    assert_eq!(started["result"]["instance"]["state"], "running");
    let deleted = parsed(fixture.run(&args("delete")));
    assert_eq!(deleted["result"]["state"], "deleted");
    assert_eq!(deleted["result"]["outcome"], "verified");
    assert_eq!(deleted["result"]["verification"], "read_back");
    assert_eq!(deleted["result"]["instance_id"], "test-id");
    let actions = fs::read_to_string(fixture.root.path().join("actions")).unwrap();
    assert_eq!(actions.lines().filter(|line| *line == "stop").count(), 2);
    assert_eq!(actions.lines().filter(|line| *line == "start").count(), 1);
    assert_eq!(actions.lines().filter(|line| *line == "rm").count(), 1);
}

#[test]
fn managed_creation_finishes_deployment_before_emitting_json() {
    let fixture = Fixture::new();
    fixture.runtime();
    fixture.script(
        "gcloud",
        r#"#!/bin/sh
if [ "$2" = list ]; then printf 'test@example.invalid\n';
else printf '{"token":"secret-registry-token"}\n'; fi
"#,
    );
    let value = parsed(fixture.run(&[
        "create",
        "--cloud",
        "local",
        "--container",
        "us-central1-docker.pkg.dev/test/repo/image:tag",
        "--hours",
        "0.25",
        "--json",
    ]));
    assert_eq!(value["result"]["status"], "created");
    assert_eq!(value["result"]["instance"]["id"], "test-id");
    assert!(!value.to_string().contains("secret-registry-token"));
    let actions = fs::read_to_string(fixture.root.path().join("actions")).unwrap();
    for action in ["login", "pull", "run", "inspect"] {
        assert!(
            actions.lines().any(|line| line == action),
            "missing {action}: {actions}"
        );
    }
    assert!(!actions.lines().any(|line| matches!(line, "exec" | "logs")));
}

#[test]
fn credential_lookup_errors_do_not_expose_raw_token_responses() {
    let fixture = Fixture::new();
    fixture.runtime();
    fixture.script("gcloud", "#!/bin/sh\nprintf 'secret-registry-token\\n'\n");
    let value = failed(fixture.run(&[
        "create",
        "--cloud",
        "local",
        "--container",
        "us-central1-docker.pkg.dev/test/repo/image:tag",
        "--json",
    ]));
    assert!(!value.to_string().contains("secret-registry-token"));
    assert!(
        value["error"]["message"]
            .as_str()
            .unwrap()
            .contains("registry credentials")
    );
}

#[test]
fn closed_json_pipe_stops_a_log_transport_with_child_processes() {
    use std::io::{BufRead, BufReader};
    use std::process::Stdio;
    use std::time::{Duration, Instant};

    let fixture = Fixture::new();
    fixture.provider(
        json!([{"name":"ice-example", "status":"RUNNING", "zone":"zones/test-a",
        "metadata":{"items":[{"key":"ice-workload-kind","value":"unpack"},
            {"key":"ice-workload-source","value":"example:tag"}]}}]),
        false,
    );
    fixture.script(
        "gcloud",
        r#"#!/bin/sh
if [ "$2" = ssh ]; then
    printf 'first'
    while [ ! -f "$ICE_TEST_STATE.release" ]; do /bin/sleep 0.01; done
    /bin/sh -c 'printf second; exec /bin/sleep 30' &
    wait
else
    /bin/cat "$ICE_TEST_RESPONSE"
fi
"#,
    );
    let mut child = fixture
        .command()
        .args([
            "logs",
            "--cloud",
            "gcp",
            "ice-example",
            "--follow",
            "--json",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&line).unwrap()["result"]["text"],
        "first"
    );
    drop(stdout);
    fs::write(fixture.root.path().join("state.release"), "continue").unwrap();
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert!(!status.success());
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("log transport kept the JSON command alive after stdout closed");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn connection_and_provider_log_flags_reject_unsupported_uses_before_provider_access() {
    let fixture = Fixture::new();
    for args in [
        vec!["shell", "--cloud", "vast.ai", "42", "--no-probe", "--json"],
        vec![
            "shell",
            "--cloud",
            "vast.ai",
            "42",
            "--print-creds",
            "--no-probe",
            "--preserve-ephemeral",
            "--json",
        ],
        vec![
            "shell",
            "--cloud",
            "gcp",
            "42",
            "--print-creds",
            "--no-probe",
            "--json",
        ],
        vec!["logs", "--cloud", "gcp", "42", "--provider-logs", "--json"],
    ] {
        let output = fixture.run(&args);
        assert!(!output.status.success(), "{args:?}");
        let value: Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(value["error"]["code"], "invalid_arguments", "{args:?}");
    }
    let output = fixture.run(&["shell", "--help"]);
    assert!(output.status.success());
    assert!(
        String::from_utf8(output.stdout)
            .unwrap()
            .contains("--no-probe")
    );
}

#[test]
fn interrupting_json_logs_stops_the_transport() {
    use std::io::{BufRead, BufReader, Read};
    use std::process::Stdio;
    use std::time::{Duration, Instant};

    let fixture = Fixture::new();
    fixture.provider(
        json!([{"name":"ice-example", "status":"RUNNING", "zone":"zones/test-a",
        "metadata":{"items":[{"key":"ice-workload-kind","value":"unpack"},
            {"key":"ice-workload-source","value":"example:tag"}]}}]),
        false,
    );
    fixture.script(
        "gcloud",
        r#"#!/bin/sh
if [ "$2" = ssh ]; then
    printf 'first'
    /bin/sleep 30 &
    wait
else
    /bin/cat "$ICE_TEST_RESPONSE"
fi
"#,
    );
    let mut child = fixture
        .command()
        .args([
            "logs",
            "--cloud",
            "gcp",
            "ice-example",
            "--follow",
            "--json",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&line).unwrap()["result"]["text"],
        "first"
    );
    // This is the test's own child, which remains unreaped throughout the call.
    assert!(
        Command::new("/bin/kill")
            .args(["-INT", &child.id().to_string()])
            .status()
            .unwrap()
            .success()
    );
    let start = Instant::now();
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert_eq!(status.code(), Some(130));
            break;
        }
        if start.elapsed() > Duration::from_secs(5) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("interrupted log transport kept the command alive");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let mut remaining = String::new();
    stdout.read_to_string(&mut remaining).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&remaining).unwrap()["error"]["code"],
        "command_failed"
    );
}

#[test]
fn log_output_is_flushed_before_the_stream_finishes() {
    use std::io::{BufRead, BufReader};
    use std::process::Stdio;
    use std::sync::mpsc;
    use std::time::Duration;

    let fixture = Fixture::new();
    fixture.provider(
        json!([{"name":"ice-example", "status":"RUNNING", "zone":"zones/test-a",
        "metadata":{"items":[{"key":"ice-workload-kind","value":"unpack"},
            {"key":"ice-workload-source","value":"example:tag"}]}}]),
        false,
    );
    // Block on a file after printing the first chunk. The test releases it only
    // after receiving JSON, proving follow does not buffer until process exit.
    fixture.script(
        "gcloud",
        r#"#!/bin/sh
if [ "$2" = ssh ]; then
    printf 'first chunk'
    while [ ! -f "$ICE_TEST_STATE" ]; do /bin/sleep 0.01; done
    printf 'last chunk'
else
    /bin/cat "$ICE_TEST_RESPONSE"
fi
"#,
    );
    let mut child = fixture
        .command()
        .args([
            "logs",
            "--cloud",
            "gcp",
            "ice-example",
            "--follow",
            "--json",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines() {
            if sender.send(line.unwrap()).is_err() {
                break;
            }
        }
    });
    let first = receiver.recv_timeout(Duration::from_secs(10));
    if first.is_err() {
        fs::write(fixture.root.path().join("state"), "continue").unwrap();
        child.kill().unwrap();
        child.wait().unwrap();
        reader.join().unwrap();
        panic!("JSON stream did not flush its first record");
    }
    let first: Value = serde_json::from_str(&first.unwrap()).unwrap();
    assert_eq!(first["result"]["text"], "first chunk");
    assert!(child.try_wait().unwrap().is_none());
    fs::write(fixture.root.path().join("state"), "continue").unwrap();
    assert!(child.wait().unwrap().success());
    reader.join().unwrap();
    let remaining: Vec<Value> = receiver
        .into_iter()
        .map(|line| serde_json::from_str(&line).unwrap())
        .collect();
    assert_eq!(remaining.last().unwrap()["result"]["event"], "complete");
}

#[test]
fn transfers_report_completion_only_after_copy_succeeds() {
    let fixture = Fixture::new();
    let dir = fixture.unpack(b"", None);
    fixture.script("cp", "#!/bin/sh\nexec /bin/cp \"$@\"\n");
    fs::write(dir.join("rootfs/source.txt"), "payload").unwrap();
    let destination = fixture.root.path().join("download.txt");
    let value = parsed(fixture.run(&[
        "pull",
        "--cloud",
        "local",
        "ice-example",
        "source.txt",
        destination.to_str().unwrap(),
        "--json",
    ]));
    assert_eq!(value["result"]["status"], "completed");
    assert_eq!(fs::read_to_string(&destination).unwrap(), "payload");
    let value = parsed(fixture.run(&[
        "--json",
        "push",
        "--cloud",
        "local",
        "ice-example",
        destination.to_str().unwrap(),
        "upload.txt",
    ]));
    assert_eq!(value["result"]["status"], "completed");
    assert_eq!(
        fs::read_to_string(dir.join("rootfs/upload.txt")).unwrap(),
        "payload"
    );
    failed(fixture.run(&[
        "pull",
        "--cloud",
        "local",
        "ice-example",
        "missing.txt",
        "--json",
    ]));
}

#[test]
fn logs_stream_both_process_pipes_as_json_lines() {
    let fixture = Fixture::new();
    fixture.runtime();
    let output = fixture.run(&[
        "logs",
        "--cloud",
        "local",
        "ice-example",
        "--follow",
        "--json",
    ]);
    assert!(output.status.success(), "{:?}", output);
    let records = records(&output.stdout);
    for stream in ["stdout", "stderr"] {
        let text: String = records
            .iter()
            .filter(|record| record["result"]["stream"] == stream)
            .map(|record| record["result"]["text"].as_str().unwrap())
            .collect();
        assert_eq!(
            text,
            if stream == "stdout" {
                "stdout 🦀\n"
            } else {
                "stderr 終"
            }
        );
    }
    assert_eq!(records.last().unwrap()["result"]["event"], "complete");
}

#[test]
fn unpack_logs_include_exit_status_without_invented_text() {
    let fixture = Fixture::new();
    fixture.unpack("first\nlast 🦀".as_bytes(), Some(7));
    let output = fixture.run(&[
        "--json",
        "logs",
        "--cloud",
        "local",
        "ice-example",
        "--tail",
        "1",
        "--follow",
    ]);
    assert!(output.status.success(), "{:?}", output);
    let records = records(&output.stdout);
    let text: String = records
        .iter()
        .filter_map(|record| record["result"]["text"].as_str())
        .collect();
    assert!(text.ends_with("last 🦀"));
    assert_eq!(
        records[records.len() - 2]["result"]["event"],
        "workload_exit"
    );
    assert_eq!(records[records.len() - 2]["result"]["exit_code"], 7);
    assert_eq!(records.last().unwrap()["result"]["event"], "complete");
}

#[test]
fn remote_log_failure_keeps_partial_records_and_ends_with_error() {
    let fixture = Fixture::new();
    fixture.provider(
        json!([{"name":"ice-example", "status":"RUNNING", "zone":"zones/test-a",
        "metadata":{"items":[{"key":"ice-workload-kind","value":"unpack"},
            {"key":"ice-workload-source","value":"example:tag"}]}}]),
        false,
    );
    fixture.script("gcloud", "#!/bin/sh\nif [ \"$2\" = ssh ]; then printf 'partial 🦀'; exit 7; fi\n/bin/cat \"$ICE_TEST_RESPONSE\"\n");
    let output = fixture.run(&["logs", "--cloud", "gcp", "ice-example", "--json"]);
    assert!(!output.status.success());
    let records = records(&output.stdout);
    assert!(
        records
            .iter()
            .any(|record| record["result"]["text"] == "partial 🦀")
    );
    assert_eq!(records.last().unwrap()["error"]["code"], "command_failed");
    assert!(
        !records
            .iter()
            .any(|record| record["result"]["event"] == "complete")
    );
}

#[test]
fn verda_missing_credentials_and_cleanup_are_structured() {
    let fixture = Fixture::new();
    for command in ["login", "list", "catalog"] {
        let value =
            failed(fixture.run(&[command, "--cloud", "verda", "--non-interactive", "--json"]));
        assert_eq!(value["cloud"], "verda");
        assert_eq!(
            value["error"]["code"],
            if command == "login" {
                "authentication_required"
            } else {
                "missing_credentials"
            }
        );
    }
    let value = failed(fixture.run(&[
        "create",
        "--cloud",
        "verda",
        "--ssh",
        "--yes",
        "--max-price-per-hr",
        "1",
        "--json",
    ]));
    assert_eq!(value["error"]["code"], "cleanup_acknowledgement_required");
    assert_eq!(value["error"]["details"]["automatic_delete"], false);
}

#[test]
fn verda_partial_environment_never_mixes_with_saved_credentials() {
    let fixture = Fixture::new();
    let saved = "[auth.verda]\nclient_id = 'saved-id'\nclient_secret = 'saved-secret'\n";
    fixture.config(saved);
    for variable in ["VERDA_CLIENT_ID", "VERDA_CLIENT_SECRET"] {
        for force in [false, true] {
            let mut command = fixture.command();
            command.env(variable, "environment-value").args([
                "login",
                "--cloud",
                "verda",
                "--json",
                "--non-interactive",
            ]);
            if force {
                command.arg("--force");
            }
            let output = bounded_output(command);
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
            for secret in ["saved-id", "saved-secret", "environment-value"] {
                assert!(!text.contains(secret));
            }
            let value = failed(output);
            assert_eq!(value["error"]["code"], "missing_credentials");
            assert_eq!(
                value["error"]["details"]["environment"],
                json!(["VERDA_CLIENT_ID", "VERDA_CLIENT_SECRET"])
            );
            assert_eq!(
                fs::read_to_string(fixture.root.path().join("config/ice/config.toml")).unwrap(),
                saved
            );
        }
    }
}

#[test]
fn forced_noninteractive_login_preserves_saved_credentials() {
    for cloud in ["vast.ai", "verda"] {
        let fixture = Fixture::new();
        let saved = "[auth.vast_ai]\napi_key = 'old-key'\n[auth.verda]\nclient_id = 'saved-id'\nclient_secret = 'saved-secret'\n";
        fixture.config(saved);
        let value = failed(fixture.run(&[
            "login",
            "--cloud",
            cloud,
            "--force",
            "--non-interactive",
            "--json",
        ]));
        assert_eq!(value["error"]["code"], "authentication_required");
        assert_eq!(
            fs::read_to_string(fixture.root.path().join("config/ice/config.toml")).unwrap(),
            saved
        );
    }
}

#[test]
fn verda_unsupported_workloads_fail_before_credentials_or_rentals() {
    let fixture = Fixture::new();
    for mode in [
        vec!["--container", "example.invalid/image"],
        vec!["--unpack", "image.tar"],
        vec!["--arca", "fixture"],
    ] {
        let mut args = vec!["create", "--cloud", "verda", "--dry-run", "--json"];
        args.extend(mode);
        let value = failed(fixture.run(&args));
        assert_eq!(value["error"]["code"], "unsupported_workload");
    }
}

#[test]
fn verda_filters_and_provider_options_preserve_provenance() {
    let fixture = Fixture::new();
    fixture.config("[default.verda]\nmin_cpus = 10\nmin_ram_gb = 32.0\ndisk_gb = 200\nimage = 'saved-image'\nlocation = 'FIN-01'\nssh_key_id = 'saved-key'\n");
    let value = failed(fixture.run(&[
        "create",
        "--cloud",
        "verda",
        "--ssh",
        "--dry-run",
        "--json",
        "--no-defaults",
        "--gpu-count",
        "1",
        "--min-gpu-memory-gb",
        "48",
        "--gpu",
        "Future GPU Model",
        "--max-price-per-hr",
        "1",
        "--image",
        "ubuntu-26.04-cuda-13.2-open",
    ]));
    assert_eq!(value["error"]["code"], "missing_credentials");
    let selection = &value["error"]["details"]["selection"];
    assert_eq!(selection["filters"]["disk_gb"]["value"], 100);
    assert_eq!(selection["filters"]["min_cpus"]["value"], Value::Null);
    assert_eq!(selection["filters"]["gpu_count"]["value"], 1);
    assert_eq!(selection["price_scope"], "compute_and_os_storage");
    assert_eq!(
        selection["provider_options"]["rental_type"],
        json!({"value":"on_demand", "source":"built_in"})
    );
    assert_eq!(
        selection["provider_options"]["image"]["source"],
        "command_line"
    );
    assert_eq!(
        selection["provider_options"]["location"]["value"],
        Value::Null
    );
    assert_eq!(
        selection["provider_options"]["ssh_key_id"]["value"],
        "saved-key"
    );
    let unsupported = failed(fixture.run(&[
        "create",
        "--cloud",
        "verda",
        "--ssh",
        "--dry-run",
        "--json",
        "--min-download-mbps",
        "100",
        "--max-price-per-hr",
        "1",
    ]));
    assert_eq!(unsupported["error"]["code"], "unsupported_filter");
}

#[test]
fn verda_configuration_round_trips_and_redacts_credentials() {
    let fixture = Fixture::new();
    for assignment in [
        "default.cloud=verda",
        "default.verda.gpu_count=1",
        "default.verda.disk_gb=100",
        "default.verda.max_price_per_hr=0.8",
        "default.verda.allowed_gpus=RTX A6000,H100",
        "default.verda.location=FIN-01",
        "auth.verda.client_id=verda-test-id",
        "auth.verda.client_secret=verda-test-secret",
    ] {
        let output = fixture.run(&["config", "set", assignment, "--json"]);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        assert!(!String::from_utf8_lossy(&output.stdout).contains("verda-test-secret"));
    }
    let config_path = fixture.root.path().join("config/ice/config.toml");
    assert_eq!(
        fs::metadata(config_path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let output = fixture.run(&["config", "list", "--json"]);
    assert!(output.status.success());
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        value["result"]["values"]["auth.verda.client_id"],
        "<redacted>"
    );
    assert_eq!(
        value["result"]["values"]["auth.verda.client_secret"],
        "<redacted>"
    );
    assert_eq!(
        value["result"]["values"]["default.verda.allowed_gpus"],
        json!(["RTX A6000", "H100"])
    );
    assert!(
        fixture
            .run(&["config", "unset", "auth.verda.client_secret", "--json"])
            .status
            .success()
    );
    assert!(
        !fixture
            .run(&["config", "set", "default.verda.disk_gb=0", "--json"])
            .status
            .success()
    );
}

#[test]
fn verda_options_do_not_silently_change_other_providers() {
    let fixture = Fixture::new();
    let value = failed(fixture.run(&[
        "create",
        "--cloud",
        "vast.ai",
        "--ssh",
        "--dry-run",
        "--image",
        "example",
        "--max-price-per-hr",
        "1",
        "--json",
    ]));
    assert_eq!(value["error"]["code"], "unsupported_arguments");
}

#[test]
fn lifecycle_delete_reconciles_once_and_does_not_confuse_gcp_stopped_with_deleted() {
    for scenario in ["gone", "lost-receipt", "stopped", "malformed", "wrong-id"] {
        let fixture = Fixture::new();
        fixture.script(
            "gcloud",
            r#"#!/bin/sh
[ "$1" != --version ] || exit 0
printf '%s\n' "$*" >> "$ICE_TEST_ACTIONS"
case "$1:$2:$3" in
 compute:instances:list)
  if [ ! -f "$ICE_TEST_STATE" ]; then
    printf '[{"name":"ice-test","zone":"zones/test-a","status":"RUNNING"}]'
  else
    case "$ICE_TEST_SCENARIO" in
      gone|lost-receipt) printf '[]' ;;
      stopped) printf '[{"name":"ice-test","zone":"zones/test-a","status":"TERMINATED"}]' ;;
      malformed) printf '{}' ;;
      wrong-id) printf '[{"name":"ice-other","zone":"zones/test-a","status":"TERMINATED"}]' ;;
    esac
  fi ;;
 compute:instances:delete)
  printf deleted > "$ICE_TEST_STATE"
  if [ "$ICE_TEST_SCENARIO" = lost-receipt ]; then printf secret-token >&2; exit 7; fi ;;
 *) exit 71 ;;
esac
"#,
        );
        let output = bounded_output({
            let mut cmd = fixture.command();
            cmd.env("ICE_TEST_SCENARIO", scenario).args([
                "delete",
                "--cloud",
                "gcp",
                "ice-test",
                "--timeout",
                "1s",
                "--json",
            ]);
            cmd
        });
        if matches!(scenario, "gone" | "lost-receipt") {
            let value = parsed(output);
            assert_eq!(value["result"]["state"], "deleted");
            assert_eq!(value["result"]["outcome"], "verified");
            assert_eq!(
                value["result"]["request"],
                if scenario == "gone" {
                    "acknowledged"
                } else {
                    "unconfirmed"
                }
            );
            assert_eq!(value["result"]["storage"]["verification"], "unknown");
        } else {
            let value = failed(output);
            assert_eq!(value["error"]["code"], "operation_unverified");
            assert_eq!(value["error"]["details"]["instance_id"], "ice-test");
            assert!(!value.to_string().contains("secret-token"));
        }
        let calls = fs::read_to_string(fixture.root.path().join("actions")).unwrap();
        assert_eq!(
            calls
                .lines()
                .filter(|l| l.contains("instances delete"))
                .count(),
            1
        );
        assert!(!calls.contains("instances stop"));
        assert!(calls.contains("--zones test-a --filter name=ice-test"));
    }
}

#[test]
fn lifecycle_timeout_also_bounds_cli_availability_checks() {
    let fixture = Fixture::new();
    fixture.script("aws", "#!/bin/sh\nexec /bin/sleep 30\n");
    let start = std::time::Instant::now();
    let value = failed(bounded_output({
        let mut cmd = fixture.command();
        cmd.args([
            "delete",
            "--cloud",
            "aws",
            "i-test",
            "--timeout",
            "1s",
            "--json",
        ]);
        cmd
    }));
    assert_eq!(value["error"]["code"], "operation_timeout");
    assert_eq!(
        value["error"]["details"]["recovery"]["instance_identifier"],
        "i-test"
    );
    assert_eq!(value["error"]["details"]["recovery"]["request"], "not_sent");
    assert!(start.elapsed() < std::time::Duration::from_secs(4));
}

#[test]
fn verda_requires_an_explicit_image_before_any_provider_request() {
    let fixture = Fixture::new();
    let value = failed(fixture.run(&[
        "create",
        "--cloud",
        "verda",
        "--ssh",
        "--max-price-per-hr",
        "1",
        "--dry-run",
        "--json",
    ]));
    assert_eq!(value["error"]["code"], "image_required");
    assert_eq!(value["error"]["details"]["flag"], "--image");
}

#[test]
fn aws_termination_waits_for_terminal_state_and_survives_removed_tags() {
    for scenario in ["terminated", "shutting-down", "malformed"] {
        let fixture = Fixture::new();
        fixture.config("[default.aws]\nregion = 'test-region'\n");
        fixture.script("aws", r#"#!/bin/sh
[ "$1" != --version ] || exit 0
printf '%s\n' "$*" >> "$ICE_TEST_ACTIONS"
case "$1:$2" in
 ec2:describe-instances)
  if [ ! -f "$ICE_TEST_STATE" ]; then
   printf '{"Reservations":[{"Instances":[{"InstanceId":"i-test","State":{"Name":"running"},"Tags":[{"Key":"Name","Value":"ice-test"}]}]}]}'
  elif [ "$ICE_TEST_SCENARIO" = malformed ]; then printf '{}'
  else
   printf '{"Reservations":[{"Instances":[{"InstanceId":"i-test","State":{"Name":"%s"}}]}]}' "$ICE_TEST_SCENARIO"
  fi ;;
 ec2:terminate-instances) printf accepted > "$ICE_TEST_STATE"; printf '{"TerminatingInstances":[{"CurrentState":{"Name":"shutting-down"}}]}' ;;
 *) exit 71 ;;
esac
"#);
        let output = bounded_output({
            let mut cmd = fixture.command();
            cmd.env("ICE_TEST_SCENARIO", scenario).args([
                "delete",
                "--cloud",
                "aws",
                "i-test",
                "--timeout",
                "1s",
                "--json",
            ]);
            cmd
        });
        if scenario == "terminated" {
            let value = parsed(output);
            assert_eq!(value["result"]["state"], "deleted");
            assert_eq!(value["result"]["instance"]["state"], "terminated");
        } else {
            let value = failed(output);
            assert_eq!(value["error"]["code"], "operation_unverified");
            assert_eq!(value["error"]["details"]["instance_id"], "i-test");
        }
        let calls = fs::read_to_string(fixture.root.path().join("actions")).unwrap();
        assert_eq!(
            calls
                .lines()
                .filter(|l| l.contains("terminate-instances"))
                .count(),
            1
        );
        assert!(!calls.contains("stop-instances"));
        assert!(calls.contains("Name=instance-id,Values=i-test"));
    }
}

#[test]
fn version_is_json_capable_and_independent_of_credentials_tools_and_config() {
    let fixture = Fixture::new();
    // Invalid credentials/configuration must not hide the identity of this binary.
    fixture.config("broken = [ 'secret-placeholder'\n");
    for args in [
        vec!["version", "--json"],
        vec!["--json", "--version"],
        vec!["--version", "--json"],
    ] {
        let value = parsed(fixture.run(&args));
        assert_eq!(value["command"], "version");
        assert_eq!(value["cloud"], Value::Null);
        assert_eq!(value["result"]["version"], env!("CARGO_PKG_VERSION"));
        assert_eq!(
            value["result"]["json_schema_version"],
            value["schema_version"]
        );
        let expected_revision = option_env!("ICE_BUILD_REVISION");
        assert_eq!(value["result"]["source_revision"], json!(expected_revision));
        assert!(!value.to_string().contains("secret-placeholder"));
    }
    for args in [vec!["version"], vec!["--version"], vec!["-V"]] {
        let output = fixture.run(&args);
        assert!(output.status.success());
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(text.starts_with(&format!("ice {} (", env!("CARGO_PKG_VERSION"))));
        assert!(text.contains("JSON schema 2"));
        assert!(output.stderr.is_empty());
    }
    assert!(!fixture.root.path().join(".ice").exists());
}

#[test]
fn verda_creation_reports_local_key_setup_errors_before_credentials_or_catalog_access() {
    let id = "44444444-4444-4444-8444-444444444444";
    for (setting, expected) in [
        ("".to_owned(), "ssh_key_required"),
        ("ssh_key_id = ''".into(), "ssh_key_required"),
        ("ssh_key_id = 'invalid'".into(), "invalid_ssh_key_id"),
        (
            format!("ssh_key_id = '{id}'\nssh_key_path = '/ice-test-missing-key'"),
            "ssh_identity_unavailable",
        ),
    ] {
        let fixture = Fixture::new();
        fixture.config(&format!("[default.verda]\n{setting}\n"));
        let value = failed(fixture.run(&[
            "create",
            "--cloud",
            "verda",
            "--ssh",
            "--image",
            "provider-image",
            "--max-price-per-hr",
            "1",
            "--yes",
            "--manual-cleanup",
            "--json",
        ]));
        assert_eq!(value["error"]["code"], expected);
        assert_eq!(value["error"]["details"]["resource_created"], false);
        let preview = failed(fixture.run(&[
            "create",
            "--cloud",
            "verda",
            "--ssh",
            "--image",
            "provider-image",
            "--max-price-per-hr",
            "1",
            "--dry-run",
            "--json",
        ]));
        assert_eq!(
            preview["error"]["code"], "missing_credentials",
            "preview should not require a usable SSH key"
        );
    }
}
