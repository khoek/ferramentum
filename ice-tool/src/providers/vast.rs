use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::blocking::{Client, RequestBuilder};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::cache::{CloudCacheModel, load_cache_store, persist_instances, upsert_instance};
use crate::cli::{CreateArgs, DownloadArgs, LogsArgs, ShellArgs};
use crate::http_retry;
use crate::listing::{
    ListedInstance, display_name_or_fallback, display_state, list_state_color,
    listed_instance as base_listed_instance, present_field, push_field, show_health_field,
};
use crate::model::{Cloud, IceConfig};
use crate::providers::{
    CloudInstance, CloudProvider, CommandProvider, CreateProvider, RemoteCloudProvider,
};
use crate::provision::{
    apply_vast_autostop_cost_estimate, build_accept_prompt, build_search_requirements,
    build_vast_autostop_plan, ensure_default_create_config, estimate_runtime_cost,
    find_cheapest_offer, load_gpu_options, print_offer_summary, prompt_adjust_search_filters,
    prompt_create_search_filters, prompt_offer_decision,
};
use crate::remote::{
    RemoteAccess, discover_local_ssh_keypair, run_rsync_download, run_rsync_upload,
};
use crate::support::{
    ICE_LABEL_PREFIX, VAST_DEFAULT_DISK_GB, VAST_DEFAULT_IMAGE,
    VAST_LOG_READY_POLL_INTERVAL_MILLIS, VAST_LOG_READY_TIMEOUT_SECS, VAST_POLL_INTERVAL_SECS,
    VAST_WAIT_TIMEOUT_SECS, build_cloud_instance_name, elapsed_since, extract_api_error_message,
    format_unix_utc, now_unix_secs, now_unix_secs_f64, parse_json_response, prefix_lookup_indices,
    prompt_confirm, spinner, truncate_ellipsis, visible_instance_name,
};
use crate::ui::{print_stage, print_warning};
use crate::unpack::{
    materialize_unpack_bundle, remote_unpack_dir_for_vast, unpack_logs_remote_command,
    unpack_prepare_remote_dir_command, unpack_start_remote_command,
};
use crate::workload::{
    ContainerImageReference, InstanceWorkload, display_unpack_source, resolve_deploy_hours,
    resolve_deploy_workload, workload_display_value,
};

const VAST_BASE_URL: &str = "https://console.vast.ai";

#[derive(Debug, Deserialize)]
struct VastOffersResponse {
    #[serde(default)]
    offers: Vec<VastOffer>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct VastOffer {
    pub(crate) id: u64,
    #[serde(default)]
    pub(crate) gpu_name: Option<String>,
    #[serde(default)]
    pub(crate) num_gpus: Option<u32>,
    #[serde(default)]
    pub(crate) cpu_cores_effective: Option<f64>,
    #[serde(default)]
    pub(crate) cpu_ram: Option<f64>,
    #[serde(default)]
    pub(crate) dph_total: Option<f64>,
    #[serde(default)]
    pub(crate) reliability: Option<f64>,
    #[serde(default)]
    pub(crate) duration: Option<f64>,
    #[serde(default)]
    pub(crate) geolocation: Option<String>,
    #[serde(default)]
    pub(crate) verification: Option<String>,
    #[serde(default)]
    search: Option<VastHourlyBreakdown>,
}

#[derive(Debug, Clone, Deserialize)]
struct VastHourlyBreakdown {
    #[serde(default, rename = "totalHour")]
    total_hour: Option<f64>,
    #[serde(default, rename = "discountedTotalPerHour")]
    discounted_total_per_hour: Option<f64>,
}

struct AccountSshKey {
    key: String,
}

#[derive(Debug, Deserialize)]
struct VastInstancesResponse {
    #[serde(default)]
    instances: Vec<VastInstance>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct VastInstance {
    pub(crate) id: u64,
    #[serde(default)]
    pub(crate) label: Option<String>,
    #[serde(default)]
    pub(crate) image: Option<String>,
    #[serde(default)]
    pub(crate) image_uuid: Option<String>,
    #[serde(default)]
    pub(crate) image_runtype: Option<String>,
    #[serde(default)]
    pub(crate) cur_state: Option<String>,
    #[serde(default)]
    pub(crate) next_state: Option<String>,
    #[serde(default)]
    pub(crate) intended_status: Option<String>,
    #[serde(default)]
    pub(crate) actual_status: Option<String>,
    #[serde(default)]
    pub(crate) status_msg: Option<String>,
    #[serde(default)]
    pub(crate) start_date: Option<f64>,
    #[serde(default)]
    pub(crate) uptime_mins: Option<f64>,
    #[serde(default)]
    pub(crate) gpu_name: Option<String>,
    #[serde(default)]
    pub(crate) dph_total: Option<f64>,
    #[serde(default)]
    pub(crate) end_date: Option<f64>,
    #[serde(default)]
    pub(crate) ssh_host: Option<String>,
    #[serde(default)]
    pub(crate) ssh_port: Option<u16>,
    #[serde(skip)]
    pub(crate) workload: Option<InstanceWorkload>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct VastScheduledJob {
    #[serde(default)]
    pub(crate) instance_id: Option<u64>,
    #[serde(default)]
    pub(crate) api_endpoint: Option<String>,
    #[serde(default)]
    pub(crate) request_method: Option<String>,
    #[serde(default)]
    pub(crate) request_body: Option<Value>,
    #[serde(default)]
    pub(crate) start_time: Option<f64>,
}

#[derive(Debug, Deserialize)]
struct VastSimpleResponse {
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    new_contract: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct VastLogsResponse {
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    msg: Option<String>,
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    result_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct VastGpuNamesResponse {
    #[serde(default)]
    success: Option<bool>,
    #[serde(default)]
    gpu_names: Vec<String>,
}

pub(crate) struct VastClient {
    http: Client,
    api_key: String,
}

#[derive(Debug, Clone, Copy)]
enum InstanceSshKeyAttachStatus {
    Attached,
    AlreadyAssociated,
}

pub(crate) struct Provider;
pub(crate) struct CacheModel;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct CacheEntry {
    pub(crate) id: u64,
    pub(crate) label: String,
    #[serde(default)]
    pub(crate) workload: Option<InstanceWorkload>,
    #[serde(default)]
    pub(crate) listed: Option<ListedInstance>,
    #[serde(default)]
    pub(crate) observed_at_unix: Option<u64>,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub(crate) struct CacheStore {
    #[serde(default)]
    pub(crate) entries: Vec<CacheEntry>,
}

impl VastOffer {
    pub(crate) fn hourly_price(&self) -> f64 {
        if let Some(value) = self.dph_total {
            return value;
        }
        if let Some(search) = &self.search {
            if let Some(value) = search.total_hour {
                return value;
            }
            if let Some(value) = search.discounted_total_per_hour {
                return value;
            }
        }
        f64::INFINITY
    }

    pub(crate) fn gpu_name(&self) -> &str {
        self.gpu_name.as_deref().unwrap_or("unknown")
    }
}

impl VastInstance {
    pub(crate) fn label_str(&self) -> &str {
        self.label.as_deref().unwrap_or("")
    }

    pub(crate) fn state_str(&self) -> &str {
        self.cur_state
            .as_deref()
            .or(self.next_state.as_deref())
            .unwrap_or("unknown")
    }

    pub(crate) fn is_running(&self) -> bool {
        self.state_str().eq_ignore_ascii_case("running")
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.state_str().eq_ignore_ascii_case("stopped")
    }

    pub(crate) fn health_hint(&self) -> String {
        if self
            .status_msg
            .as_deref()
            .map(|message| message.to_ascii_lowercase().contains("unhealthy"))
            .unwrap_or(false)
        {
            return "unhealthy".to_owned();
        }

        if let Some(actual_status) = self
            .actual_status
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            if actual_status.eq_ignore_ascii_case("running") {
                return "ok".to_owned();
            }

            let expected_running = self
                .intended_status
                .as_deref()
                .map(|status| status.eq_ignore_ascii_case("running"))
                .unwrap_or(self.is_running());
            if expected_running {
                return actual_status.to_ascii_lowercase();
            }
        }

        "ok".to_owned()
    }

    pub(crate) fn runtime_hours(&self) -> f64 {
        if let Some(uptime_mins) = self.uptime_mins
            && uptime_mins > 0.0
        {
            return uptime_mins / 60.0;
        }

        if self.is_running()
            && let Some(start) = self.start_date
        {
            let now = now_unix_secs_f64();
            if now > start {
                return (now - start) / 3600.0;
            }
        }

        0.0
    }
}

impl VastClient {
    pub(crate) fn new(api_key: &str) -> Result<Self> {
        let api_key = api_key.trim();
        if api_key.is_empty() {
            bail!("Missing Vast API key. Run `ice login --cloud vast.ai`.");
        }

        Ok(Self {
            http: Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .context("Failed to build HTTP client")?,
            api_key: api_key.to_owned(),
        })
    }

    pub(crate) fn validate_api_key(&self) -> Result<()> {
        let _ = self.get_json("/api/v0/users/current/", "validate vast.ai API key")?;
        Ok(())
    }

    pub(crate) fn fetch_gpu_names(&self) -> Result<Vec<String>> {
        let parsed = serde_json::from_value::<VastGpuNamesResponse>(
            self.get_json("/api/v0/gpu_names/unique/", "fetch gpu names")?,
        )
        .context("Failed to parse gpu names response from vast.ai")?;
        if parsed.success == Some(false) {
            bail!("vast.ai rejected GPU names request");
        }
        Ok(parsed.gpu_names)
    }

    pub(crate) fn list_instances(&self) -> Result<Vec<VastInstance>> {
        Ok(serde_json::from_value::<VastInstancesResponse>(
            self.get_json("/api/v0/instances/", "list instances")?,
        )
        .context("Failed to parse vast.ai instances response")?
        .instances)
    }

    fn list_scheduled_jobs(&self) -> Result<Vec<VastScheduledJob>> {
        let value = self.get_json("/api/v0/commands/schedule_job/", "list scheduled jobs")?;
        let rows = if let Some(rows) = value.as_array() {
            rows.clone()
        } else if let Some(rows) = value.get("results").and_then(Value::as_array) {
            rows.clone()
        } else {
            Vec::new()
        };
        Ok(rows
            .into_iter()
            .filter_map(|row| serde_json::from_value::<VastScheduledJob>(row).ok())
            .collect())
    }

    fn get_instance(&self, id: u64) -> Result<Option<VastInstance>> {
        match self.get_instance_by_id(id) {
            Ok(Some(instance)) => Ok(Some(instance)),
            Ok(None) => Ok(self
                .list_instances()?
                .into_iter()
                .find(|instance| instance.id == id)),
            Err(err) if should_fallback_to_list_lookup(&err) => Ok(self
                .list_instances()?
                .into_iter()
                .find(|instance| instance.id == id)),
            Err(err) => Err(err),
        }
    }

    fn get_instance_by_id(&self, id: u64) -> Result<Option<VastInstance>> {
        parse_instance_from_value(
            &self.get_json(&format!("/api/v0/instances/{id}/"), "get instance")?,
        )
    }

    pub(crate) fn search_offers(&self, body: &Value) -> Result<Vec<VastOffer>> {
        Ok(serde_json::from_value::<VastOffersResponse>(self.post_json(
            "/api/v0/bundles/",
            body,
            "search offers",
        )?)
        .context("Failed to parse vast.ai offers response")?
        .offers)
    }

    pub(crate) fn create_instance(&self, offer_id: u64, body: &Value) -> Result<u64> {
        let parsed = serde_json::from_value::<VastSimpleResponse>(self.put_json(
            &format!("/api/v0/asks/{offer_id}/"),
            body,
            "create instance",
        )?)
        .context("Failed to parse create instance response")?;
        if parsed.success != Some(true) {
            bail!(
                "Failed to create instance: {}",
                parsed
                    .msg
                    .or(parsed.error)
                    .unwrap_or_else(|| "unknown create error".to_owned())
            );
        }
        parsed
            .new_contract
            .ok_or_else(|| anyhow!("Vast API response missing `new_contract`"))
    }

    pub(crate) fn set_instance_state(&self, id: u64, state: &str) -> Result<()> {
        let parsed = serde_json::from_value::<VastSimpleResponse>(self.put_json(
            &format!("/api/v0/instances/{id}/"),
            &json!({ "state": state }),
            &format!("set instance {id} to {state}"),
        )?)
        .context("Failed to parse set state response")?;
        if parsed.success != Some(true) {
            bail!(
                "Failed to set instance state: {}",
                parsed
                    .msg
                    .or(parsed.error)
                    .unwrap_or_else(|| "unknown state update error".to_owned())
            );
        }
        Ok(())
    }

    pub(crate) fn delete_instance(&self, id: u64) -> Result<()> {
        let parsed = serde_json::from_value::<VastSimpleResponse>(
            self.delete_json(&format!("/api/v0/instances/{id}/"), "delete instance")?,
        )
        .context("Failed to parse delete response")?;
        if parsed.success != Some(true) {
            bail!(
                "Failed to delete instance: {}",
                parsed
                    .msg
                    .or(parsed.error)
                    .unwrap_or_else(|| "unknown delete error".to_owned())
            );
        }
        Ok(())
    }

    pub(crate) fn schedule_instance_stop(
        &self,
        id: u64,
        stop_at_unix: u64,
        schedule_end_unix: u64,
    ) -> Result<()> {
        if schedule_end_unix <= stop_at_unix {
            bail!(
                "Invalid auto-stop schedule for instance {id}: end ({schedule_end_unix}) must be greater than stop time ({stop_at_unix})."
            );
        }

        let value = self.post_json(
            "/api/v0/commands/schedule_job/",
            &json!({
                "start_time": stop_at_unix as f64,
                "end_time": schedule_end_unix as f64,
                "api_endpoint": format!("/api/v0/instances/{id}/"),
                "request_method": "PUT",
                "request_body": { "state": "stopped" },
                "day_of_the_week": Value::Null,
                "hour_of_the_day": Value::Null,
                "frequency": "HOURLY",
                "instance_id": id
            }),
            "schedule vast.ai instance auto-stop",
        )?;
        if value
            .get("success")
            .and_then(Value::as_bool)
            .is_some_and(|success| !success)
        {
            bail!(
                "Failed to schedule instance auto-stop: {}",
                value
                    .get("msg")
                    .and_then(Value::as_str)
                    .or_else(|| value.get("error").and_then(Value::as_str))
                    .unwrap_or("unknown schedule error")
            );
        }
        Ok(())
    }

    fn attach_instance_ssh_key(
        &self,
        id: u64,
        ssh_key: &str,
    ) -> Result<InstanceSshKeyAttachStatus> {
        let parsed = serde_json::from_value::<VastSimpleResponse>(self.post_json(
            &format!("/api/v0/instances/{id}/ssh/"),
            &json!({ "ssh_key": ssh_key }),
            "attach ssh key to instance",
        )?)
        .context("Failed to parse attach ssh key response")?;
        if parsed.success == Some(false) {
            let message = parsed
                .msg
                .or(parsed.error)
                .unwrap_or_else(|| "unknown attach ssh key error".to_owned());
            let lower = message.to_ascii_lowercase();
            if lower.contains("already associated with instance")
                || lower.contains("already associated")
            {
                return Ok(InstanceSshKeyAttachStatus::AlreadyAssociated);
            }
            bail!("Failed to attach ssh key: {message}");
        }
        Ok(InstanceSshKeyAttachStatus::Attached)
    }

    fn list_account_ssh_keys(&self) -> Result<Vec<AccountSshKey>> {
        let value = self.get_json("/api/v0/ssh/", "list account ssh keys")?;
        let rows = if let Some(rows) = value.as_array() {
            rows.clone()
        } else if let Some(rows) = value.get("keys").and_then(Value::as_array) {
            rows.clone()
        } else {
            Vec::new()
        };
        Ok(rows
            .into_iter()
            .filter_map(|row| {
                let key = row
                    .get("key")
                    .and_then(Value::as_str)
                    .or_else(|| row.get("public_key").and_then(Value::as_str))
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned)?;
                Some(AccountSshKey { key })
            })
            .collect())
    }

    fn create_account_ssh_key(&self, ssh_key: &str) -> Result<u64> {
        let value = self.post_json(
            "/api/v0/ssh/",
            &json!({ "ssh_key": ssh_key }),
            "create account ssh key",
        )?;
        if value
            .get("success")
            .and_then(Value::as_bool)
            .is_some_and(|success| !success)
        {
            bail!(
                "Failed to create account ssh key: {}",
                value
                    .get("msg")
                    .and_then(Value::as_str)
                    .or_else(|| value.get("error").and_then(Value::as_str))
                    .unwrap_or("unknown create account ssh key error")
            );
        }
        value
            .get("key")
            .and_then(|key| key.get("id"))
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("create account ssh key response did not contain a key id"))
    }

    fn ensure_account_ssh_key(&self, ssh_key: &str) -> Result<bool> {
        let target = ssh_key.trim();
        if target.is_empty() {
            bail!("SSH key cannot be empty.");
        }
        if self
            .list_account_ssh_keys()?
            .iter()
            .any(|candidate| candidate.key.trim() == target)
        {
            return Ok(false);
        }
        let _ = self.create_account_ssh_key(target)?;
        Ok(true)
    }

    fn delete_account_ssh_key(&self, ssh_key_id: u64) -> Result<()> {
        let value = self.delete_json(
            &format!("/api/v0/ssh/{ssh_key_id}/"),
            "delete account ssh key",
        )?;
        if value
            .get("success")
            .and_then(Value::as_bool)
            .is_some_and(|success| !success)
        {
            bail!(
                "Failed to delete account ssh key {ssh_key_id}: {}",
                value
                    .get("msg")
                    .and_then(Value::as_str)
                    .or_else(|| value.get("error").and_then(Value::as_str))
                    .unwrap_or("unknown delete account ssh key error")
            );
        }
        Ok(())
    }

    fn request_logs(
        &self,
        id: u64,
        tail: u32,
        filter: Option<&str>,
        daemon_logs: bool,
    ) -> Result<String> {
        let mut body = json!({ "tail": tail });
        if let Some(filter) = filter
            && !filter.trim().is_empty()
        {
            body["filter"] = Value::String(filter.trim().to_owned());
        }
        if daemon_logs {
            body["daemon_logs"] = Value::Bool(true);
        }
        let parsed = serde_json::from_value::<VastLogsResponse>(self.put_json(
            &format!("/api/v0/instances/request_logs/{id}"),
            &body,
            "request vast.ai instance logs",
        )?)
        .context("Failed to parse vast.ai logs response")?;
        if parsed.success == Some(false) {
            bail!(
                "Failed to request instance logs: {}",
                parsed
                    .msg
                    .or(parsed.error)
                    .unwrap_or_else(|| "unknown logs error".to_owned())
            );
        }
        self.wait_for_log_download(
            parsed
                .result_url
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| anyhow!("Vast logs response missing `result_url`."))?,
            Duration::from_secs(VAST_LOG_READY_TIMEOUT_SECS),
        )
    }

    fn wait_for_log_download(&self, url: &str, timeout: Duration) -> Result<String> {
        let start = SystemTime::now();
        loop {
            let response = self
                .http
                .get(url)
                .send()
                .with_context(|| format!("Failed to fetch vast.ai log artifact from {url}"))?;
            let status = response.status();
            let text = response.text().with_context(|| {
                format!("Failed to read vast.ai log artifact response body from {url}")
            })?;

            if status.is_success() {
                return Ok(text);
            }
            if matches!(status.as_u16(), 403 | 404) && elapsed_since(start)? < timeout {
                thread::sleep(Duration::from_millis(VAST_LOG_READY_POLL_INTERVAL_MILLIS));
                continue;
            }
            let message = extract_api_error_message(&text);
            if matches!(status.as_u16(), 403 | 404) {
                bail!(
                    "Timed out waiting for vast.ai log artifact to become readable: HTTP {} {}",
                    status.as_u16(),
                    message
                );
            }
            bail!(
                "Failed to fetch vast.ai log artifact: HTTP {} {}",
                status.as_u16(),
                message
            );
        }
    }

    fn get_json(&self, path: &str, context: &str) -> Result<Value> {
        self.send_json(
            || self.auth(self.http.get(format!("{VAST_BASE_URL}{path}"))),
            context,
        )
    }

    fn post_json(&self, path: &str, body: &Value, context: &str) -> Result<Value> {
        self.send_json(
            || self.auth(self.http.post(format!("{VAST_BASE_URL}{path}")).json(body)),
            context,
        )
    }

    fn put_json(&self, path: &str, body: &Value, context: &str) -> Result<Value> {
        self.send_json(
            || self.auth(self.http.put(format!("{VAST_BASE_URL}{path}")).json(body)),
            context,
        )
    }

    fn delete_json(&self, path: &str, context: &str) -> Result<Value> {
        self.send_json(
            || {
                self.auth(
                    self.http
                        .delete(format!("{VAST_BASE_URL}{path}"))
                        .json(&json!({})),
                )
            },
            context,
        )
    }

    fn auth(&self, request: RequestBuilder) -> RequestBuilder {
        request.header("Authorization", format!("Bearer {}", self.api_key))
    }

    fn send_json<F>(&self, make_request: F, context: &str) -> Result<Value>
    where
        F: FnMut() -> RequestBuilder,
    {
        parse_json_response(
            http_retry::send_with_429_backoff(
                make_request,
                context,
                http_retry::BackoffPolicy::default(),
            )?,
            context,
        )
    }
}

impl CloudInstance for VastInstance {
    type ListContext = HashMap<u64, f64>;

    fn cache_key(&self) -> String {
        self.id.to_string()
    }

    fn display_name(&self) -> String {
        display_name_or_fallback(self.label_str(), self.id.to_string())
    }

    fn state_value(&self) -> &str {
        self.state_str()
    }

    fn is_running(&self) -> bool {
        self.is_running()
    }

    fn is_stopped(&self) -> bool {
        self.is_stopped()
    }

    fn workload(&self) -> Option<&InstanceWorkload> {
        self.workload.as_ref()
    }

    fn render(&self, context: &Self::ListContext, pending_context: bool) -> ListedInstance {
        let health = self.health_hint();
        let state = display_state(self.state_str());
        let mut fields = Vec::new();
        push_field(&mut fields, show_health_field(&health));
        fields.push(format!("{:.2}h", self.runtime_hours()));
        push_field(
            &mut fields,
            remaining_field(self, context.get(&self.id).copied(), pending_context),
        );
        push_field(
            &mut fields,
            self.dph_total.map(|value| format!("${value:.4}/hr")),
        );
        push_field(
            &mut fields,
            present_field(self.gpu_name.as_deref().unwrap_or("unknown")),
        );

        let mut detail_fields = vec![format!("vast://{}", self.id)];
        if let (Some(host), Some(port)) = (
            self.ssh_host
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty()),
            self.ssh_port,
        ) {
            detail_fields.push(format!("ssh://{host}:{port}"));
        }
        push_field(&mut detail_fields, present_field(&workload_display(self)));

        base_listed_instance(
            display_name_or_fallback(self.label_str(), self.id.to_string()),
            state.clone(),
            list_state_color(&state, Some(&health)),
            fields,
            detail_fields,
        )
    }
}

impl CloudCacheModel for CacheModel {
    type Instance = VastInstance;
    type ListContext = HashMap<u64, f64>;
    type Entry = CacheEntry;
    type Store = CacheStore;

    const CLOUD: Cloud = Cloud::VastAi;

    fn entries(store: &Self::Store) -> &[Self::Entry] {
        &store.entries
    }

    fn entries_mut(store: &mut Self::Store) -> &mut Vec<Self::Entry> {
        &mut store.entries
    }

    fn key_for_entry(entry: &Self::Entry) -> String {
        entry.id.to_string()
    }

    fn entry_from_instance(
        instance: &Self::Instance,
        observed_at_unix: u64,
        context: &Self::ListContext,
    ) -> Option<Self::Entry> {
        let label = instance.label_str();
        if !label.starts_with(ICE_LABEL_PREFIX) {
            return None;
        }
        Some(CacheEntry {
            id: instance.id,
            label: label.to_owned(),
            workload: infer_workload(instance),
            listed: Some(instance.render(context, false)),
            observed_at_unix: Some(observed_at_unix),
        })
    }

    fn listed_from_entry(entry: &Self::Entry) -> Option<&ListedInstance> {
        entry.listed.as_ref()
    }

    fn observed_at_unix(entry: &Self::Entry) -> u64 {
        entry.observed_at_unix.unwrap_or_default()
    }
}

impl CloudProvider for Provider {
    type Instance = VastInstance;
    type ProviderContext<'a> = VastClient;
    const CLOUD: Cloud = Cloud::VastAi;

    fn context<'a>(config: &'a IceConfig) -> Result<Self::ProviderContext<'a>> {
        client_from_config(config)
    }

    fn list_instances(
        context: &Self::ProviderContext<'_>,
        on_progress: &mut dyn FnMut(String),
    ) -> Result<Vec<Self::Instance>> {
        on_progress(Self::initial_loading_message());
        let mut instances = context
            .list_instances()?
            .into_iter()
            .map(|mut instance| {
                hydrate_instance_workload(&mut instance);
                instance
            })
            .filter(|instance| instance.label_str().starts_with(ICE_LABEL_PREFIX))
            .collect::<Vec<_>>();
        Self::sort_instances(&mut instances);
        Ok(instances)
    }

    fn sort_instances(instances: &mut [Self::Instance]) {
        instances.sort_by(|left, right| right.id.cmp(&left.id));
    }

    fn resolve_instance(
        context: &Self::ProviderContext<'_>,
        identifier: &str,
    ) -> Result<Self::Instance> {
        resolve_instance(context, identifier)
    }

    fn set_running(
        context: &Self::ProviderContext<'_>,
        instance: &Self::Instance,
        running: bool,
    ) -> Result<()> {
        context.set_instance_state(instance.id, if running { "running" } else { "stopped" })
    }

    fn wait_for_running_state(
        context: &Self::ProviderContext<'_>,
        instance: &Self::Instance,
        running: bool,
        timeout: Duration,
    ) -> Result<Self::Instance> {
        wait_for_state(
            context,
            instance.id,
            if running { "running" } else { "stopped" },
            timeout,
        )
    }

    fn delete_instance(
        context: &Self::ProviderContext<'_>,
        instance: &Self::Instance,
    ) -> Result<()> {
        context.delete_instance(instance.id)
    }
}

impl RemoteCloudProvider for Provider {
    type CacheModel = CacheModel;

    fn list_context_loading_message() -> Option<String> {
        Some("Resolving vast.ai auto-stop state...".to_owned())
    }

    fn resolve_list_context(
        context: &Self::ProviderContext<'_>,
        _instances: &[Self::Instance],
        on_progress: &mut dyn FnMut(String),
    ) -> Result<<Self::Instance as CloudInstance>::ListContext> {
        on_progress(Self::list_context_loading_message().unwrap_or_default());
        Ok(nearest_scheduled_termination_by_instance(
            &context.list_scheduled_jobs()?,
        ))
    }
}

impl CommandProvider for Provider {
    fn logs(config: &IceConfig, args: &LogsArgs) -> Result<()> {
        let client = client_from_config(config)?;
        let instance = resolve_instance(&client, &args.instance)?;
        if matches!(
            instance.workload.as_ref(),
            Some(InstanceWorkload::Unpack(_))
        ) {
            if args.filter.is_some() || args.daemon {
                bail!("`ice logs` filter/daemon flags are not supported for `unpack` workloads.");
            }
            return stream_unpack_logs_with_auto_key(&client, &instance, args.tail, args.follow);
        }
        stream_logs(
            &client,
            &instance,
            args.tail,
            args.filter.as_deref(),
            args.daemon,
            args.follow,
        )
    }

    fn shell(config: &IceConfig, args: &ShellArgs) -> Result<()> {
        let client = client_from_config(config)?;
        let mut instance = resolve_instance(&client, &args.instance)?;
        if !instance_supports_ssh(&instance) {
            bail!(
                "Instance `{}` is a Vast entrypoint workload. Use `ice logs --cloud vast.ai {}` to inspect stdout/stderr.",
                instance.id,
                instance.id
            );
        }

        if instance.is_stopped() {
            if !prompt_confirm("Instance is stopped. Start it before opening shell?", true)? {
                bail!("Aborted: instance is stopped.");
            }
            let spinner = spinner("Starting instance...");
            client
                .set_instance_state(instance.id, "running")
                .context("Failed to start stopped instance")?;
            spinner.finish_with_message("Start requested.");
            instance = wait_for_state(
                &client,
                instance.id,
                "running",
                Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
            )?;
        }

        instance = wait_for_ssh_ready(
            &client,
            instance.id,
            Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
        )?;
        if matches!(
            instance.workload.as_ref(),
            Some(InstanceWorkload::Unpack(_))
        ) {
            let remote_command =
                crate::unpack::unpack_shell_remote_command(&remote_unpack_dir_for_vast(&instance));
            open_remote_shell_with_auto_key(&client, &instance, Some(&remote_command))
        } else {
            open_shell_with_auto_key(&client, &instance)
        }
    }

    fn download(config: &IceConfig, args: &DownloadArgs) -> Result<()> {
        let client = client_from_config(config)?;
        let instance = resolve_instance(&client, &args.instance)?;
        if !instance.is_running() {
            bail!(
                "Instance `{}` is not running (state: {}).",
                instance.id,
                instance.state_str()
            );
        }
        if !instance_supports_ssh(&instance) {
            bail!(
                "Instance `{}` is a Vast entrypoint workload. Downloading files requires SSH access, which Vast does not provide for entrypoint-mode containers.",
                instance.id
            );
        }
        ensure_instance_has_ssh(&instance)?;
        run_download_with_auto_key(
            &client,
            &instance,
            &args.remote_path,
            args.local_path.as_deref(),
        )
    }
}

impl CreateProvider for Provider {
    fn create(config: &mut IceConfig, args: &CreateArgs) -> Result<()> {
        let client = client_from_config(config)?;
        let gpu_options = load_gpu_options(Cloud::VastAi, Some(&client));
        ensure_default_create_config(config, Cloud::VastAi, &gpu_options)?;
        let hours = resolve_deploy_hours(config, args.hours)?;
        let workload = resolve_deploy_workload(&args.target_request())?;
        let label = build_cloud_instance_name(&collect_existing_visible_names(&client)?)?;
        let create_body = build_create_request(config, &label, &workload)?;

        let mut search = build_search_requirements(config, Cloud::VastAi)?;
        if args.custom {
            prompt_create_search_filters(Cloud::VastAi, &mut search, &gpu_options)?;
        }

        let mut rejected_offer_ids = HashSet::new();
        let instance_id = loop {
            let offer = find_cheapest_offer(
                &client,
                &search,
                hours,
                args.machine.as_deref(),
                &rejected_offer_ids,
            )?;
            let price = offer.hourly_price();
            if !price.is_finite() {
                bail!("Vast returned an offer without usable hourly price.");
            }
            let cost = apply_vast_autostop_cost_estimate(estimate_runtime_cost(
                Cloud::VastAi,
                price,
                hours,
            )?)?;

            print_offer_summary(&offer, &cost, &search);

            if cost.hourly_usd > search.max_price_per_hr {
                let available_hours = offer
                    .duration
                    .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
                    .map(|seconds| seconds / 3600.0)
                    .unwrap_or(0.0);
                bail!(
                    "No offer meets max price ${:.4}/hr. Best matching offer is ${:.4}/hr (est ${:.4} for {:.3}h scheduled, {:.3}h requested). Offer {} is available for {:.3}h.",
                    search.max_price_per_hr,
                    price,
                    cost.total_usd,
                    cost.billed_hours,
                    cost.requested_hours,
                    offer.id,
                    available_hours
                );
            }

            if args.dry_run {
                println!(
                    "Dry run: best matching offer is {} at ${:.4}/hr, est ${:.4} for {:.3}h scheduled ({:.3}h requested). Aborting before accept/pay/create.",
                    offer.id, price, cost.total_usd, cost.billed_hours, cost.requested_hours
                );
                return Ok(());
            }

            match prompt_offer_decision(&build_accept_prompt(&cost))? {
                crate::model::OfferDecision::ChangeFilter => {
                    prompt_adjust_search_filters(
                        Cloud::VastAi,
                        &mut search,
                        &load_gpu_options(Cloud::VastAi, Some(&client)),
                    )?;
                }
                crate::model::OfferDecision::Reject => {
                    println!("Aborted.");
                    return Ok(());
                }
                crate::model::OfferDecision::Accept => {
                    print_stage("Creating instance from accepted offer");
                    let create_spinner = spinner("Accepting offer and creating instance...");
                    match client.create_instance(offer.id, &create_body) {
                        Ok(instance_id) => {
                            create_spinner
                                .finish_with_message(format!("Created instance {instance_id}."));
                            break instance_id;
                        }
                        Err(err) => {
                            create_spinner.finish_and_clear();
                            rejected_offer_ids.insert(offer.id);
                            print_warning(&format!(
                                "Offer {} acceptance failed: {err:#}",
                                offer.id
                            ));
                            if !io::stdin().is_terminal() {
                                return Err(err).with_context(|| {
                                    format!("Failed to create instance from offer {}", offer.id)
                                });
                            }
                            if prompt_confirm("Offer accept failed. Retry search?", true)? {
                                continue;
                            }
                            return Err(err).with_context(|| {
                                format!("Failed to create instance from offer {}", offer.id)
                            });
                        }
                    }
                }
            }
        };

        print_stage("Scheduling instance auto-stop");
        let auto_stop_plan = build_vast_autostop_plan(now_unix_secs(), hours)?;
        let auto_stop_spinner = spinner("Scheduling instance auto-stop...");
        client
            .schedule_instance_stop(
                instance_id,
                auto_stop_plan.stop_at_unix,
                auto_stop_plan.schedule_end_unix,
            )
            .with_context(|| {
                format!("Failed to schedule auto-stop for vast.ai instance {instance_id}.")
            })?;
        auto_stop_spinner.finish_with_message(format!(
            "Auto-stop scheduled for {} ({:.3}h planned runtime).",
            format_unix_utc(auto_stop_plan.stop_at_unix),
            auto_stop_plan.runtime_hours
        ));

        match &workload {
            InstanceWorkload::Shell => {
                print_stage("Waiting for SSH access");
                let instance = wait_for_ssh_ready(
                    &client,
                    instance_id,
                    Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
                )?;
                if prompt_confirm("Open shell in the new instance now?", true)? {
                    print_stage("Opening shell");
                    open_shell_with_auto_key(&client, &instance)?;
                }
            }
            InstanceWorkload::Container(_) => {
                print_stage("Waiting for container workload startup");
                let instance = wait_for_workload_start(
                    &client,
                    instance_id,
                    Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
                )?;
                println!("Container workload status: {}", status_summary(&instance));
                if prompt_confirm("Follow container logs now?", true)? {
                    print_stage("Following container logs");
                    stream_logs(&client, &instance, 200, None, false, true)?;
                } else {
                    println!(
                        "Use `ice logs --cloud vast.ai {} --follow` to inspect stdout/stderr.",
                        instance.id
                    );
                }
            }
            InstanceWorkload::Unpack(source) => {
                print_stage("Waiting for SSH access");
                let mut instance = wait_for_ssh_ready(
                    &client,
                    instance_id,
                    Duration::from_secs(VAST_WAIT_TIMEOUT_SECS),
                )?;
                instance.workload = Some(workload.clone());
                upsert_instance::<CacheModel>(&instance);
                deploy_unpack(config, &client, &instance, source)?;
                println!(
                    "Unpack workload staged from {}.",
                    display_unpack_source(source)
                );
                if prompt_confirm("Follow unpack logs now?", true)? {
                    print_stage("Following unpack logs");
                    stream_unpack_logs_with_auto_key(&client, &instance, 200, true)?;
                } else {
                    println!(
                        "Use `ice logs --cloud vast.ai {} --follow` to inspect stdout/stderr.",
                        instance.id
                    );
                }
            }
        }

        Ok(())
    }
}

pub(crate) fn client_from_config(config: &IceConfig) -> Result<VastClient> {
    VastClient::new(
        config.auth.vast_ai.api_key.as_deref().ok_or_else(|| {
            anyhow!("Missing Vast API key. Run `ice login --cloud vast.ai` first.")
        })?,
    )
}

pub(crate) fn build_create_request(
    config: &IceConfig,
    label: &str,
    workload: &InstanceWorkload,
) -> Result<Value> {
    let mut body = json!({
        "client_id": "me",
        "disk": VAST_DEFAULT_DISK_GB,
        "runtype": runtype_for_workload(workload),
        "label": label,
        "cancel_unavail": true,
    });

    match workload {
        InstanceWorkload::Shell | InstanceWorkload::Unpack(_) => {
            body["image"] = Value::String(VAST_DEFAULT_IMAGE.to_owned());
        }
        InstanceWorkload::Container(container) => {
            body["image"] = Value::String(container.container_ref());
            let registry_auth = crate::providers::gcp::registry_login(config)?;
            body["image_login"] = Value::String(format!(
                "-u {} -p {} {}",
                registry_auth.username,
                registry_auth.secret,
                container.registry_host()
            ));
        }
    }

    Ok(body)
}

pub(crate) fn resolve_instance(client: &VastClient, identifier: &str) -> Result<VastInstance> {
    let identifier = identifier.trim();
    if identifier.is_empty() {
        bail!("Instance identifier cannot be empty.");
    }

    if let Ok(id) = identifier.parse::<u64>() {
        return try_instance_by_id(client, id)?
            .ok_or_else(|| anyhow!("No instance found with ID `{id}`."));
    }

    let cache = load_cache_store::<CacheModel>();
    if let Some(instance) = resolve_instance_from_cache(client, &cache, identifier)? {
        return Ok(instance);
    }

    let instances = client
        .list_instances()?
        .into_iter()
        .map(|mut instance| {
            hydrate_instance_workload(&mut instance);
            instance
        })
        .filter(|instance| instance.label_str().starts_with(ICE_LABEL_PREFIX))
        .collect::<Vec<_>>();
    persist_instances::<CacheModel>(&instances);
    resolve_instance_from_list(instances, identifier)
}

pub(crate) fn infer_workload(instance: &VastInstance) -> Option<InstanceWorkload> {
    if let Some(workload) = instance.workload.as_ref() {
        return Some(workload.clone());
    }
    let image = instance_image_ref(instance)?;
    if image == VAST_DEFAULT_IMAGE {
        return Some(InstanceWorkload::Shell);
    }
    ContainerImageReference::from_container_ref(image)
        .ok()
        .map(InstanceWorkload::Container)
}

pub(crate) fn hydrate_instance_workload(instance: &mut VastInstance) {
    if instance.workload.is_some() {
        return;
    }
    instance.workload = cached_workload(instance.id).or_else(|| infer_workload(instance));
}

pub(crate) fn instance_supports_ssh(instance: &VastInstance) -> bool {
    match instance
        .image_runtype
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        Some(value) => {
            value.eq_ignore_ascii_case("ssh") || value.eq_ignore_ascii_case("ssh_direct")
        }
        None => matches!(infer_workload(instance), Some(InstanceWorkload::Shell)),
    }
}

pub(crate) fn wait_for_state(
    client: &VastClient,
    instance_id: u64,
    desired_state: &str,
    timeout: Duration,
) -> Result<VastInstance> {
    let start = SystemTime::now();
    let spinner = spinner(&format!(
        "Waiting for instance {instance_id} to reach state `{desired_state}`..."
    ));
    loop {
        if elapsed_since(start)? > timeout {
            spinner.finish_and_clear();
            bail!("Timed out waiting for instance {instance_id} to reach state `{desired_state}`.");
        }

        if let Some(mut instance) = client.get_instance(instance_id)? {
            hydrate_instance_workload(&mut instance);
            upsert_instance::<CacheModel>(&instance);
            if instance.state_str().eq_ignore_ascii_case(desired_state) {
                spinner.finish_with_message(format!(
                    "Instance {} is now {}.",
                    instance_id,
                    instance.state_str()
                ));
                return Ok(instance);
            }
            spinner.set_message(format!(
                "Waiting for instance {instance_id} to reach state `{desired_state}`... {}",
                status_summary(&instance)
            ));
        }

        thread::sleep(Duration::from_secs(VAST_POLL_INTERVAL_SECS));
    }
}

pub(crate) fn wait_for_ssh_ready(
    client: &VastClient,
    instance_id: u64,
    timeout: Duration,
) -> Result<VastInstance> {
    let start = SystemTime::now();
    let spinner = spinner(&format!(
        "Waiting for instance {instance_id} to be running with SSH..."
    ));
    let mut last_issue = None;
    loop {
        if elapsed_since(start)? > timeout {
            spinner.finish_and_clear();
            if let Some(issue) = last_issue {
                bail!(
                    "Timed out waiting for SSH readiness on instance {instance_id}. Last issue: {issue}"
                );
            }
            bail!("Timed out waiting for SSH readiness on instance {instance_id}.");
        }

        if let Some(mut instance) = client.get_instance(instance_id)? {
            hydrate_instance_workload(&mut instance);
            upsert_instance::<CacheModel>(&instance);
            if instance.is_running()
                && instance.ssh_host.as_deref().is_some()
                && instance.ssh_port.is_some()
            {
                let (host, port) = ssh_target(&instance)?;
                match crate::support::tcp_port_open(&host, port, Duration::from_secs(3)) {
                    Ok(()) => {
                        spinner
                            .finish_with_message(format!("Instance {instance_id} is SSH-ready."));
                        return Ok(instance);
                    }
                    Err(err) => {
                        last_issue =
                            Some(format!("{host}:{port} not accepting connections ({err})"));
                    }
                }
            }
            spinner.set_message(format!(
                "Waiting for instance {instance_id} to be running with SSH... {}",
                status_summary(&instance)
            ));
        }

        thread::sleep(Duration::from_secs(VAST_POLL_INTERVAL_SECS));
    }
}

pub(crate) fn wait_for_workload_start(
    client: &VastClient,
    instance_id: u64,
    timeout: Duration,
) -> Result<VastInstance> {
    let start = SystemTime::now();
    let spinner = spinner(&format!(
        "Waiting for Vast entrypoint workload on instance {instance_id}..."
    ));
    let mut last_status = "no status yet".to_owned();
    loop {
        if elapsed_since(start)? > timeout {
            spinner.finish_and_clear();
            bail!(
                "Timed out waiting for Vast entrypoint workload on instance {instance_id}. Last status: {last_status}"
            );
        }

        if let Some(mut instance) = client.get_instance(instance_id)? {
            hydrate_instance_workload(&mut instance);
            upsert_instance::<CacheModel>(&instance);
            last_status = status_summary(&instance);
            if instance
                .actual_status
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some_and(|status| !status.eq_ignore_ascii_case("loading"))
            {
                spinner.finish_with_message(format!(
                    "Vast workload on instance {instance_id} reached {last_status}."
                ));
                return Ok(instance);
            }
            spinner.set_message(format!(
                "Waiting for Vast entrypoint workload on instance {instance_id}... {last_status}"
            ));
        }

        thread::sleep(Duration::from_secs(VAST_POLL_INTERVAL_SECS));
    }
}

pub(crate) fn open_shell_with_auto_key(client: &VastClient, instance: &VastInstance) -> Result<()> {
    open_remote_shell_with_auto_key(client, instance, None)
}

pub(crate) fn open_remote_shell_with_auto_key(
    client: &VastClient,
    instance: &VastInstance,
    remote_command: Option<&str>,
) -> Result<()> {
    with_auto_key(client, instance, |identity| {
        run_ssh_command(instance, identity, remote_command, true)
    })
}

pub(crate) fn ensure_instance_has_ssh(instance: &VastInstance) -> Result<()> {
    let _ = ssh_target(instance)?;
    Ok(())
}

pub(crate) fn run_download_with_auto_key(
    client: &VastClient,
    instance: &VastInstance,
    remote_path: &str,
    local_path: Option<&Path>,
) -> Result<()> {
    let (host, port) = ssh_target(instance)?;
    with_auto_key(client, instance, |identity| {
        run_rsync_download(
            RemoteAccess {
                user: "root",
                host: &host,
                port: Some(port),
                identity_file: identity,
            },
            remote_path,
            local_path,
            &format!("download from vast.ai instance {}", instance.id),
        )
    })
}

pub(crate) fn stream_logs(
    client: &VastClient,
    instance: &VastInstance,
    tail: u32,
    filter: Option<&str>,
    daemon_logs: bool,
    follow: bool,
) -> Result<()> {
    let mut previous = String::new();
    loop {
        let logs = client.request_logs(instance.id, tail, filter, daemon_logs)?;
        let changed = logs != previous;
        print_log_delta(&mut previous, &logs)?;
        if !follow {
            return Ok(());
        }
        if !changed && let Some(mut current) = client.get_instance(instance.id)? {
            hydrate_instance_workload(&mut current);
            upsert_instance::<CacheModel>(&current);
            if workload_completed(&current) {
                return Ok(());
            }
        }
        thread::sleep(Duration::from_secs(VAST_POLL_INTERVAL_SECS));
    }
}

pub(crate) fn stream_unpack_logs_with_auto_key(
    client: &VastClient,
    instance: &VastInstance,
    tail: u32,
    follow: bool,
) -> Result<()> {
    let remote_command =
        unpack_logs_remote_command(&remote_unpack_dir_for_vast(instance), tail, follow);
    with_auto_key(client, instance, |identity| {
        run_ssh_command(instance, identity, Some(&remote_command), false)
    })
}

pub(crate) fn status_summary(instance: &VastInstance) -> String {
    let mut parts = vec![format!("state={}", instance.state_str())];
    if let Some(actual_status) = instance
        .actual_status
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        parts.push(format!("actual={actual_status}"));
    }
    if let Some(status_msg) = instance
        .status_msg
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        parts.push(truncate_ellipsis(status_msg, 100));
    }
    parts.join(", ")
}

pub(crate) fn collect_existing_visible_names(client: &VastClient) -> Result<HashSet<String>> {
    let instances = client
        .list_instances()?
        .into_iter()
        .map(|mut instance| {
            hydrate_instance_workload(&mut instance);
            instance
        })
        .filter(|instance| instance.label_str().starts_with(ICE_LABEL_PREFIX))
        .collect::<Vec<_>>();
    persist_instances::<CacheModel>(&instances);
    Ok(instances
        .iter()
        .map(|instance| visible_instance_name(instance.label_str()).to_owned())
        .filter(|name| !name.is_empty())
        .collect())
}

pub(crate) fn deploy_unpack(
    config: &IceConfig,
    client: &VastClient,
    instance: &VastInstance,
    source: &str,
) -> Result<()> {
    print_stage(&format!(
        "Materializing unpack bundle from {}",
        display_unpack_source(source)
    ));
    let bundle = materialize_unpack_bundle(config, source)?;
    let remote_dir = remote_unpack_dir_for_vast(instance);
    let result = (|| {
        let prepare = unpack_prepare_remote_dir_command(&remote_dir);
        print_stage("Preparing remote unpack directory");
        with_auto_key(client, instance, |identity| {
            run_ssh_command(instance, identity, Some(&prepare), false)
        })?;
        let (host, port) = ssh_target(instance)?;
        print_stage("Uploading unpack bundle");
        with_auto_key(client, instance, |identity| {
            run_rsync_upload(
                RemoteAccess {
                    user: "root",
                    host: &host,
                    port: Some(port),
                    identity_file: identity,
                },
                &bundle.root,
                &remote_dir,
                &format!("upload unpack bundle to vast.ai instance {}", instance.id),
            )
        })?;
        let start = unpack_start_remote_command(&remote_dir);
        print_stage("Starting unpack workload");
        with_auto_key(client, instance, |identity| {
            run_ssh_command(instance, identity, Some(&start), false)
        })
    })();
    let _ = fs::remove_dir_all(&bundle.root);
    result
}

pub(crate) fn remaining_contract_hours_at(
    instance: &VastInstance,
    scheduled_termination_unix: Option<f64>,
    now: f64,
) -> f64 {
    let contract_remaining = instance.end_date.and_then(|end_date| {
        if end_date > now {
            Some((end_date - now) / 3600.0)
        } else {
            None
        }
    });
    let scheduled_remaining = scheduled_termination_unix.and_then(|time| {
        if time > now {
            Some((time - now) / 3600.0)
        } else {
            None
        }
    });

    match (contract_remaining, scheduled_remaining) {
        (Some(contract), Some(scheduled)) => contract.min(scheduled),
        (Some(contract), None) => contract,
        (None, Some(scheduled)) => scheduled,
        (None, None) => 0.0,
    }
}

pub(crate) fn nearest_scheduled_termination_by_instance(
    jobs: &[VastScheduledJob],
) -> HashMap<u64, f64> {
    let now = now_unix_secs_f64();
    let mut nearest = HashMap::new();
    for job in jobs {
        let Some(instance_id) = job.instance_id else {
            continue;
        };
        let Some(termination_unix) = job_termination_unix(job) else {
            continue;
        };
        if termination_unix <= now {
            continue;
        }
        nearest
            .entry(instance_id)
            .and_modify(|existing: &mut f64| *existing = existing.min(termination_unix))
            .or_insert(termination_unix);
    }
    nearest
}

pub(crate) fn job_termination_unix(job: &VastScheduledJob) -> Option<f64> {
    let start_time = job.start_time?;
    if !job
        .api_endpoint
        .as_deref()
        .unwrap_or("")
        .contains("/api/v0/instances/")
    {
        return None;
    }

    let method = job
        .request_method
        .as_deref()
        .unwrap_or("")
        .trim()
        .to_ascii_uppercase();
    if method == "DELETE" {
        return Some(start_time);
    }
    if method == "PUT" {
        let body = job.request_body.as_ref()?;
        let state = body
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        if state == "stopped" || state == "deleted" {
            return Some(start_time);
        }
    }
    None
}

pub(crate) fn ssh_args(host: &str, port: u16, identity_file: Option<&Path>) -> Vec<String> {
    let mut args = vec![
        "-p".to_owned(),
        port.to_string(),
        "-o".to_owned(),
        "StrictHostKeyChecking=accept-new".to_owned(),
    ];
    if let Some(identity) = identity_file {
        args.push("-i".to_owned());
        args.push(identity.display().to_string());
        args.push("-o".to_owned());
        args.push("IdentitiesOnly=yes".to_owned());
    }
    args.push(format!("root@{host}"));
    args
}

fn remaining_field(
    instance: &VastInstance,
    scheduled_termination_unix: Option<f64>,
    pending_context: bool,
) -> Option<String> {
    if pending_context {
        return Some("resolving auto-stop...".to_owned());
    }
    present_field(&remaining_hours_display(
        instance,
        scheduled_termination_unix,
    ))
    .map(|value| format!("rem {value}"))
}

fn instance_image_ref(instance: &VastInstance) -> Option<&str> {
    instance
        .image_uuid
        .as_deref()
        .or(instance.image.as_deref())
        .map(str::trim)
        .filter(|image| !image.is_empty())
}

fn cached_workload(instance_id: u64) -> Option<InstanceWorkload> {
    load_cache_store::<CacheModel>()
        .entries
        .into_iter()
        .find(|entry| entry.id == instance_id)
        .and_then(|entry| entry.workload)
}

fn workload_display(instance: &VastInstance) -> String {
    infer_workload(instance)
        .map(|workload| workload_display_value(Some(&workload)))
        .unwrap_or_else(|| {
            instance_image_ref(instance)
                .map(str::to_owned)
                .unwrap_or_else(|| "-".to_owned())
        })
}

pub(crate) fn runtype_for_workload(workload: &InstanceWorkload) -> &'static str {
    match workload {
        InstanceWorkload::Shell => "ssh_direct",
        InstanceWorkload::Container(_) => "args",
        InstanceWorkload::Unpack(_) => "ssh_direct",
    }
}

fn resolve_instance_from_cache(
    client: &VastClient,
    cache: &CacheStore,
    identifier: &str,
) -> Result<Option<VastInstance>> {
    match prefix_lookup_indices(
        <CacheModel as CloudCacheModel>::entries(cache),
        identifier,
        |entry| entry.label.as_str(),
    )? {
        crate::model::PrefixLookup::Unique(index) => try_instance_by_id(
            client,
            <CacheModel as CloudCacheModel>::entries(cache)[index].id,
        ),
        crate::model::PrefixLookup::Ambiguous(_) | crate::model::PrefixLookup::None => Ok(None),
    }
}

fn try_instance_by_id(client: &VastClient, id: u64) -> Result<Option<VastInstance>> {
    match client.get_instance_by_id(id) {
        Ok(Some(mut instance)) if instance.label_str().starts_with(ICE_LABEL_PREFIX) => {
            hydrate_instance_workload(&mut instance);
            upsert_instance::<CacheModel>(&instance);
            Ok(Some(instance))
        }
        Ok(Some(_)) => Ok(None),
        Ok(None) => find_list_instance_by_id(client, id),
        Err(err) if should_fallback_to_list_lookup(&err) => find_list_instance_by_id(client, id),
        Err(err) => Err(err),
    }
}

fn find_list_instance_by_id(client: &VastClient, id: u64) -> Result<Option<VastInstance>> {
    let instances = client
        .list_instances()?
        .into_iter()
        .map(|mut instance| {
            hydrate_instance_workload(&mut instance);
            instance
        })
        .collect::<Vec<_>>();
    let instance = instances
        .into_iter()
        .find(|instance| instance.id == id && instance.label_str().starts_with(ICE_LABEL_PREFIX));
    if let Some(instance) = instance.as_ref() {
        upsert_instance::<CacheModel>(instance);
    }
    Ok(instance)
}

fn resolve_instance_from_list(
    instances: Vec<VastInstance>,
    identifier: &str,
) -> Result<VastInstance> {
    match prefix_lookup_indices(&instances, identifier, |instance| instance.label_str())? {
        crate::model::PrefixLookup::Unique(index) => Ok(instances[index].clone()),
        crate::model::PrefixLookup::Ambiguous(indices) => {
            let listing = indices
                .into_iter()
                .map(|index| {
                    let instance = &instances[index];
                    format!(
                        "{} ({})",
                        instance.id,
                        visible_instance_name(instance.label_str())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            bail!("`{identifier}` matched multiple instances: {listing}");
        }
        crate::model::PrefixLookup::None => bail!("No instance matched `{identifier}`."),
    }
}

fn should_fallback_to_list_lookup(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_ascii_lowercase();
    message.contains("http 404") || message.contains("http 405") || message.contains("not found")
}

fn parse_instance_from_value(value: &Value) -> Result<Option<VastInstance>> {
    if value.is_null() {
        return Ok(None);
    }
    if let Some(instance_value) = value.get("instance") {
        return serde_json::from_value::<VastInstance>(instance_value.clone())
            .context("Failed to parse vast instance payload from `instance` key")
            .map(Some);
    }
    if let Some(instances) = value.get("instances").and_then(Value::as_array) {
        if let Some(first) = instances.first() {
            return serde_json::from_value::<VastInstance>(first.clone())
                .context("Failed to parse vast instance payload from `instances[0]`")
                .map(Some);
        }
        return Ok(None);
    }
    if value.is_object() {
        if let Ok(parsed) = serde_json::from_value::<VastInstance>(value.clone()) {
            return Ok(Some(parsed));
        }
    }
    Ok(None)
}

fn run_ssh_command(
    instance: &VastInstance,
    identity_file: Option<&Path>,
    remote_command: Option<&str>,
    allocate_tty: bool,
) -> Result<()> {
    let (host, port) = ssh_target(instance)?;
    wait_for_ssh_port_preflight(instance.id, &host, port, Duration::from_secs(30))?;

    let mut command = Command::new("ssh");
    command.args(ssh_args(&host, port, identity_file));
    if let Some(remote_command) = remote_command {
        if allocate_tty {
            command.arg("-t");
        }
        command.arg(remote_command);
    }

    let status = command
        .status()
        .with_context(|| format!("Failed to run ssh into instance {}", instance.id))?;
    if !status.success() {
        bail!("ssh exited with status {status}");
    }
    Ok(())
}

fn with_auto_key<T, F>(client: &VastClient, instance: &VastInstance, mut action: F) -> Result<T>
where
    F: FnMut(Option<&Path>) -> Result<T>,
{
    match action(None) {
        Ok(value) => Ok(value),
        Err(first_err) => {
            let first_err_text = format!("{first_err:#}");
            if !should_retry_with_auto_key(&first_err_text) {
                return Err(first_err);
            }
            print_stage("SSH access failed with existing keys; attaching a local SSH key");
            let Some((identity, attach_status)) =
                attach_local_ssh_key_to_instance(client, instance.id)?
            else {
                return Err(first_err.context(
                    "Initial SSH attempt failed, and no local SSH keypair was found in `~/.ssh` to attach to this instance.",
                ));
            };

            if let Ok(value) = retry_with_identity(
                &mut action,
                None,
                AUTO_KEY_RETRY_ATTEMPTS,
                AUTO_KEY_RETRY_DELAY,
            ) {
                return Ok(value);
            }
            if let Ok(value) = retry_with_identity(
                &mut action,
                Some(identity.as_path()),
                AUTO_KEY_RETRY_ATTEMPTS,
                AUTO_KEY_RETRY_DELAY,
            ) {
                return Ok(value);
            }

            print_stage("Syncing the local SSH key to the Vast account");
            let account_identity = ensure_local_ssh_key_on_account(client)?;
            if let Ok(value) = retry_with_identity(
                &mut action,
                None,
                AUTO_KEY_RETRY_ATTEMPTS,
                AUTO_KEY_RETRY_DELAY,
            ) {
                return Ok(value);
            }

            let final_identity = account_identity.as_deref().unwrap_or(identity.as_path());
            if let Ok(value) = retry_with_identity(
                &mut action,
                Some(final_identity),
                AUTO_KEY_RETRY_ATTEMPTS,
                AUTO_KEY_RETRY_DELAY,
            ) {
                return Ok(value);
            }

            print_stage("Installing a temporary Vast RSA SSH key");
            let temp_key = TemporarySshKey::generate()?;
            let temp_key_id = client
                .create_account_ssh_key(&temp_key.public_key)
                .context("Failed to create temporary vast.ai account SSH key")?;
            let temp_result = retry_with_identity(
                &mut action,
                Some(temp_key.private_key_path.as_path()),
                AUTO_KEY_RETRY_ATTEMPTS,
                AUTO_KEY_RETRY_DELAY,
            );
            if let Err(err) = client.delete_account_ssh_key(temp_key_id) {
                print_warning(&format!(
                    "Failed to delete temporary vast.ai account SSH key {temp_key_id}: {err:#}"
                ));
            }
            temp_result.with_context(|| {
                let attach_hint = match attach_status {
                    InstanceSshKeyAttachStatus::Attached => "The key attach call succeeded.",
                    InstanceSshKeyAttachStatus::AlreadyAssociated => {
                        "Vast reports an SSH key is already associated with this instance."
                    }
                };
                format!(
                    "Initial SSH attempt failed: {first_err_text}. Retried with instance-level key attach, account-level key sync, and finally a temporary RSA account key, but authentication still failed. Local key: `{}`. {attach_hint}",
                    identity.display()
                )
            })
        }
    }
}

const AUTO_KEY_RETRY_ATTEMPTS: usize = 8;
const AUTO_KEY_RETRY_DELAY: Duration = Duration::from_secs(3);

struct TemporarySshKey {
    dir: PathBuf,
    private_key_path: PathBuf,
    public_key: String,
}

impl TemporarySshKey {
    fn generate() -> Result<Self> {
        let dir = std::env::temp_dir().join(format!(
            "ice-vast-key-{}-{}",
            std::process::id(),
            now_unix_secs()
        ));
        fs::create_dir(&dir)
            .with_context(|| format!("Failed to create temporary key dir {}", dir.display()))?;
        let private_key_path = dir.join("id_rsa");
        let status = Command::new("ssh-keygen")
            .args([
                "-q",
                "-t",
                "rsa",
                "-b",
                "4096",
                "-N",
                "",
                "-C",
                "ice-vast-temp",
                "-f",
            ])
            .arg(&private_key_path)
            .status()
            .context("Failed to run `ssh-keygen` for temporary Vast SSH key")?;
        if !status.success() {
            bail!(
                "`ssh-keygen` exited with status {status} while generating a temporary Vast SSH key."
            );
        }
        let public_key_path = private_key_path.with_extension("pub");
        let public_key = fs::read_to_string(&public_key_path)
            .with_context(|| format!("Failed to read {}", public_key_path.display()))?
            .trim()
            .to_owned();
        if public_key.is_empty() {
            bail!(
                "Temporary Vast SSH public key at {} was empty.",
                public_key_path.display()
            );
        }
        Ok(Self {
            dir,
            private_key_path,
            public_key,
        })
    }
}

impl Drop for TemporarySshKey {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

fn retry_with_identity<T, F>(
    action: &mut F,
    identity: Option<&Path>,
    attempts: usize,
    delay: Duration,
) -> Result<T>
where
    F: FnMut(Option<&Path>) -> Result<T>,
{
    let mut last_error = None;
    for attempt in 0..attempts {
        match action(identity) {
            Ok(value) => return Ok(value),
            Err(err) => {
                let error_text = format!("{err:#}");
                if !should_retry_with_auto_key(&error_text) || attempt + 1 == attempts {
                    return Err(err);
                }
                last_error = Some(err);
                thread::sleep(delay);
            }
        }
    }
    Err(last_error.expect("retry_with_identity must record an error before exhausting attempts"))
}

fn should_retry_with_auto_key(error_text: &str) -> bool {
    [
        "Permission denied",
        "permission denied",
        "publickey",
        "connection refused",
        "Connection refused",
        "Connection reset",
        "No route to host",
        "timed out",
        "ssh exited with status exit status: 255",
        "command exited with status exit status: 255",
        "Failed to run ssh",
    ]
    .iter()
    .any(|needle| error_text.contains(needle))
}

fn wait_for_ssh_port_preflight(
    instance_id: u64,
    host: &str,
    port: u16,
    timeout: Duration,
) -> Result<()> {
    let start = SystemTime::now();
    let mut last_error = None;
    loop {
        if elapsed_since(start)? >= timeout {
            bail!(
                "SSH endpoint for instance {instance_id} is not accepting connections yet ({host}:{port}). Last issue: {}",
                last_error.unwrap_or_else(|| "unknown network error".to_owned())
            );
        }

        match crate::support::tcp_port_open(host, port, Duration::from_secs(3)) {
            Ok(()) => return Ok(()),
            Err(err) => last_error = Some(err.to_string()),
        }

        thread::sleep(Duration::from_secs(2));
    }
}

fn attach_local_ssh_key_to_instance(
    client: &VastClient,
    instance_id: u64,
) -> Result<Option<(PathBuf, InstanceSshKeyAttachStatus)>> {
    let Some((private_key_path, public_key)) = discover_local_ssh_keypair()? else {
        return Ok(None);
    };
    Ok(Some((
        private_key_path,
        client
            .attach_instance_ssh_key(instance_id, &public_key)
            .with_context(|| {
                format!("Failed to attach local SSH key to vast.ai instance {instance_id}")
            })?,
    )))
}

fn ensure_local_ssh_key_on_account(client: &VastClient) -> Result<Option<PathBuf>> {
    let Some((private_key_path, public_key)) = discover_local_ssh_keypair()? else {
        return Ok(None);
    };
    client
        .ensure_account_ssh_key(&public_key)
        .context("Failed to ensure local SSH key is present on vast.ai account")?;
    Ok(Some(private_key_path))
}

fn ssh_target(instance: &VastInstance) -> Result<(String, u16)> {
    let host = instance
        .ssh_host
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| anyhow!("Instance {} has no ssh_host", instance.id))?
        .to_owned();
    let port = instance
        .ssh_port
        .ok_or_else(|| anyhow!("Instance {} has no ssh_port", instance.id))?;
    Ok((host, port))
}

fn workload_completed(instance: &VastInstance) -> bool {
    instance
        .actual_status
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some_and(|status| {
            matches!(
                status.to_ascii_lowercase().as_str(),
                "exited" | "dead" | "stopped" | "error"
            )
        })
}

fn print_log_delta(previous: &mut String, current: &str) -> Result<()> {
    let mut stdout = io::stdout().lock();
    if current.is_empty() {
        if previous.is_empty() {
            writeln!(stdout, "(no logs yet)")?;
        }
    } else if let Some(delta) = current.strip_prefix(previous.as_str()) {
        if !delta.is_empty() {
            write!(stdout, "{delta}")?;
            if !delta.ends_with('\n') {
                writeln!(stdout)?;
            }
        }
    } else {
        if !previous.is_empty() {
            writeln!(stdout, "\n--- refreshed log snapshot ---")?;
        }
        write!(stdout, "{current}")?;
        if !current.ends_with('\n') {
            writeln!(stdout)?;
        }
    }
    stdout.flush()?;
    previous.clear();
    previous.push_str(current);
    Ok(())
}

fn remaining_hours(instance: &VastInstance, scheduled_termination_unix: Option<f64>) -> f64 {
    remaining_contract_hours_at(instance, scheduled_termination_unix, now_unix_secs_f64())
}

fn remaining_hours_display(
    instance: &VastInstance,
    scheduled_termination_unix: Option<f64>,
) -> String {
    if instance.end_date.is_none() && scheduled_termination_unix.is_none() {
        return "-".to_owned();
    }
    format!(
        "{:.2}h",
        remaining_hours(instance, scheduled_termination_unix).max(0.0)
    )
}
