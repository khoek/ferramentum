use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{self, IsTerminal, Read, Write};
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::blocking::{Client, RequestBuilder};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::cache::{CloudCacheModel, load_cache_store, persist_instances, upsert_instance};
use crate::cli::{CreateArgs, LogsArgs, PullArgs, PushArgs, ShellArgs};
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
    build_vast_autostop_plan, estimate_runtime_cost, find_cheapest_offer, load_gpu_options,
    print_offer_summary, prompt_adjust_search_filters, prompt_create_search_filters,
    prompt_offer_decision,
};
use crate::remote::{run_rsync_download, run_rsync_upload, run_rsync_upload_path};
use crate::support::{
    ICE_LABEL_PREFIX, VAST_DEFAULT_DISK_GB, VAST_DEFAULT_IMAGE,
    VAST_LOG_READY_POLL_INTERVAL_MILLIS, VAST_LOG_READY_TIMEOUT_SECS, VAST_POLL_INTERVAL_SECS,
    build_cloud_instance_name, elapsed_since, extract_api_error_message, format_unix_utc,
    now_unix_secs, now_unix_secs_f64, parse_json_response, prefix_lookup_indices, prompt_confirm,
    spinner, truncate_ellipsis, visible_instance_name,
};
use crate::ui::{print_stage, print_warning};
use crate::unpack::{
    materialize_unpack_bundle, remote_unpack_dir_for_vast, unpack_logs_remote_command,
    unpack_prepare_remote_dir_command, unpack_shell_remote_command, unpack_start_remote_command,
};
use crate::workload::{
    ContainerImageReference, InstanceWorkload, display_unpack_source, resolve_deploy_hours,
    resolve_deploy_workload, workload_display_value,
};

const VAST_BASE_URL: &str = "https://console.vast.ai";

mod autostop;
mod ssh;

#[derive(Debug, Deserialize)]
struct VastOffersResponse {
    #[serde(default)]
    offers: Vec<VastOffer>,
}

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct VastOffer {
    #[serde(default)]
    pub(crate) gpu_ram: Option<f64>,
    #[serde(default)]
    pub(crate) disk_space: Option<f64>,
    #[serde(default)]
    pub(crate) inet_down: Option<f64>,
    #[serde(default)]
    pub(crate) inet_up: Option<f64>,
    #[serde(default)]
    pub(crate) inet_down_cost: Option<f64>,
    #[serde(default)]
    pub(crate) inet_up_cost: Option<f64>,
    pub(crate) id: u64,
    #[serde(default)]
    pub(crate) machine_id: Option<u64>,
    #[serde(default)]
    pub(crate) host_id: Option<u64>,
    #[serde(default)]
    pub(crate) driver_version: Option<String>,
    #[serde(default)]
    pub(crate) vms_enabled: Option<bool>,
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
}

#[derive(Debug, Deserialize)]
struct VastInstancesResponse {
    // Explicit null is the provider's empty-list form; a missing field is not.
    #[serde(deserialize_with = "Option::deserialize")]
    instances: Option<Vec<VastInstance>>,
    #[serde(default)]
    next_token: Option<String>,
}

fn collect_instance_pages<F>(mut fetch_page: F) -> Result<Vec<VastInstance>>
where
    F: FnMut(Option<&str>) -> Result<VastInstancesResponse>,
{
    let mut instances = Vec::new();
    let mut after_token: Option<String> = None;
    let mut seen_tokens = HashSet::new();
    loop {
        let page = fetch_page(after_token.as_deref())?;
        instances.extend(page.instances.unwrap_or_default());
        let Some(next_token) = page.next_token.filter(|token| !token.is_empty()) else {
            return Ok(instances);
        };
        if !seen_tokens.insert(next_token.clone()) {
            bail!("vast.ai returned a repeated instance pagination token");
        }
        after_token = Some(next_token);
    }
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
    #[serde(default)]
    pub(crate) public_ipaddr: Option<String>,
    #[serde(default)]
    pub(crate) ports: Value,
    #[serde(skip)]
    pub(crate) workload: Option<InstanceWorkload>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub(crate) struct VastScheduledJob {
    #[serde(default)]
    pub(crate) id: Option<u64>,
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
    #[serde(default)]
    pub(crate) end_time: Option<f64>,
    #[serde(default)]
    pub(crate) frequency: Option<String>,
    #[serde(default)]
    pub(crate) day_of_the_week: Option<u32>,
    #[serde(default)]
    pub(crate) hour_of_the_day: Option<u32>,
    #[serde(default)]
    pub(crate) min_of_the_hour: Option<u32>,
    #[serde(default)]
    pub(crate) last_executed_around: Option<f64>,
    #[serde(default)]
    pub(crate) status: Option<String>,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
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
        // Search is made with the same allocated_storage as the create request.
        // Do not fall back to an ambiguous base rate or a reserved-price discount.
        self.quoted_total_hourly_price().unwrap_or(f64::INFINITY)
    }

    pub(crate) fn quoted_total_hourly_price(&self) -> Option<f64> {
        self.search
            .as_ref()
            .and_then(|quote| quote.total_hour)
            .filter(|price| price.is_finite() && *price > 0.0)
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
        collect_instance_pages(|after_token| {
            // v0 listing is retired. Leave select_cols unset so v1 returns the
            // full instance records used by SSH, workload inference and display.
            let mut url = reqwest::Url::parse(&format!("{VAST_BASE_URL}/api/v1/instances/"))
                .context("Invalid vast.ai instance listing URL")?;
            url.query_pairs_mut()
                .append_pair("select_filters", "{}")
                .append_pair("order_by", r#"[{"col":"id","dir":"asc"}]"#)
                .append_pair("limit", "25");
            if let Some(token) = after_token {
                url.query_pairs_mut().append_pair("after_token", token);
            }
            let value =
                self.send_json(|| self.auth(self.http.get(url.clone())), "list instances")?;
            serde_json::from_value(value).context("Failed to parse vast.ai instances response")
        })
    }

    fn list_scheduled_jobs(&self) -> Result<Vec<VastScheduledJob>> {
        let value = self.get_json("/api/v0/commands/schedule_job/", "list scheduled jobs")?;
        autostop::parse_jobs(value)
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

    fn attach_instance_ssh_key(
        &self,
        id: u64,
        ssh_key: &str,
        timeout: Duration,
    ) -> Result<InstanceSshKeyAttachStatus> {
        // A single bounded mutation: if the response is lost, report uncertainty
        // instead of repeating the request or escalating to account-wide keys.
        let response = self
            .auth(
                self.http
                    .post(format!("{VAST_BASE_URL}/api/v0/instances/{id}/ssh/"))
                    .json(&json!({"ssh_key": ssh_key}))
                    .timeout(timeout),
            )
            .send()?;
        let parsed = serde_json::from_value::<VastSimpleResponse>(parse_json_response(
            response,
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
        if parsed.success != Some(true) {
            bail!("Vast did not confirm instance-key attachment");
        }
        Ok(InstanceSshKeyAttachStatus::Attached)
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
        let parsed = serde_json::from_value::<VastLogsResponse>(self.send_json(
            || self.build_logs_request(id, &body),
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

    fn build_logs_request(&self, id: u64, body: &Value) -> RequestBuilder {
        self.auth(
            self.http
                .put(format!(
                    "{VAST_BASE_URL}/api/v0/instances/request_logs/{id}/"
                ))
                .json(body),
        )
    }

    fn ssh_permissions_rejected(&self, id: u64, deadline: Instant) -> Result<bool> {
        let timeout = || -> Result<Duration> {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                bail!("Provider log diagnostic timed out");
            }
            Ok(left)
        };
        // sshd runs in the container: daemon system logs are a different source.
        let response = self
            .build_logs_request(id, &json!({"tail": 200}))
            .timeout(timeout()?)
            .send()?;
        let parsed: VastLogsResponse = serde_json::from_value(parse_json_response(
            response,
            "request SSH diagnostic logs",
        )?)?;
        if parsed.success == Some(false) {
            bail!("Provider rejected log diagnostic request");
        }
        let url = parsed
            .result_url
            .filter(|url| !url.trim().is_empty())
            .ok_or_else(|| anyhow!("Provider did not return a log artifact URL"))?;
        loop {
            let response = self.http.get(&url).timeout(timeout()?).send()?;
            if response.status().is_success() {
                let mut bytes = Vec::new();
                response.take(128 * 1024).read_to_end(&mut bytes)?;
                return Ok(ssh::logs_reject_permissions(&String::from_utf8_lossy(
                    &bytes,
                )));
            }
            if !matches!(response.status().as_u16(), 403 | 404) {
                bail!("Provider log artifact unavailable");
            }
            thread::sleep(Duration::from_millis(250).min(timeout()?));
        }
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

    fn send_json<F>(&self, mut make_request: F, context: &str) -> Result<Value>
    where
        F: FnMut() -> RequestBuilder,
    {
        if crate::lifecycle::deadline().is_some() {
            // A lifecycle mutation is submitted once; reconcile uncertain outcomes.
            let response = make_request()
                .timeout(crate::lifecycle::remaining_timeout(Duration::from_secs(
                    30,
                ))?)
                .send()?;
            return parse_json_response(response, context);
        }
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

    fn json_summary(&self) -> Value {
        json!({
            "id": self.id.to_string(), "name": self.label, "state": self.state_str(),
            "gpu_model": self.gpu_name, "hourly_usd": self.dph_total,
            "image": self.image.as_ref().or(self.image_uuid.as_ref()),
            "ssh_host": self.ssh_host, "ssh_port": self.ssh_port,
            "ssh_endpoints": ssh::endpoints(self),
            "contract_end_unix": self.end_date,
            "workload": crate::output::workload(self.workload.as_ref()),
        })
    }

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

    fn observe_instance(
        context: &Self::ProviderContext<'_>,
        instance: &Self::Instance,
    ) -> Result<Option<Self::Instance>> {
        let current = context.get_instance(instance.id)?;
        anyhow::ensure!(
            current.as_ref().is_none_or(|i| i.id == instance.id),
            "Vast returned a different instance"
        );
        Ok(current)
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
        ) && !args.daemon
            && !args.provider_logs
        {
            if args.filter.is_some() {
                bail!(
                    "`ice logs --filter` requires --provider-logs or --daemon for Vast unpack workloads."
                );
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

        if args.no_probe {
            return print_reported_connection(&instance);
        }
        if args.preserve_ephemeral {
            print_warning(
                "--preserve-ephemeral is no longer needed: Vast SSH recovery only attaches an existing key to the selected instance.",
            );
        }

        if instance.is_stopped() {
            crate::automation::require_running(instance.id)?;
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
                crate::automation::startup_timeout(),
            )?;
        }

        if !instance.is_running() || ssh::endpoints(&instance).is_empty() {
            instance =
                wait_for_ssh_ready(&client, instance.id, crate::automation::startup_timeout())?;
        }
        let remote_command = shell_remote_command(&instance);
        if args.print_creds {
            print_shell_command_with_auto_key(&client, &instance, remote_command.as_deref())
        } else if let Some(remote_command) = remote_command.as_deref() {
            open_remote_shell_with_auto_key(
                &client,
                &instance,
                Some(remote_command),
                args.preserve_ephemeral,
            )
        } else {
            open_shell_with_auto_key(&client, &instance, args.preserve_ephemeral)
        }
    }

    fn pull(config: &IceConfig, args: &PullArgs) -> Result<()> {
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

    fn push(config: &IceConfig, args: &PushArgs) -> Result<()> {
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
                concat!(
                    "Instance `{}` is a Vast entrypoint workload. Uploading files requires SSH ",
                    "access, which Vast does not provide for entrypoint-mode containers."
                ),
                instance.id
            );
        }
        ensure_instance_has_ssh(&instance)?;
        run_upload_with_auto_key(
            &client,
            &instance,
            args.local_path.as_path(),
            args.remote_path.as_deref(),
        )
    }
}

impl CreateProvider for Provider {
    fn create(config: &mut IceConfig, args: &CreateArgs) -> Result<()> {
        let client = client_from_config(config)?;
        let hours = resolve_deploy_hours(config, args.hours)?;
        // Reject unsupported schedule windows before accepting a paid offer.
        build_vast_autostop_plan(now_unix_secs(), hours)?;
        let workload = resolve_deploy_workload(&args.target_request())?;
        let label = build_cloud_instance_name(&collect_existing_visible_names(&client)?)?;
        let create_body = build_create_request(config, &label, &workload)?;

        let mut search = build_search_requirements(config, Cloud::VastAi)?;
        if args.custom {
            prompt_create_search_filters(
                Cloud::VastAi,
                &mut search,
                &load_gpu_options(Cloud::VastAi, Some(&client)),
            )?;
        }

        let mut rejected_offer_ids = HashSet::new();
        let (instance_id, selected_offer, mut selected_cost, available_until) = loop {
            crate::selection::record(&search);
            let planning_start = now_unix_secs();
            let plan = build_vast_autostop_plan(planning_start, hours)?;
            let offer = find_cheapest_offer(
                &client,
                &search,
                plan.runtime_hours,
                args.machine.as_deref(),
                &rejected_offer_ids,
            )?;
            let price = offer.hourly_price();
            let available_until =
                planning_start.saturating_add(offer.duration.unwrap_or(0.0) as u64);
            if !price.is_finite() {
                bail!("Vast returned an offer without usable hourly price.");
            }
            let cost = apply_vast_autostop_cost_estimate(
                estimate_runtime_cost(Cloud::VastAi, price, hours)?,
                planning_start,
            )?;

            if !crate::output::is_json() {
                print_offer_summary(&offer, &cost, &search, plan.stop_at_unix);
            }

            if cost.hourly_usd > search.max_price_per_hr {
                let available_hours = offer
                    .duration
                    .filter(|seconds| seconds.is_finite() && *seconds > 0.0)
                    .map(|seconds| seconds / 3600.0)
                    .unwrap_or(0.0);
                return Err(crate::automation::error(
                    "no_matching_offers",
                    format!(
                        "No offer meets max price ${:.4}/hr. Best matching offer is ${:.4}/hr (est ${:.4} for {:.3}h scheduled, {:.3}h requested). Offer {} is available for {:.3}h.",
                        search.max_price_per_hr,
                        price,
                        cost.total_usd,
                        cost.billed_hours,
                        cost.requested_hours,
                        offer.id,
                        available_hours
                    ),
                    serde_json::json!({}),
                ));
            }

            if args.dry_run {
                if crate::output::is_json() {
                    return crate::output::emit(
                        "create",
                        Cloud::VastAi,
                        json!({
                            "status": "preview", "dry_run": true,
                            "offer": crate::output::vast_offer(&offer),
                            "cost": crate::output::cost(&cost),
                            "cost_scope": "compute_and_allocated_storage",
                            "bandwidth_included": false,
                            "quoted_total_hourly_usd": offer.quoted_total_hourly_price(),
                            "allocated_disk_gb": config.default.vast_ai.disk_gb.unwrap_or(VAST_DEFAULT_DISK_GB as u32),
                            "image": create_body.get("image"),
                            "scheduled_stop_unix": plan.stop_at_unix,
                            "auto_stop": {"verification": "planned", "frequency": "WEEKLY"},
                            "storage_charges_continue_after_stop": true,
                            "workload": crate::output::workload(Some(&workload)),
                        }),
                    );
                }
                println!(
                    "Dry run: best matching offer is {} at ${:.4}/hr, est ${:.4} for {:.3}h scheduled ({:.3}h requested). Aborting before accept/pay/create.",
                    offer.id, price, cost.total_usd, cost.billed_hours, cost.requested_hours
                );
                return Ok(());
            }

            if crate::output::is_json() {
                eprintln!(
                    "Selected offer {} ({}) at ${:.4}/hr.",
                    offer.id,
                    offer.gpu_name(),
                    price
                );
            }
            match if args.yes {
                crate::model::OfferDecision::Accept
            } else {
                prompt_offer_decision(&build_accept_prompt(&cost))?
            } {
                crate::model::OfferDecision::ChangeFilter => {
                    prompt_adjust_search_filters(
                        Cloud::VastAi,
                        &mut search,
                        &load_gpu_options(Cloud::VastAi, Some(&client)),
                    )?;
                }
                crate::model::OfferDecision::Reject => {
                    if crate::output::is_json() {
                        crate::output::emit(
                            "create",
                            Cloud::VastAi,
                            json!({"status": "cancelled"}),
                        )?;
                    } else {
                        println!("Aborted.");
                    }
                    return Ok(());
                }
                crate::model::OfferDecision::Accept => {
                    print_stage("Creating instance from accepted offer");
                    let create_spinner = spinner("Accepting offer and creating instance...");
                    crate::automation::recovery(
                        "accepting_offer",
                        json!({"cloud": "vast.ai", "offer_id": offer.id, "instance_name": label}),
                    );
                    match client.create_instance(offer.id, &create_body) {
                        Ok(instance_id) => {
                            crate::automation::recovery(
                                "scheduling_auto_stop",
                                json!({"cloud": "vast.ai", "instance_id": instance_id.to_string()}),
                            );
                            create_spinner
                                .finish_with_message(format!("Created instance {instance_id}."));
                            break (instance_id, offer, cost, available_until);
                        }
                        Err(err) => {
                            create_spinner.finish_and_clear();
                            rejected_offer_ids.insert(offer.id);
                            print_warning(&format!(
                                "Offer {} acceptance failed: {err:#}",
                                offer.id
                            ));
                            if crate::automation::non_interactive()
                                || args.yes
                                || !io::stdin().is_terminal()
                            {
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
        let auto_stop_spinner = spinner("Scheduling instance auto-stop...");
        let (auto_stop_plan, auto_stop_receipt) =
            autostop::schedule(&client, instance_id, hours, available_until)?;
        auto_stop_spinner.finish_with_message(format!(
            "Auto-stop configuration verified for {} ({:.3}h planned runtime).",
            format_unix_utc(auto_stop_plan.stop_at_unix),
            auto_stop_plan.runtime_hours
        ));

        crate::automation::recovery(
            "waiting_for_startup",
            json!({"scheduled_stop_unix": auto_stop_plan.stop_at_unix, "auto_stop": auto_stop_receipt}),
        );
        if crate::output::is_json() || crate::automation::non_interactive() || args.yes {
            let timeout = crate::automation::startup_timeout();
            let mut instance = match &workload {
                InstanceWorkload::Container(_) => {
                    wait_for_workload_start(&client, instance_id, timeout)?
                }
                _ => wait_for_ssh_ready(&client, instance_id, timeout)?,
            };
            instance.workload = Some(workload.clone());
            upsert_instance::<CacheModel>(&instance);
            if let InstanceWorkload::Unpack(source) = &workload {
                crate::automation::recovery("deploying", json!({}));
                deploy_unpack(config, &client, &instance, source)?;
            }
            if !crate::output::is_json() {
                println!(
                    "Created instance {instance_id}; auto-stop at {}.",
                    format_unix_utc(auto_stop_plan.stop_at_unix)
                );
                return Ok(());
            }
            selected_cost.billed_hours = auto_stop_plan.runtime_hours;
            selected_cost.total_usd = selected_cost.hourly_usd * auto_stop_plan.runtime_hours;
            return crate::output::emit(
                "create",
                Cloud::VastAi,
                json!({
                    "status": "created", "instance": instance.json_summary(),
                    "offer": crate::output::vast_offer(&selected_offer),
                    "cost": crate::output::cost(&selected_cost),
                    "cost_scope": "compute_and_allocated_storage",
                    "bandwidth_included": false,
                    "quoted_total_hourly_usd": selected_offer.quoted_total_hourly_price(),
                    "allocated_disk_gb": config.default.vast_ai.disk_gb.unwrap_or(VAST_DEFAULT_DISK_GB as u32),
                    "image": create_body.get("image"),
                    "scheduled_stop_unix": auto_stop_plan.stop_at_unix,
                    "auto_stop": auto_stop_receipt,
                    "storage_charges_continue_after_stop": true,
                }),
            );
        }

        match &workload {
            InstanceWorkload::Shell => {
                print_stage("Waiting for SSH access");
                let instance =
                    wait_for_ssh_ready(&client, instance_id, crate::automation::startup_timeout())?;
                if prompt_confirm("Open shell in the new instance now?", true)? {
                    print_stage("Opening shell");
                    open_shell_with_auto_key(&client, &instance, false)?;
                }
            }
            InstanceWorkload::Container(_) => {
                print_stage("Waiting for container workload startup");
                let instance = wait_for_workload_start(
                    &client,
                    instance_id,
                    crate::automation::startup_timeout(),
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
                let mut instance =
                    wait_for_ssh_ready(&client, instance_id, crate::automation::startup_timeout())?;
                instance.workload = Some(workload.clone());
                upsert_instance::<CacheModel>(&instance);
                crate::automation::recovery("deploying", json!({}));
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
    let environment_key = std::env::var("VAST_API_KEY")
        .ok()
        .filter(|key| !key.trim().is_empty());
    VastClient::new(
        environment_key
            .as_deref()
            .or(config.auth.vast_ai.api_key.as_deref())
            .ok_or_else(|| {
                crate::automation::error(
                    "authentication_required",
                    "Missing Vast API key. Supply VAST_API_KEY or configure auth.vast_ai.api_key.",
                    json!({"environment": "VAST_API_KEY"}),
                )
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
        "disk": config.default.vast_ai.disk_gb.unwrap_or(VAST_DEFAULT_DISK_GB as u32),
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
            return Err(crate::automation::error(
                "startup_timeout",
                format!(
                    "Timed out waiting for instance {instance_id} to reach state `{desired_state}`."
                ),
                serde_json::json!({"instance_id": instance_id.to_string(), "timeout_seconds": timeout.as_secs()}),
            ));
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
            return Err(crate::automation::error(
                "startup_timeout",
                format!("Timed out waiting for SSH readiness on instance {instance_id}."),
                json!({"instance_id": instance_id.to_string(), "timeout_seconds": timeout.as_secs(), "last_issue": last_issue}),
            ));
        }

        if let Some(mut instance) = client.get_instance(instance_id)? {
            hydrate_instance_workload(&mut instance);
            upsert_instance::<CacheModel>(&instance);
            let endpoints = ssh::endpoints(&instance);
            if instance.is_running() && !endpoints.is_empty() {
                // Preserve creation's reachable-port readiness check, but allow
                // a reported direct endpoint to establish readiness if a relay
                // is unavailable. Already-running shell commands go straight to
                // the bounded SSH recovery path instead of this startup wait.
                for endpoint in endpoints {
                    match crate::support::tcp_port_open(
                        &endpoint.host,
                        endpoint.port,
                        Duration::from_secs(3),
                    ) {
                        Ok(()) => {
                            spinner.finish_with_message(format!(
                                "Instance {instance_id} has a reachable SSH port."
                            ));
                            return Ok(instance);
                        }
                        Err(err) => {
                            last_issue = Some(format!(
                                "{}:{} ({}) not accepting connections ({err})",
                                endpoint.host, endpoint.port, endpoint.kind
                            ))
                        }
                    }
                }
            } else {
                last_issue =
                    Some("Instance is not running or has no reported SSH endpoint".to_owned());
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
            return Err(crate::automation::error(
                "startup_timeout",
                format!(
                    "Timed out waiting for Vast entrypoint workload on instance {instance_id}. Last status: {last_status}"
                ),
                json!({"instance_id": instance_id.to_string(), "timeout_seconds": timeout.as_secs(), "last_status": last_status}),
            ));
        }

        if let Some(mut instance) = client.get_instance(instance_id)? {
            hydrate_instance_workload(&mut instance);
            upsert_instance::<CacheModel>(&instance);
            last_status = status_summary(&instance);
            if instance.actual_status.as_deref().is_some_and(|state| {
                matches!(state.to_ascii_lowercase().as_str(), "error" | "failed")
            }) {
                return Err(crate::automation::error(
                    "startup_failed",
                    format!("Instance {instance_id} failed to start: {last_status}"),
                    json!({"instance_id": instance_id.to_string(), "last_status": last_status}),
                ));
            }
            if instance
                .actual_status
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some_and(|status| {
                    matches!(
                        status.to_ascii_lowercase().as_str(),
                        "running" | "exited" | "stopped"
                    )
                })
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

pub(crate) fn open_shell_with_auto_key(
    client: &VastClient,
    instance: &VastInstance,
    preserve_ephemeral: bool,
) -> Result<()> {
    open_remote_shell_with_auto_key(client, instance, None, preserve_ephemeral)
}

pub(crate) fn open_remote_shell_with_auto_key(
    client: &VastClient,
    instance: &VastInstance,
    remote_command: Option<&str>,
    _preserve_ephemeral: bool,
) -> Result<()> {
    with_ssh_access(client, instance, |connection| {
        run_ssh_command(connection, remote_command, true)
    })
}

fn shell_remote_command(instance: &VastInstance) -> Option<String> {
    matches!(
        instance.workload.as_ref(),
        Some(InstanceWorkload::Unpack(_))
    )
    .then(|| unpack_shell_remote_command(&remote_unpack_dir_for_vast(instance)))
}

fn reported_connection(instance: &VastInstance) -> Result<Value> {
    let endpoints = ssh::endpoints(instance);
    let Some(endpoint) = endpoints.first() else {
        return Err(crate::automation::error(
            "ssh_endpoint_unavailable",
            "Vast has not reported an SSH endpoint for this instance.",
            json!({"instance_id": instance.id.to_string(), "readiness": "unchecked"}),
        ));
    };
    let command =
        ssh::connection_command(endpoint, None, shell_remote_command(instance).as_deref());
    Ok(json!({
        "instance": instance.json_summary(), "connect_command": command,
        "readiness": "unchecked", "endpoint": endpoint,
        "endpoints": endpoints,
    }))
}

fn print_reported_connection(instance: &VastInstance) -> Result<()> {
    let result = reported_connection(instance)?;
    if crate::output::is_json() {
        return crate::output::emit("shell", Cloud::VastAi, result);
    }
    eprintln!("Reported connection only; readiness and credentials have not been checked.");
    println!("{}", result["connect_command"].as_str().unwrap());
    for alternate in ssh::endpoints(instance).iter().skip(1) {
        eprintln!(
            "Alternative ({}): {}",
            alternate.kind,
            ssh::connection_command(alternate, None, shell_remote_command(instance).as_deref())
        );
    }
    Ok(())
}

fn print_shell_command_with_auto_key(
    client: &VastClient,
    instance: &VastInstance,
    remote_command: Option<&str>,
) -> Result<()> {
    let connection = ssh::connect(client, instance)?;
    let command = connection.command(remote_command);
    if crate::output::is_json() {
        return crate::output::emit(
            "shell",
            Cloud::VastAi,
            json!({
                "instance": instance.json_summary(), "connect_command": command,
                "readiness": "verified", "endpoint": connection.endpoint,
                "ssh": connection.diagnostics,
            }),
        );
    }
    println!("{command}");
    Ok(())
}

pub(crate) fn ensure_instance_has_ssh(instance: &VastInstance) -> Result<()> {
    if ssh::endpoints(instance).is_empty() {
        bail!("Instance {} has no reported SSH endpoint", instance.id);
    }
    Ok(())
}

pub(crate) fn run_download_with_auto_key(
    client: &VastClient,
    instance: &VastInstance,
    remote_path: &str,
    local_path: Option<&Path>,
) -> Result<()> {
    with_ssh_access(client, instance, |connection| {
        run_rsync_download(
            connection.access(),
            remote_path,
            local_path,
            &format!("download from vast.ai instance {}", instance.id),
        )
    })
}

pub(crate) fn run_upload_with_auto_key(
    client: &VastClient,
    instance: &VastInstance,
    local_path: &Path,
    remote_path: Option<&str>,
) -> Result<()> {
    with_ssh_access(client, instance, |connection| {
        run_rsync_upload_path(
            connection.access(),
            local_path,
            remote_path,
            &format!("upload to vast.ai instance {}", instance.id),
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
    with_ssh_access(client, instance, |connection| {
        run_ssh_command(connection, Some(&remote_command), false)
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
    let result = with_ssh_access(client, instance, |connection| {
        let prepare = unpack_prepare_remote_dir_command(&remote_dir);
        print_stage("Preparing remote unpack directory");
        run_ssh_command(connection, Some(&prepare), false)?;
        print_stage("Uploading unpack bundle");
        run_rsync_upload(
            connection.access(),
            &bundle.root,
            &remote_dir,
            &format!("upload unpack bundle to vast.ai instance {}", instance.id),
        )?;
        let start = unpack_start_remote_command(&remote_dir);
        print_stage("Starting unpack workload");
        run_ssh_command(connection, Some(&start), false)
    });
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
    args.extend(crate::automation::ssh_options());
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
        anyhow::ensure!(instances.len() <= 1, "Expected at most one Vast instance");
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
    bail!("Unrecognized Vast instance response; absence is unverified")
}

fn run_ssh_command(
    connection: &ssh::Connection,
    remote_command: Option<&str>,
    allocate_tty: bool,
) -> Result<()> {
    let mut command = Command::new("ssh");
    crate::automation::prepare_command(&mut command);
    command.args(ssh_args(
        &connection.endpoint.host,
        connection.endpoint.port,
        connection.identity.as_deref(),
    ));
    if let Some(remote_command) = remote_command {
        if allocate_tty {
            command.arg("-t");
        }
        command.arg(remote_command);
    }
    if crate::output::streaming_logs() {
        return crate::output::run_log_command(&mut command, "stream vast.ai unpack logs");
    }
    // Preserve the interactive shell's terminal and stream human output. JSON
    // deployment output must not precede the final machine-readable document.
    if crate::output::is_json() {
        command.stdout(std::process::Stdio::null());
    }
    let status = command
        .status()
        .context("Failed to run SSH operation on vast.ai instance")?;
    ssh::command_status(status)
}

fn with_ssh_access<T>(
    client: &VastClient,
    instance: &VastInstance,
    action: impl FnOnce(&ssh::Connection) -> Result<T>,
) -> Result<T> {
    let connection = ssh::connect(client, instance)?;
    ssh::operation(&connection, action)
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
    if crate::output::streaming_logs() {
        let (text, reset) = match current.strip_prefix(previous.as_str()) {
            Some(delta) => (delta, false),
            None => (current, true),
        };
        crate::output::log_text("combined", text, reset)?;
        previous.clear();
        previous.push_str(current);
        return Ok(());
    }
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

#[cfg(test)]
mod quote_tests {
    use super::*;

    #[test]
    fn invalid_allocated_storage_quotes_never_fall_back_to_a_base_rate() {
        let mut offer: VastOffer = serde_json::from_value(json!({
            "id": 123, "dph_total": 0.01, "search": {"totalHour": 0.15}
        }))
        .unwrap();
        for invalid in [
            None,
            Some(0.0),
            Some(-0.1),
            Some(f64::NAN),
            Some(f64::INFINITY),
        ] {
            offer.search.as_mut().unwrap().total_hour = invalid;
            assert!(offer.quoted_total_hourly_price().is_none());
            assert!(!offer.hourly_price().is_finite());
        }
    }
}

#[cfg(test)]
mod connection_tests {
    use super::*;

    #[test]
    fn unchecked_details_allow_stopped_instances_and_require_no_ssh_identity() {
        let instance: VastInstance = serde_json::from_value(json!({
            "id": 42, "label": "ice-test", "cur_state": "stopped", "image_runtype": "ssh",
            "ssh_host": "ssh1.vast.ai", "ssh_port": 1234,
            "public_ipaddr": "203.0.113.42", "ports": {"22/tcp": [{"HostPort": "4321"}]}
        }))
        .unwrap();
        let details = reported_connection(&instance).unwrap();
        assert_eq!(details["readiness"], "unchecked");
        assert_eq!(details["instance"]["state"], "stopped");
        assert_eq!(details["endpoints"][1]["port"], 4321);
        assert!(details.get("ssh").is_none());
        let command = details["connect_command"].as_str().unwrap();
        assert!(command.contains("root@ssh1.vast.ai"));
        assert!(!command.contains(" -i "));
    }

    #[test]
    fn missing_reported_endpoint_is_an_error_instead_of_an_invented_command() {
        let instance: VastInstance = serde_json::from_value(json!({"id": 42})).unwrap();
        let err = reported_connection(&instance).unwrap_err();
        let typed = err.downcast_ref::<crate::automation::AgentError>().unwrap();
        assert_eq!(typed.code, "ssh_endpoint_unavailable");
        assert_eq!(typed.details["readiness"], "unchecked");
    }
}

#[cfg(test)]
mod logs_tests {
    use super::*;

    #[test]
    fn logs_request_uses_canonical_put_route_and_json_body() {
        let client = VastClient::new("test-api-key").unwrap();
        let body = json!({"tail": 100, "filter": "sshd", "daemon_logs": true});
        let request = client.build_logs_request(123, &body).build().unwrap();

        // The slashless route returned HTTP 400 for a valid JSON body. Match
        // Vast's official CLI route directly instead of depending on redirects.
        assert_eq!(request.method(), reqwest::Method::PUT);
        assert_eq!(
            request.url().as_str(),
            "https://console.vast.ai/api/v0/instances/request_logs/123/"
        );
        assert_eq!(request.headers()["content-type"], "application/json");
        assert_eq!(
            serde_json::from_slice::<Value>(request.body().unwrap().as_bytes().unwrap()).unwrap(),
            body
        );
    }
}

#[cfg(test)]
mod pagination_tests {
    use super::*;

    #[test]
    fn collects_all_pages_including_an_empty_intermediate_page() {
        let mut pages = vec![
            json!({"instances": [{"id": 1, "image_runtype": "ssh"}],
                   "next_token": "token+/="}),
            json!({"instances": null, "next_token": "last"}),
            json!({"instances": [{"id": 2, "label": "ice-second"}],
                   "next_token": null}),
        ]
        .into_iter();
        let mut requested_tokens = Vec::new();
        let instances = collect_instance_pages(|token| {
            requested_tokens.push(token.map(str::to_owned));
            Ok(serde_json::from_value(
                pages.next().expect("unexpected extra page"),
            )?)
        })
        .unwrap();

        assert_eq!(
            requested_tokens,
            [None, Some("token+/=".to_owned()), Some("last".to_owned())]
        );
        assert_eq!(
            instances.iter().map(|item| item.id).collect::<Vec<_>>(),
            [1, 2]
        );
        assert_eq!(instances[0].image_runtype.as_deref(), Some("ssh"));
        assert_eq!(instances[1].label.as_deref(), Some("ice-second"));
    }

    #[test]
    fn empty_accounts_finish_without_a_continuation_token() {
        for page in [
            json!({"instances": []}),
            json!({"instances": null, "next_token": null}),
            json!({"instances": [], "next_token": ""}),
        ] {
            let instances = collect_instance_pages(|token| {
                assert!(token.is_none());
                Ok(serde_json::from_value(page.clone())?)
            })
            .unwrap();
            assert!(instances.is_empty());
        }
    }

    #[test]
    fn a_failed_later_page_does_not_return_a_partial_listing() {
        let error = collect_instance_pages(|token| {
            if token.is_some() {
                bail!("page fetch failed");
            }
            Ok(serde_json::from_value(json!({
                "instances": [{"id": 1}], "next_token": "next"
            }))?)
        })
        .unwrap_err();
        assert!(error.to_string().contains("page fetch failed"));
    }

    #[test]
    fn repeated_tokens_fail_instead_of_looping_forever() {
        let mut calls = 0;
        let error = collect_instance_pages(|_| {
            calls += 1;
            assert!(calls <= 2);
            Ok(serde_json::from_value(json!({
                "instances": [], "next_token": "repeated"
            }))?)
        })
        .unwrap_err();
        assert_eq!(calls, 2);
        assert!(
            error
                .to_string()
                .contains("repeated instance pagination token")
        );
    }
}

#[cfg(test)]
mod lifecycle_response_tests {
    use super::*;

    #[test]
    fn malformed_instance_data_does_not_establish_absence() {
        for value in [
            json!({}),
            json!({"error":"unavailable"}),
            json!({"instances":"unknown"}),
            json!({"instances":[{"id":1},{"id":2}]}),
        ] {
            assert!(parse_instance_from_value(&value).is_err());
        }
        assert!(
            parse_instance_from_value(&json!({"instances":[]}))
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn missing_instances_field_cannot_become_an_empty_listing() {
        assert!(
            serde_json::from_value::<VastInstancesResponse>(json!({"error":"unavailable"}))
                .is_err()
        );
        assert!(serde_json::from_value::<VastInstancesResponse>(json!({"instances":null})).is_ok());
    }
}
