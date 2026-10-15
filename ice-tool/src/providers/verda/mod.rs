//! Short-lived ordinary GPU VMs. Workload setup and validation are external.
mod catalog;
mod client;
mod deletion;
mod keys;
mod ssh;
#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::automation::{error, recovery};
use crate::cache::CloudCacheModel;
use crate::cli::{CreateArgs, LogsArgs, PullArgs, PushArgs, ShellArgs};
use crate::listing::{ListedInstance, list_state_color};
use crate::model::{Cloud, IceConfig, LoginOutcome};
use crate::providers::{
    CloudInstance, CloudProvider, CommandProvider, CreateProvider, RemoteCloudProvider,
};
use crate::support::{build_cloud_instance_name, now_unix_secs, prompt_confirm};
use crate::workload::InstanceWorkload;
use catalog::{Catalog, Offer};
use client::{Client, Credentials};
use deletion::delete_selected;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const BILLING: &str = "Verda shutdown retains compute billing and storage. Delete the VM to end compute billing; retained volumes continue to incur storage charges.";

pub(crate) struct Provider;

#[derive(Debug, Clone, Deserialize)]
pub(crate) struct Instance {
    id: String,
    hostname: String,
    status: String,
    ip: Option<String>,
    #[serde(default)]
    instance_type: String,
    #[serde(default)]
    image: String,
    #[serde(default)]
    location: String,
    #[serde(default)]
    price_per_hour: Option<f64>,
    #[serde(default)]
    os_volume_id: Option<String>,
    #[serde(default)]
    volume_ids: Vec<String>,
    #[serde(default)]
    ssh_key_ids: Vec<String>,
}

fn valid_id(id: &str) -> bool {
    id.len() == 36
        && id.chars().enumerate().all(|(i, c)| {
            if [8, 13, 18, 23].contains(&i) {
                c == '-'
            } else {
                c.is_ascii_hexdigit()
            }
        })
}

fn resource_path(kind: &str, id: &str) -> Result<String> {
    if !valid_id(id) {
        bail!("Invalid Verda resource ID");
    }
    Ok(format!("/{kind}/{id}"))
}

fn is_not_found(err: &anyhow::Error) -> bool {
    err.downcast_ref::<crate::automation::AgentError>()
        .is_some_and(|e| e.code == "verda_not_found")
}

impl Client {
    fn instance(&self, id: &str, timeout: Duration) -> Result<Option<Instance>> {
        match self.get::<Instance>(&resource_path("instances", id)?, timeout) {
            Ok(instance) if instance.id == id => Ok(Some(instance)),
            Ok(_) => Err(error(
                "verda_response_mismatch",
                "Verda returned a different instance than requested; no action taken.",
                json!({"instance_id":id}),
            )),
            Err(err) if is_not_found(&err) => Ok(None),
            Err(err) => Err(err),
        }
    }

    fn action(&self, id: &str, action: &str, volumes: Option<&[String]>) -> Result<()> {
        resource_path("instances", id)?;
        let mut body = json!({"id": id, "action": action});
        if let Some(volumes) = volumes {
            for id in volumes {
                resource_path("volumes", id)?;
            }
            body["volume_ids"] = json!(volumes);
            body["delete_permanently"] = json!(true);
        }
        let response = self.mutate(Method::PUT, "/instances", &body)?;
        // 204 has no body. 202/207 carry per-instance success/error results.
        if !response.is_null() {
            let confirmed = response.as_array().is_some_and(|rows| {
                rows.len() == 1
                    && rows[0]["instanceId"] == id
                    && rows[0]["action"] == action
                    && rows[0]["status"] == "success"
            });
            if !confirmed {
                return Err(error(
                    "verda_action_failed",
                    "Verda did not confirm the requested instance action. Inspect its current state before retrying.",
                    json!({"instance_id": id, "action": action,
                        "receipt": deletion::receipt_summary(&response, id, action),
                        "mutation_retried": false, "reconcile_before_retry": true,
                        "next_command": "ice list --cloud verda --json"}),
                ));
            }
        }
        Ok(())
    }

    fn wait(&self, id: &str, running: bool, timeout: Duration) -> Result<Instance> {
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(error(
                    "startup_timeout",
                    "Verda readiness deadline elapsed; the resource may still be billable.",
                    cleanup_details(id, None),
                ));
            }
            if let Some(instance) = self.instance(id, remaining.min(REQUEST_TIMEOUT))? {
                recovery("waiting_for_startup", cleanup_details(id, Some(&instance)));
                if (running && instance.is_running()) || (!running && instance.is_stopped()) {
                    return Ok(instance);
                }
                if [
                    "error",
                    "no_capacity",
                    "installation_failed",
                    "discontinued",
                ]
                .contains(&instance.status.as_str())
                {
                    return Err(error(
                        "instance_startup_failed",
                        format!(
                            "Verda instance entered {}. Inspect and delete it; billing may continue.",
                            instance.status
                        ),
                        cleanup_details(id, Some(&instance)),
                    ));
                }
            }
            thread::sleep(
                Duration::from_secs(2).min(deadline.saturating_duration_since(Instant::now())),
            );
        }
    }
}

fn cleanup_details(id: &str, instance: Option<&Instance>) -> Value {
    json!({"instance_id": id, "os_volume_id": instance.and_then(|i| i.os_volume_id.as_ref()),
        "volume_ids": instance.map(|i| &i.volume_ids), "cleanup_required": true,
        "automatic_stop": false, "automatic_delete": false, "billing_may_continue": true,
        "cleanup_command": format!("ice delete --cloud verda {id} --non-interactive --json"),
        "billing_note": BILLING})
}

impl CloudInstance for Instance {
    type ListContext = ();
    fn json_summary(&self) -> Value {
        json!({"id": self.id, "name": self.hostname, "state": self.status,
            "machine": self.instance_type, "image": self.image, "location": self.location,
            "compute_hourly_usd": self.price_per_hour, "ssh_host": self.ip, "ssh_user": "root", "ssh_port": 22,
            "os_volume_id": self.os_volume_id, "volume_ids": self.volume_ids,
            "compute_charges_continue_after_stop": true, "storage_charges_continue_after_stop": true,
            "automatic_stop": false, "automatic_delete": false,
            "workload": {"kind":"shell"}})
    }
    fn cache_key(&self) -> String {
        self.id.clone()
    }
    fn display_name(&self) -> String {
        self.hostname.clone()
    }
    fn state_value(&self) -> &str {
        &self.status
    }
    fn is_running(&self) -> bool {
        self.status == "running"
    }
    fn is_stopped(&self) -> bool {
        self.status == "offline"
    }
    fn workload(&self) -> Option<&InstanceWorkload> {
        Some(&InstanceWorkload::Shell)
    }
    fn render(&self, _: &(), _: bool) -> ListedInstance {
        ListedInstance {
            display_name: self.hostname.clone(),
            state: self.status.clone(),
            color: list_state_color(&self.status, None),
            fields: vec![
                self.instance_type.clone(),
                self.location.clone(),
                "compute + storage billed until deletion".into(),
            ],
            detail_fields: vec![
                format!("verda://{}", self.id),
                "Manual cleanup required".into(),
            ],
            status_note: None,
            text_effect: Default::default(),
        }
    }
}

impl CloudProvider for Provider {
    type Instance = Instance;
    type ProviderContext<'a> = Client;
    const CLOUD: Cloud = Cloud::Verda;
    fn context(config: &IceConfig) -> Result<Client> {
        Client::from_config(config)
    }
    fn list_instances(client: &Client, _: &mut dyn FnMut(String)) -> Result<Vec<Instance>> {
        Ok(client
            .list::<Instance>("/instances")?
            .into_iter()
            .filter(|i| i.hostname.starts_with("ice-"))
            .collect())
    }
    fn sort_instances(instances: &mut [Instance]) {
        instances.sort_by(|a, b| a.hostname.cmp(&b.hostname));
    }
    fn resolve_instance(client: &Client, identifier: &str) -> Result<Instance> {
        if valid_id(identifier) {
            return client
                .instance(identifier, REQUEST_TIMEOUT)?
                .context("Verda instance not found");
        }
        let instances = Self::list_instances(client, &mut |_| {})?;
        let matches: Vec<_> = instances
            .into_iter()
            .filter(|i| {
                i.hostname == identifier
                    || i.hostname
                        .strip_prefix("ice-")
                        .is_some_and(|name| name.starts_with(identifier))
            })
            .collect();
        if matches.len() != 1 {
            bail!(
                "Expected one Verda instance matching `{identifier}`, found {}. Use the full resource ID.",
                matches.len()
            );
        }
        Ok(matches.into_iter().next().unwrap())
    }
    fn set_running(client: &Client, instance: &Instance, running: bool) -> Result<()> {
        recovery(
            if running { "starting" } else { "stopping" },
            cleanup_details(&instance.id, Some(instance)),
        );
        if !running {
            eprintln!("{BILLING}");
        }
        client.action(
            &instance.id,
            if running { "start" } else { "shutdown" },
            None,
        )
    }
    fn wait_for_running_state(
        client: &Client,
        instance: &Instance,
        running: bool,
        timeout: Duration,
    ) -> Result<Instance> {
        client.wait(&instance.id, running, timeout)
    }
    fn observe_instance(client: &Client, instance: &Instance) -> Result<Option<Instance>> {
        client.instance(&instance.id, REQUEST_TIMEOUT)
    }
    fn delete_instance(client: &Client, instance: &Instance) -> Result<()> {
        delete_selected(client, instance, crate::automation::startup_timeout()).map(|_| ())
    }
}

impl RemoteCloudProvider for Provider {
    type CacheModel = CacheModel;
}

pub(crate) struct CacheModel;
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CacheEntry {
    id: String,
    row: ListedInstance,
    at: u64,
}
#[derive(Default, Serialize, Deserialize)]
pub(crate) struct CacheStore {
    entries: Vec<CacheEntry>,
}
impl CloudCacheModel for CacheModel {
    type Instance = Instance;
    type ListContext = ();
    type Entry = CacheEntry;
    type Store = CacheStore;
    const CLOUD: Cloud = Cloud::Verda;
    fn entries(store: &CacheStore) -> &[CacheEntry] {
        &store.entries
    }
    fn entries_mut(store: &mut CacheStore) -> &mut Vec<CacheEntry> {
        &mut store.entries
    }
    fn key_for_entry(entry: &CacheEntry) -> String {
        entry.id.clone()
    }
    fn entry_from_instance(instance: &Instance, at: u64, _: &()) -> Option<CacheEntry> {
        Some(CacheEntry {
            id: instance.id.clone(),
            row: instance.render(&(), false),
            at,
        })
    }
    fn listed_from_entry(entry: &CacheEntry) -> Option<&ListedInstance> {
        Some(&entry.row)
    }
    fn observed_at_unix(entry: &CacheEntry) -> u64 {
        entry.at
    }
}

impl CommandProvider for Provider {
    fn logs(_: &IceConfig, _: &LogsArgs) -> Result<()> {
        Err(error(
            "unsupported_workload",
            "Verda currently supports shell VMs only; managed workload logs are unavailable.",
            json!({"cloud":"verda"}),
        ))
    }
    fn shell(config: &IceConfig, args: &ShellArgs) -> Result<()> {
        ssh::shell(config, args)
    }
    fn push(config: &IceConfig, args: &PushArgs) -> Result<()> {
        ssh::push(config, args)
    }
    fn pull(config: &IceConfig, args: &PullArgs) -> Result<()> {
        ssh::pull(config, args)
    }
}

pub(crate) fn login(config: &mut IceConfig, force: bool) -> Result<LoginOutcome> {
    crate::login::CredentialLogin {
        cloud: Cloud::Verda,
        environment: Credentials::environment()?,
        cached: Credentials::saved(config),
        force,
        interactive: !crate::automation::non_interactive(),
    }.run(
        |credentials| Client::from_credentials(credentials)?.authenticate(),
        || {
            crate::login::begin_prompt(Cloud::Verda, "https://console.verda.com/")?;
            crate::ui::print_notice("Select Credentials → Cloud API credentials → Create. Both values are hidden while you paste them.");
            Ok(Credentials {
                client_id: crate::login::prompt_secret("Verda Client ID")?,
                client_secret: crate::login::prompt_secret("Verda Client Secret")?,
            })
        },
        |credentials| crate::login::save_credentials(config, |updated| {
            updated.auth.verda.client_id = Some(credentials.client_id);
            updated.auth.verda.client_secret = Some(credentials.client_secret);
        }),
    )
}

pub(crate) fn catalog_command(cloud: Cloud, config: &IceConfig) -> Result<()> {
    if cloud != Cloud::Verda {
        bail!("`ice catalog` currently supports --cloud verda only");
    }
    let catalog = Catalog::load(&Client::from_config(config)?)?;
    let result = json!({"catalog": catalog, "rental_type":"on_demand", "prices_are_estimates":true, "billing_note":BILLING});
    if crate::output::is_json() {
        crate::output::emit("catalog", cloud, result)
    } else {
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}

fn preview(offer: &Offer, hours: f64, now: u64) -> Result<Value> {
    let seconds = hours * 3600.0;
    if !seconds.is_finite() || seconds <= 0.0 || seconds > (u64::MAX - now) as f64 {
        bail!("Invalid planned Verda runtime");
    }
    let total = offer.hourly_usd * hours;
    if !total.is_finite() {
        bail!("Invalid Verda cost estimate");
    }
    let planned_deadline = now
        .checked_add(seconds.ceil() as u64)
        .context("Verda planned deadline overflows")?;
    Ok(
        json!({"status":"preview", "dry_run":true, "rental_type":"on_demand", "offer": offer,
        "cost":{"currency":"USD", "hourly_usd":offer.hourly_usd, "compute_hourly_usd":offer.compute_hourly_usd,
            "storage_hourly_usd":offer.storage_hourly_usd, "requested_hours":hours, "estimated_total_usd":total,
            "cost_scope":"compute_and_os_storage", "bandwidth_included":false, "taxes_included":false},
        "cleanup":{"mode":"manual", "planned_deadline_unix":planned_deadline,
            "deadline_enforced":false, "automatic_stop":false, "automatic_delete":false,
            "required_creation_flag":"--manual-cleanup", "billing_note":BILLING},
        "image":{"id":offer.image.id,"reference":offer.image.image_type,"source":"provider_catalog","local_upload":false}}),
    )
}

pub(crate) fn validate_create(args: &CreateArgs) -> Result<()> {
    Instant::now()
        .checked_add(Duration::from_secs(args.startup_timeout))
        .context("Startup timeout is out of range")?;
    if !args.ssh
        || args.target.is_some()
        || args.container.is_some()
        || args.unpack.is_some()
        || args.arca.is_some()
    {
        return Err(error(
            "unsupported_workload",
            "Verda currently supports create --ssh only; containers, unpack and arca are unsupported.",
            json!({"supported_modes":["ssh"]}),
        ));
    }
    if args.custom {
        bail!("Use explicit filter flags for Verda instead of --custom");
    }
    if !args.dry_run && !args.manual_cleanup {
        return Err(error(
            "cleanup_acknowledgement_required",
            "Verda has no Ice-enforced deadline. Arrange deletion and pass --manual-cleanup; --hours only estimates cost.",
            json!({"required_flags":["--manual-cleanup"], "automatic_delete":false}),
        ));
    }
    Ok(())
}

#[derive(Deserialize)]
struct SshKey {
    id: String,
    #[serde(default)]
    key: Option<String>,
}

fn create_vm(client: &Client, offer: &Offer, hostname: &str, key: &str) -> Result<String> {
    let body = json!({"instance_type":offer.machine.instance_type, "image":offer.image.id,
        "location_code":offer.location, "hostname":hostname, "ssh_key_ids":[key],
        "contract":"PAY_AS_YOU_GO", "is_spot":false,
        "os_volume":{"name":format!("{hostname}-os"),"size":offer.allocated_disk_gb},
        "tags":[{"key":"managed-by","value":"ice"}]});
    recovery(
        "accepting_offer",
        json!({"instance_name":hostname, "cloud":"verda", "cleanup_required":true,
        "automatic_delete":false, "next_command":"ice list --cloud verda --json", "reconcile_before_retry":true}),
    );
    let unknown = || {
        error(
            "creation_outcome_unknown",
            "Verda create did not return a usable receipt. Do not repeat creation: find the recorded hostname in ice list or the Verda console, inspect its volumes, then delete unwanted resources.",
            json!({"instance_name":hostname, "instance_id":null, "reconcile_before_retry":true,"cleanup_required":true,"next_command":"ice list --cloud verda --json"}),
        )
    };
    let response = client
        .mutate(Method::POST, "/instances", &body)
        .map_err(|_| unknown())?;
    let id = response
        .as_str()
        .or_else(|| response.get("id").and_then(Value::as_str))
        .filter(|id| valid_id(id))
        .ok_or_else(unknown)?;
    recovery("waiting_for_startup", cleanup_details(id, None));
    Ok(id.to_owned())
}

impl CreateProvider for Provider {
    fn create(config: &mut IceConfig, args: &CreateArgs) -> Result<()> {
        Self::create_with_client(config, args, Client::from_config)
    }
}

impl Provider {
    // Exercise the real creation flow against a mock endpoint without changing
    // credential handling or adding a configurable production API URL.
    fn create_with_client(
        config: &mut IceConfig,
        args: &CreateArgs,
        connect: impl FnOnce(&IceConfig) -> Result<Client>,
    ) -> Result<()> {
        validate_create(args)?;
        catalog::required_image(&config.default.verda)?;
        let creation_key = if args.dry_run {
            None
        } else {
            Some(keys::creation_key_id(config)?)
        };
        let started = Instant::now();
        let requirements = crate::provision::build_search_requirements(config, Cloud::Verda)?;
        let hours = crate::workload::resolve_deploy_hours(config, args.hours)?;
        let client = connect(config)?;
        let offer = Catalog::load(&client)?.select(
            &requirements,
            &config.default.verda,
            args.machine.as_deref(),
        )?;
        let discovered = Instant::now();
        let mut result = preview(&offer, hours, now_unix_secs())?;
        if args.dry_run {
            if crate::output::is_json() {
                return crate::output::emit("create", Cloud::Verda, result);
            }
            println!("{}", serde_json::to_string_pretty(&result)?);
            return Ok(());
        }
        eprintln!(
            "Verda on-demand {} in {}: ${:.4}/hour including {} GB OS storage. {BILLING}",
            offer.machine.instance_type, offer.location, offer.hourly_usd, offer.allocated_disk_gb
        );
        if !args.yes && !prompt_confirm("Create this VM with manual cleanup?", false)? {
            bail!("Creation cancelled");
        }
        let key = creation_key.context("Creation requires a checked SSH key ID")?;
        let identity = keys::creation_identity(&client, config, key)?;
        let names = client
            .list::<Instance>("/instances")?
            .into_iter()
            .map(|i| i.hostname)
            .collect::<HashSet<_>>();
        let hostname = build_cloud_instance_name(&names)?;
        // Refresh immediately before the billable mutation, including storage and availability.
        let mut pinned = config.default.verda.clone();
        pinned.image = Some(offer.image.id.clone());
        pinned.location = Some(offer.location.clone());
        let current = Catalog::load(&client)?.select(
            &requirements,
            &pinned,
            Some(&offer.machine.instance_type),
        )?;
        if current.hourly_usd > offer.hourly_usd {
            return Err(error(
                "price_changed",
                "Verda price increased after preview/confirmation; preview again before creating.",
                json!({"previous_hourly_usd":offer.hourly_usd,"current_hourly_usd":current.hourly_usd}),
            ));
        }
        result = preview(&current, hours, now_unix_secs())?;
        let prepared = Instant::now();
        let id = create_vm(&client, &current, &hostname, key)?;
        let accepted = Instant::now();
        eprintln!("Created Verda instance {id}. Manual cleanup: ice delete --cloud verda {id}");
        let deadline = Instant::now() + crate::automation::startup_timeout();
        let ready = (|| {
            let instance = client.wait(
                &id,
                true,
                deadline.saturating_duration_since(Instant::now()),
            )?;
            ssh::wait_ready(&instance, identity.as_deref(), deadline)?;
            Ok(instance)
        })();
        let instance: Instance = ready.map_err(|err: anyhow::Error| {
            let message = format!("Verda created instance {id}, but readiness failed: {err}. It remains billable; use the cleanup command and inspect retained volumes.");
            if capulus::error_is_cancelled(&err) { return err.context(message); }
            let mut details = crate::automation::recovery_details();
            details["timings_ms"] = json!({"discovery":discovered.duration_since(started).as_millis(), "preflight_and_confirmation":prepared.duration_since(discovered).as_millis(), "create_request":accepted.duration_since(prepared).as_millis(), "readiness":accepted.elapsed().as_millis(), "total":started.elapsed().as_millis()});
            if let Some(cause) = err.downcast_ref::<crate::automation::AgentError>() {
                details["cause"] = json!({"code":cause.code,"details":cause.details});
            }
            error("creation_incomplete", message, details)
        })?;
        crate::cache::upsert_instance::<CacheModel>(&instance);
        result["timings_ms"] = json!({"discovery":discovered.duration_since(started).as_millis(), "preflight_and_confirmation":prepared.duration_since(discovered).as_millis(), "create_request":accepted.duration_since(prepared).as_millis(), "readiness":accepted.elapsed().as_millis(), "total":started.elapsed().as_millis()});
        result["status"] = json!("created");
        result["dry_run"] = json!(false);
        result["instance"] = instance.json_summary();
        result["readiness"] = json!("ssh_verified");
        result["connect_command"] = json!(ssh::connection_command(&instance, identity.as_deref())?);
        result["cleanup"]["command"] =
            cleanup_details(&id, Some(&instance))["cleanup_command"].clone();
        if crate::output::is_json() {
            crate::output::emit("create", Cloud::Verda, result)
        } else {
            println!(
                "Created {hostname} ({id}). {}",
                result["connect_command"].as_str().unwrap()
            );
            eprintln!("Manual cleanup: ice delete --cloud verda {id}.");
            Ok(())
        }
    }
}

pub(crate) fn delete(config: &IceConfig, identifier: &str) -> Result<()> {
    recovery(
        "resolving_instance",
        json!({"action":"delete", "instance_identifier":identifier,"request":"not_sent"}),
    );
    let client = Client::from_config(config)?;
    let instance = Provider::resolve_instance(&client, identifier)?;
    let started = Instant::now();
    let cleanup = delete_selected(
        &client,
        &instance,
        crate::lifecycle::remaining_timeout(Duration::from_secs(900))?,
    ).map_err(|err| {
        if capulus::error_is_cancelled(&err) { return err; }
        let cause = err.downcast_ref::<crate::automation::AgentError>();
        error("operation_unverified", "Verda deletion or storage cleanup remains unverified. Inspect the resource IDs before retrying; billing may continue.",
            json!({"action":"delete","instance_id":instance.id,"instance":instance.json_summary(),"outcome":"unverified",
                "mutation_retried":false,"reconcile_before_retry":true,"elapsed_ms":started.elapsed().as_millis(),
                "provider_details":cause.map(|e| &e.details),"verification_error_code":cause.map(|e| e.code).unwrap_or("provider_error")}))
    })?;
    let mut result = crate::lifecycle::verified(
        crate::lifecycle::Action::Delete,
        instance.id.clone(),
        if cleanup["action_receipt_confirmed"] == true {
            "acknowledged"
        } else {
            "unconfirmed"
        },
        started,
    );
    result["instance"] = Value::Null;
    result["storage"] = json!({"verification":if cleanup["storage_cleanup_known"] == true {"read_back"} else {"unknown"},
        "deleted_volume_ids":cleanup["permanently_deleted_os_volume_ids"], "retained_volume_ids":cleanup["retained_volume_ids"],
        "charges_may_continue":cleanup["retained_storage_charges_continue"]});
    result["provider_details"] = cleanup;
    crate::cache::remove_instance::<CacheModel>(&instance);
    if crate::output::is_json() {
        crate::output::emit("delete", Cloud::Verda, result)
    } else {
        println!("{}", serde_json::to_string_pretty(&result)?);
        Ok(())
    }
}
