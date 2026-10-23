use super::*;

fn value_type(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Keep structure and comparisons, never arbitrary provider text or field names.
pub(super) fn receipt_summary(value: &Value, id: &str, action: &str) -> Value {
    fn row(value: &Value, id: &str, action: &str) -> Value {
        let fields = ["instanceId", "action", "status", "error", "statusCode"];
        let types: serde_json::Map<String, Value> = fields
            .iter()
            .filter_map(|name| {
                value
                    .get(name)
                    .map(|v| ((*name).into(), json!(value_type(v))))
            })
            .collect();
        json!({"type": value_type(value), "fields": types,
            "unknown_field_count": value.as_object().map(|o| o.keys().filter(|k| !fields.contains(&k.as_str())).count()),
            "instance_id_matches": value.get("instanceId").map(|v| v == id),
            "action_matches": value.get("action").map(|v| v == action),
            "status": match value["status"].as_str() {
                Some("success") => "success", Some("error") => "error", _ => "unrecognized_or_missing"
            },
            "status_code": value["statusCode"].as_u64().filter(|n| (100..=599).contains(n))})
    }
    if let Some(rows) = value.as_array() {
        json!({"type":"array", "row_count":rows.len(),
            "rows":rows.iter().take(4).map(|v| row(v, id, action)).collect::<Vec<_>>()})
    } else {
        row(value, id, action)
    }
}

fn failure_summary(err: &anyhow::Error) -> Value {
    if let Some(agent) = err.downcast_ref::<crate::automation::AgentError>() {
        json!({"code":agent.code, "details":agent.details})
    } else {
        // Parsing and transport errors may contain response data. Do not echo them.
        json!({"code":"unclassified_response_error"})
    }
}

#[derive(Deserialize)]
struct Record {
    id: String,
    #[serde(default)]
    status: Option<String>,
    #[serde(default)]
    is_permanently_deleted: Option<bool>,
}

fn deleted_record(
    client: &Client,
    kind: &str,
    id: &str,
    deadline: Instant,
) -> Result<Option<&'static str>> {
    let record = client.get::<Record>(
        &resource_path(kind, id)?,
        deadline
            .saturating_duration_since(Instant::now())
            .min(REQUEST_TIMEOUT),
    );
    match record {
        Err(err) if is_not_found(&err) => Ok(Some("absent")),
        Err(err) => Err(err),
        Ok(record) if record.id != id => Err(error(
            "verda_response_mismatch",
            "Verda returned a different resource during deletion verification.",
            json!({"resource_kind":kind,"resource_id":id}),
        )),
        Ok(record) => Ok(
            if kind == "instances" && record.status.as_deref() == Some("discontinued") {
                Some("discontinued")
            } else if kind == "volumes"
                && record.status.as_deref() == Some("deleted")
                && record.is_permanently_deleted == Some(true)
            {
                Some("permanently_deleted")
            } else {
                None
            },
        ),
    }
}

fn lists_absent(client: &Client, path: &str, ids: &[String], deadline: Instant) -> Result<bool> {
    // Deserialize IDs even on unrelated rows: malformed pages cannot establish absence.
    Ok(!client
        .list_until::<Record>(path, deadline)?
        .iter()
        .any(|v| ids.contains(&v.id)))
}

fn verify(
    client: &Client,
    instance: &Instance,
    os: &[String],
    retained: &[String],
    deadline: Instant,
    details: &Value,
) -> Result<Value> {
    let cancellation = capulus::Cancellation::install()?;
    loop {
        cancellation.check()?;
        if Instant::now() >= deadline {
            return Err(error(
                "deletion_unverified",
                "Verda deletion was not verified before the deadline. Billing may continue; inspect the recorded IDs before retrying.",
                details.clone(),
            ));
        }
        if let Some(vm_state) = deleted_record(client, "instances", &instance.id, deadline)? {
            let mut volume_states = serde_json::Map::new();
            for id in os {
                if let Some(state) = deleted_record(client, "volumes", id, deadline)? {
                    volume_states.insert(id.clone(), json!(state));
                }
            }
            if volume_states.len() == os.len()
                && lists_absent(
                    client,
                    "/instances",
                    std::slice::from_ref(&instance.id),
                    deadline,
                )?
                && lists_absent(client, "/volumes", os, deadline)?
            {
                if !lists_absent(client, "/volumes/trash", os, deadline)? {
                    return Err(error(
                        "storage_cleanup_unverified",
                        "The VM is deleted, but its OS volume remains in Verda trash. Inspect the recorded IDs and permanently delete the volume using Verda.",
                        details.clone(),
                    ));
                }
                let terminal =
                    vm_state != "absent" || volume_states.values().any(|v| v != "absent");
                return Ok(json!({"status":"deleted", "instance_id":instance.id,
                    "verification": if terminal {"terminal_deletion_and_active_absence_verified"} else {"read_back_absent"},
                    "instance_state":vm_state, "os_volume_states":volume_states,
                    "active_instances_absent":true,"active_os_volumes_absent":true,"os_volumes_absent_from_trash":true,
                    "permanently_deleted_os_volume_ids":os,"retained_volume_ids":retained,
                    "storage_cleanup_known":instance.os_volume_id.is_some(),
                    "retained_storage_charges_continue":!retained.is_empty() || instance.os_volume_id.is_none()}));
            }
        }
        cancellation.sleep(
            Duration::from_secs(2).min(deadline.saturating_duration_since(Instant::now())),
        )?;
    }
}

pub(super) fn delete_selected(
    client: &Client,
    instance: &Instance,
    timeout: Duration,
) -> Result<Value> {
    let os = instance.os_volume_id.iter().cloned().collect::<Vec<_>>();
    let retained = instance
        .volume_ids
        .iter()
        .filter(|id| !os.contains(id))
        .cloned()
        .collect::<Vec<_>>();
    let mut details = json!({"instance_id":instance.id,"os_volume_ids":os,"retained_volume_ids":retained,
        "storage_cleanup_known":instance.os_volume_id.is_some(), "billing_note":BILLING,
        "request":"unconfirmed","mutation_retried":false,"reconcile_before_retry":true,
        "next_command":"Inspect the instance and volume IDs in the Verda console before retrying cleanup."});
    recovery("deleting", details.clone());
    // Even a rejected/unreadable receipt can follow a completed deletion. Read
    // back within the normal verification budget; never resend the mutation.
    let action_error = client.action(&instance.id, "delete", Some(&os)).err();
    details["request"] = json!(if action_error.is_none() {
        "acknowledged"
    } else {
        "unconfirmed"
    });
    if let Some(err) = &action_error {
        details["action_error"] = failure_summary(err);
    }
    recovery("verifying_deletion", details.clone());
    match verify(
        client,
        instance,
        &os,
        &retained,
        Instant::now() + timeout,
        &details,
    ) {
        Ok(mut receipt) => {
            receipt["action_receipt_confirmed"] = json!(action_error.is_none());
            receipt["mutation_retried"] = json!(false);
            if action_error.is_some() {
                receipt["action_error"] = details["action_error"].clone();
            }
            Ok(receipt)
        }
        Err(err) if capulus::error_is_cancelled(&err) => Err(err),
        Err(err) => {
            let code = if err
                .downcast_ref::<crate::automation::AgentError>()
                .is_some_and(|e| e.code == "storage_cleanup_unverified")
            {
                "storage_cleanup_unverified"
            } else {
                "deletion_unverified"
            };
            details["verification_error"] = failure_summary(&err);
            Err(error(
                code,
                "Verda deletion could not be verified. Billing may continue; inspect the recorded VM and volume IDs before retrying.",
                details,
            ))
        }
    }
}
