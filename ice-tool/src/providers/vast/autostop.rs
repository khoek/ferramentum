//! Pin the provider's calendar and verify its stored job before reporting success.
//! Read-back proves configuration, not that the provider will execute on time.
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Datelike, Timelike, Utc};
use serde_json::{Value, json};

use super::{VAST_BASE_URL, VastClient, VastScheduledJob};
use crate::model::VastAutoStopPlan;
use crate::provision::build_vast_autostop_plan;
use crate::support::{now_unix_secs, parse_json_response};

pub(super) fn parse_jobs(value: Value) -> Result<Vec<VastScheduledJob>> {
    let rows = value
        .as_array()
        .or_else(|| value.get("results").and_then(Value::as_array))
        .context("Vast scheduled-job response has no results array")?;
    rows.iter()
        .map(|row| serde_json::from_value(row.clone()).context("Invalid Vast scheduled-job record"))
        .collect()
}

fn request(id: u64, plan: &VastAutoStopPlan) -> Result<Value> {
    let stop = DateTime::<Utc>::from_timestamp(i64::try_from(plan.stop_at_unix)?, 0)
        .context("Vast auto-stop timestamp is out of range")?;
    Ok(json!({
        "instance_id": id, "api_endpoint": format!("/api/v0/instances/{id}/"),
        "request_method": "PUT", "request_body": {"state": "stopped"},
        "start_time": plan.stop_at_unix, "end_time": plan.schedule_end_unix,
        "frequency": "WEEKLY", "day_of_the_week": stop.weekday().num_days_from_sunday(),
        "hour_of_the_day": stop.hour(),
    }))
}

fn summary(job: &VastScheduledJob) -> Value {
    // Project known scheduling fields; never echo an arbitrary request body.
    json!({"job_id": job.id, "instance_id": job.instance_id, "start_time": job.start_time,
        "api_endpoint": job.api_endpoint, "request_method": job.request_method,
        "state": job.request_body.as_ref().and_then(|body| body.get("state")).and_then(Value::as_str),
        "end_time": job.end_time, "frequency": job.frequency, "day_of_the_week": job.day_of_the_week,
        "hour_of_the_day": job.hour_of_the_day, "min_of_the_hour": job.min_of_the_hour,
        "status": job.status, "last_executed_around": job.last_executed_around})
}

fn verify(id: u64, plan: &VastAutoStopPlan, jobs: &[VastScheduledJob], now: u64) -> Result<Value> {
    if now >= plan.stop_at_unix {
        bail!("Auto-stop deadline passed before schedule verification");
    }
    let related = jobs
        .iter()
        .filter(|job| job.instance_id == Some(id))
        .collect::<Vec<_>>();
    if related.len() != 1 {
        bail!(
            "Expected one stored auto-stop job for the new instance, found {}",
            related.len()
        );
    }
    let job = related[0];
    let expected = request(id, plan)?;
    if job.id.is_none_or(|id| id == 0)
        || job.api_endpoint.as_deref() != expected["api_endpoint"].as_str()
        || job.request_method.as_deref() != Some("PUT")
        || job.request_body.as_ref() != Some(&json!({"state": "stopped"}))
        || job.start_time != Some(plan.stop_at_unix as f64)
        || job.end_time != Some(plan.schedule_end_unix as f64)
        || job.frequency.as_deref() != Some("WEEKLY")
        || job.day_of_the_week.map(u64::from) != expected["day_of_the_week"].as_u64()
        || job.hour_of_the_day.map(u64::from) != expected["hour_of_the_day"].as_u64()
        || job.min_of_the_hour != Some(0)
    {
        bail!("Stored auto-stop action, window or UTC calendar does not match the request");
    }
    if job
        .last_executed_around
        .is_some_and(|time| !time.is_finite() || time != 0.0)
    {
        bail!("Vast reports that the new auto-stop job already executed before its deadline");
    }
    if job.status.as_deref().is_some_and(|status| {
        [
            "DISABLED",
            "CANCELLED",
            "CANCELED",
            "FAILED",
            "ERROR",
            "COMPLETED",
            "FINISHED",
        ]
        .iter()
        .any(|invalid| status.eq_ignore_ascii_case(invalid))
    }) {
        bail!("Stored auto-stop job is not pending");
    }
    let mut receipt = summary(job);
    receipt["verification"] = json!("read_back");
    Ok(receipt)
}

trait ScheduleIo {
    fn now(&self) -> u64;
    fn submit(&mut self, body: &Value) -> Result<()>;
    fn jobs(&mut self) -> Result<Vec<VastScheduledJob>>;
    fn stop(&mut self, instance_id: u64) -> Result<()>;
}

impl ScheduleIo for &VastClient {
    fn now(&self) -> u64 {
        now_unix_secs()
    }
    fn submit(&mut self, body: &Value) -> Result<()> {
        // Never replay a scheduling mutation when its outcome is uncertain.
        let response = self
            .auth(
                self.http
                    .post(format!("{VAST_BASE_URL}/api/v0/commands/schedule_job/")),
            )
            .json(body)
            .send()?;
        let value = parse_json_response(response, "schedule Vast auto-stop")?;
        if value.get("success") == Some(&Value::Bool(false)) {
            bail!("Vast rejected the auto-stop request");
        }
        Ok(())
    }
    fn jobs(&mut self) -> Result<Vec<VastScheduledJob>> {
        self.list_scheduled_jobs()
    }
    fn stop(&mut self, id: u64) -> Result<()> {
        self.set_instance_state(id, "stopped")
    }
}

pub(super) fn schedule(
    client: &VastClient,
    id: u64,
    hours: f64,
    available_until: u64,
) -> Result<(VastAutoStopPlan, Value)> {
    let mut io = client;
    install(&mut io, id, hours, available_until)
}

fn install(
    io: &mut impl ScheduleIo,
    id: u64,
    hours: f64,
    available_until: u64,
) -> Result<(VastAutoStopPlan, Value)> {
    let mut details = json!({"instance_id": id.to_string(), "auto_stop_verified": false,
        "storage_charges_continue_after_stop": true,
        "next_command": "ice list --cloud vast.ai --json"});
    let result = (|| {
        let plan = build_vast_autostop_plan(io.now(), hours)?;
        if plan.stop_at_unix > available_until {
            bail!("Offer availability no longer covers the rounded stop deadline");
        }
        let body = request(id, &plan)?;
        details["requested_schedule"] = body.clone();
        io.submit(&body)?;
        let jobs = io.jobs()?;
        details["observed_schedules"] = json!(
            jobs.iter()
                .filter(|job| job.instance_id == Some(id))
                .map(summary)
                .collect::<Vec<_>>()
        );
        let receipt = verify(id, &plan, &jobs, io.now())?;
        Ok((plan, receipt))
    })();
    result.map_err(|err: anyhow::Error| {
        // This is a just-created rental. Stop it before upload/deployment if its
        // guard cannot be established, preserving the instance for inspection.
        let stopped = io.stop(id).is_ok();
        details["stop_request"] = json!(if stopped { "accepted" } else { "failed_or_unknown" });
        crate::automation::error("auto_stop_unverified", format!(
            "Could not verify auto-stop for instance {id}: {err:#}. {} Confirm its state and delete it when no longer needed; storage billing continues.",
            if stopped { "A stop was requested." } else { "The stop request failed or its outcome is unknown; stop the instance explicitly." }
        ), details)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Reproduce the reported 01:38 creation / intended 05:00 stop on Monday UTC.
    const START: u64 = 1_790_559_480;

    fn plan() -> VastAutoStopPlan {
        build_vast_autostop_plan(START, 3.0).unwrap()
    }
    fn stored() -> Value {
        let mut value = request(42, &plan()).unwrap();
        value["id"] = json!(2110);
        value["min_of_the_hour"] = json!(0);
        value["status"] = json!("PENDING");
        value
    }
    fn job(value: Value) -> VastScheduledJob {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn calendar_does_not_match_the_reported_early_hour_even_without_start_time() {
        let request = request(42, &plan()).unwrap();
        assert_eq!(request["start_time"], 1_790_571_600u64);
        assert_eq!(request["frequency"], "WEEKLY");
        assert_eq!(request["day_of_the_week"], 1); // Vast uses Sunday = 0.
        assert_eq!(request["hour_of_the_day"], 5);
        // Every hour between creation and the requested deadline, ignoring
        // start_time altogether, must be excluded by the calendar selectors.
        let first_hour = START.div_ceil(3600) * 3600;
        for at in (first_hour..plan().stop_at_unix).step_by(3600) {
            let date = DateTime::<Utc>::from_timestamp(at as i64, 0).unwrap();
            assert!(
                date.weekday().num_days_from_sunday()
                    != request["day_of_the_week"].as_u64().unwrap() as u32
                    || date.hour() != request["hour_of_the_day"].as_u64().unwrap() as u32
            );
        }
    }

    #[test]
    fn calendar_covers_utc_day_week_and_year_rollovers() {
        for timestamp in [
            "2026-09-30T23:50:00Z",
            "2026-10-03T23:50:00Z",
            "2026-12-31T23:50:00Z",
        ] {
            let start = DateTime::parse_from_rfc3339(timestamp).unwrap().timestamp() as u64;
            for hours in [0.25, 25.0, 167.0] {
                let plan = build_vast_autostop_plan(start, hours).unwrap();
                let body = request(42, &plan).unwrap();
                let matching = (start.div_ceil(3600) * 3600..=plan.stop_at_unix)
                    .step_by(3600)
                    .filter(|at| {
                        let date = DateTime::<Utc>::from_timestamp(*at as i64, 0).unwrap();
                        Some(u64::from(date.weekday().num_days_from_sunday()))
                            == body["day_of_the_week"].as_u64()
                            && Some(u64::from(date.hour())) == body["hour_of_the_day"].as_u64()
                    })
                    .collect::<Vec<_>>();
                assert_eq!(matching, vec![plan.stop_at_unix]);
            }
        }
        for hours in [168.0, 200.0, f64::MAX] {
            assert!(build_vast_autostop_plan(START, hours).is_err());
        }
    }

    #[test]
    fn recorded_early_execution_and_legacy_hourly_jobs_are_rejected() {
        let mut observed = stored();
        observed["frequency"] = json!("HOURLY");
        observed["day_of_the_week"] = Value::Null;
        observed["hour_of_the_day"] = Value::Null;
        observed["last_executed_around"] = json!(1790560828.1913486);
        observed["status"] = json!("IN-PROGRESS");
        assert!(verify(42, &plan(), &[job(observed)], START).is_err());
        let mut executed = stored();
        executed["last_executed_around"] = json!(1790560828.1913486);
        assert!(verify(42, &plan(), &[job(executed)], START).is_err());
    }

    #[test]
    fn read_back_checks_action_calendar_window_and_identity() {
        let baseline = stored();
        assert_eq!(
            verify(42, &plan(), &[job(baseline.clone())], START).unwrap()["job_id"],
            2110
        );
        for (key, value) in [
            ("id", Value::Null),
            ("instance_id", json!(99)),
            ("api_endpoint", json!("/api/v0/instances/99/")),
            ("request_method", json!("DELETE")),
            ("request_body", json!({"state":"running"})),
            ("start_time", json!(plan().stop_at_unix + 3600)),
            ("end_time", json!(plan().schedule_end_unix + 86400)),
            ("frequency", json!("HOURLY")),
            ("day_of_the_week", json!(2)),
            ("hour_of_the_day", json!(2)),
            ("min_of_the_hour", json!(30)),
            ("min_of_the_hour", Value::Null),
            ("status", json!("FAILED")),
        ] {
            let mut changed = baseline.clone();
            changed[key] = value;
            assert!(
                verify(42, &plan(), &[job(changed)], START).is_err(),
                "accepted {key}"
            );
        }
        assert!(verify(42, &plan(), &[], START).is_err());
        assert!(
            verify(
                42,
                &plan(),
                &[job(baseline.clone()), job(baseline.clone())],
                START
            )
            .is_err()
        );
        assert!(verify(42, &plan(), &[job(baseline)], plan().stop_at_unix).is_err());
    }

    struct Fake {
        submitted: Vec<Value>,
        stops: Vec<u64>,
        reads: usize,
        submit_fails: bool,
        lookup_fails: bool,
        stop_fails: bool,
        stored: Value,
    }
    impl Default for Fake {
        fn default() -> Self {
            Self {
                submitted: vec![],
                stops: vec![],
                reads: 0,
                submit_fails: false,
                lookup_fails: false,
                stop_fails: false,
                stored: stored(),
            }
        }
    }
    impl ScheduleIo for Fake {
        fn now(&self) -> u64 {
            START
        }
        fn submit(&mut self, body: &Value) -> Result<()> {
            self.submitted.push(body.clone());
            if self.submit_fails {
                bail!("request outcome unknown");
            }
            Ok(())
        }
        fn jobs(&mut self) -> Result<Vec<VastScheduledJob>> {
            self.reads += 1;
            if self.lookup_fails {
                bail!("read-back unavailable");
            }
            Ok(vec![job(self.stored.clone())])
        }
        fn stop(&mut self, id: u64) -> Result<()> {
            self.stops.push(id);
            if self.stop_fails {
                bail!("stop outcome unknown");
            }
            Ok(())
        }
    }

    #[test]
    fn success_requires_provider_read_back_without_stopping_the_instance() {
        let mut io = Fake::default();
        let (_, receipt) = install(&mut io, 42, 3.0, plan().stop_at_unix).unwrap();
        assert_eq!(receipt["verification"], "read_back");
        assert_eq!(receipt["start_time"], plan().stop_at_unix as f64);
        assert_eq!((io.submitted.len(), io.reads, io.stops.len()), (1, 1, 0));
    }

    #[test]
    fn uncertain_or_mismatched_schedule_requests_stop_only_the_created_instance() {
        for failure in ["submit", "lookup", "mismatch", "stop"] {
            let mut io = Fake::default();
            match failure {
                "submit" => io.submit_fails = true,
                "lookup" => io.lookup_fails = true,
                "mismatch" => io.stored["frequency"] = json!("HOURLY"),
                "stop" => {
                    io.lookup_fails = true;
                    io.stop_fails = true;
                }
                _ => unreachable!(),
            }
            let err = install(&mut io, 42, 3.0, plan().stop_at_unix).unwrap_err();
            let typed = err.downcast_ref::<crate::automation::AgentError>().unwrap();
            assert_eq!(typed.code, "auto_stop_unverified");
            assert_eq!(typed.details["instance_id"], "42");
            assert_eq!(typed.details["auto_stop_verified"], false);
            assert_eq!(
                typed.details["stop_request"],
                if failure == "stop" {
                    "failed_or_unknown"
                } else {
                    "accepted"
                }
            );
            assert_eq!(io.stops, vec![42]);
            assert_eq!(io.submitted.len(), 1);
        }
    }

    #[test]
    fn malformed_job_responses_are_errors_not_an_empty_success() {
        for value in [
            json!({}),
            json!({"results":null}),
            json!({"results":[{"id":"bad"}]}),
            json!({"error":"denied"}),
        ] {
            assert!(parse_jobs(value).is_err());
        }
        assert!(parse_jobs(json!([])).unwrap().is_empty());
        assert_eq!(parse_jobs(json!({"results":[stored()]})).unwrap().len(), 1);
    }

    #[test]
    fn a_delay_that_exceeds_offer_availability_stops_before_scheduling() {
        let mut io = Fake::default();
        assert!(install(&mut io, 42, 3.0, plan().stop_at_unix - 1).is_err());
        assert!(io.submitted.is_empty());
        assert_eq!(io.stops, vec![42]);
    }
}
