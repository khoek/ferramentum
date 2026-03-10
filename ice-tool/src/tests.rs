use std::collections::HashSet;
use std::fs;
use std::path::Path;

use clap::Parser;
use serde_json::json;

use crate::cli::{Cli, Commands};
use crate::config_store::{
    get_config_value, normalize_config_key, parse_cloud, set_config_value, unset_config_value,
};
use crate::gpu::{gpu_quality_score, gpu_selector_label};
use crate::listing::{listed_instance, missing_remote_cached_instance, render_listed_instance};
use crate::local::{
    local_instance_is_managed, local_instance_labels, local_workload_display,
    parse_local_instance_row,
};
use crate::model::{
    Cloud, CloudMachineCandidate, CreateSearchRequirements, DeployTargetRequest, IceConfig,
    PrefixLookup, RuntimeCostEstimate,
};
use crate::providers::RemoteSshProvider;
use crate::providers::aws::AwsInstance;
use crate::providers::gcp::{GcpInstance, GcpMachineCatalogEntry};
use crate::providers::vast::{
    VastInstance, VastScheduledJob, infer_workload, instance_supports_ssh, job_termination_unix,
    nearest_scheduled_termination_by_instance, remaining_contract_hours_at, runtype_for_workload,
    ssh_args,
};
use crate::provision::{
    apply_vast_autostop_cost_estimate, build_search_requirements, build_vast_autostop_plan,
    estimate_runtime_cost, load_gpu_options,
};
use crate::support::{
    ICE_WORKLOAD_CONTAINER_METADATA_KEY, ICE_WORKLOAD_KIND_METADATA_KEY,
    ICE_WORKLOAD_REGISTRY_METADATA_KEY, ICE_WORKLOAD_SOURCE_METADATA_KEY, VAST_DEFAULT_IMAGE,
    generate_unique_verb_noun_name, normalize_instance_name_for_match, now_unix_secs_f64,
    prefix_lookup_indices, required_runtime_seconds,
};
use crate::ui::{Color, StdoutRenderTarget};
use crate::unpack::{
    create_temp_dir, extract_saved_image_layers, load_saved_image_bundle,
    materialize_unpack_bundle_in, render_unpack_run_script, unpack_prepare_remote_dir_command,
    unpack_shell_remote_command,
};
use crate::workload::{
    ContainerImageReference, InstanceWorkload, parse_workload_metadata, resolve_deploy_hours,
    resolve_deploy_workload, workload_metadata_values,
};

fn test_vast_instance(end_date: Option<f64>) -> VastInstance {
    VastInstance {
        id: 42,
        label: Some("ice-test".to_owned()),
        image: None,
        image_uuid: None,
        image_runtype: None,
        cur_state: Some("running".to_owned()),
        next_state: None,
        intended_status: None,
        actual_status: None,
        status_msg: None,
        start_date: None,
        uptime_mins: None,
        gpu_name: None,
        dph_total: None,
        end_date,
        ssh_host: None,
        ssh_port: None,
        workload: None,
    }
}

fn tar_bytes(entries: Vec<(String, Vec<u8>, u32)>) -> Vec<u8> {
    let mut builder = tar::Builder::new(Vec::new());
    for (path, data, mode) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_mode(mode);
        header.set_size(data.len() as u64);
        header.set_cksum();
        builder
            .append_data(&mut header, path, std::io::Cursor::new(data))
            .expect("append tar entry");
    }
    builder.into_inner().expect("finish tar")
}

fn write_test_saved_image_archive(path: &Path) {
    let layer_one = tar_bytes(vec![
        ("usr/local/bin/app".to_owned(), b"app".to_vec(), 0o755),
        ("workspace/obsolete.txt".to_owned(), b"old".to_vec(), 0o644),
    ]);
    let layer_two = tar_bytes(vec![
        ("workspace/.wh.obsolete.txt".to_owned(), Vec::new(), 0o644),
        ("workspace/new.txt".to_owned(), b"new".to_vec(), 0o644),
    ]);
    let config = serde_json::json!({
        "config": {
            "Entrypoint": ["/usr/local/bin/app"],
            "Cmd": ["--flag"],
            "Env": ["EXAMPLE=1"],
            "WorkingDir": "/workspace"
        }
    })
    .to_string();
    let manifest = serde_json::json!([{
        "Config": "blobs/sha256/config",
        "Layers": [
            "blobs/sha256/layer1",
            "blobs/sha256/layer2"
        ]
    }])
    .to_string();

    let archive = tar_bytes(vec![
        ("manifest.json".to_owned(), manifest.into_bytes(), 0o644),
        ("blobs/sha256/config".to_owned(), config.into_bytes(), 0o644),
        ("blobs/sha256/layer1".to_owned(), layer_one, 0o644),
        ("blobs/sha256/layer2".to_owned(), layer_two, 0o644),
    ]);
    fs::write(path, archive).expect("write saved image archive");
}

#[test]
fn unpack_run_script_tracks_child_pid_and_exit_code() {
    let script = render_unpack_run_script(&["/usr/local/bin/app".to_owned(), "--flag".to_owned()]);
    assert!(script.contains("child_pid=$!"));
    assert!(script.contains("printf '%s\\n' \"$child_pid\" > \"$ICE_UNPACK_STATE_DIR/pid\""));
    assert!(script.contains("wait \"$child_pid\""));
    assert!(script.contains("rm -f \"$ICE_UNPACK_STATE_DIR/pid\""));
    assert!(script.contains("printf '%s\\n' \"$status\" > \"$ICE_UNPACK_STATE_DIR/exit-code\""));
}

#[test]
fn unpack_remote_commands_expand_home_dirs() {
    let prepare = unpack_prepare_remote_dir_command("~/.ice/unpack/vast-42");
    assert!(prepare.contains("rm -rf $HOME/.ice/unpack/vast-42"));
    assert!(!prepare.contains("'~/.ice/unpack/vast-42'"));

    let shell = unpack_shell_remote_command("~/.ice/unpack/vast-42");
    assert!(shell.contains("exec sh $HOME/.ice/unpack/vast-42/shell"));
}

#[test]
fn unpack_follow_logs_command_stops_after_exit_code() {
    let command = crate::unpack::unpack_logs_remote_command("~/.ice/unpack/vast-42", 200, true);
    assert!(!command.contains("tail -n 200 -F"));
    assert!(command.contains("tail -c \"+$((sent_bytes + 1))\" \"$log_file\""));
    assert!(command.contains("(exited with status %s)"));
}

#[test]
fn materialize_unpack_bundle_drops_staged_image_tar() {
    let root = create_temp_dir("ice-unpack-stage-test").expect("temp dir");
    let archive_path = root.join("image.tar");
    write_test_saved_image_archive(&archive_path);

    let _bundle = materialize_unpack_bundle_in(
        &IceConfig::default(),
        &archive_path.display().to_string(),
        &root,
    )
    .expect("materialize unpack bundle");

    assert!(
        !archive_path.exists(),
        "staged image.tar should be removed after extraction"
    );
    fs::remove_dir_all(&root).expect("cleanup temp dir");
}

#[test]
fn gpu_fp32_ordering_sanity() {
    assert!(gpu_quality_score("Tesla T4") < gpu_quality_score("L4"));
    assert!(gpu_quality_score("L4") < gpu_quality_score("RTX 5090"));
    assert!(gpu_quality_score("Tesla T4") < gpu_quality_score("RTX 6000Ada"));
    assert!(gpu_quality_score("RTX 4090") < gpu_quality_score("RTX 5090"));
    assert!(gpu_quality_score("A100 SXM4") < gpu_quality_score("H100 SXM"));
    assert!(gpu_quality_score("H100 SXM") < gpu_quality_score("B200"));
}

#[test]
fn prefix_lookup_accepts_prefix_without_internal_label_prefix() {
    let names = vec![
        "ice-fortuitous-dog".to_owned(),
        "ice-gentle-otter".to_owned(),
        "ice-misty-river".to_owned(),
    ];

    match prefix_lookup_indices(&names, "f", |name| name).expect("lookup should work") {
        PrefixLookup::Unique(index) => assert_eq!(index, 0),
        other => panic!("Expected unique match for `f`, got {other:?}"),
    }

    match prefix_lookup_indices(&names, "ice-gent", |name| name).expect("lookup should work") {
        PrefixLookup::Unique(index) => assert_eq!(index, 1),
        other => panic!("Expected unique match for `ice-gent`, got {other:?}"),
    }
}

#[test]
fn verb_noun_name_generation_avoids_taken_names() {
    let mut taken = HashSet::new();
    taken.insert("fortuitous-dog".to_owned());
    taken.insert("gentle-otter".to_owned());

    let generated = generate_unique_verb_noun_name(&taken).expect("should generate a fresh name");
    assert!(!taken.contains(&normalize_instance_name_for_match(&generated)));
    assert!(generated.contains('-'));
}

#[test]
fn vast_gpu_catalog_includes_a100_and_h100_family() {
    let options = load_gpu_options(Cloud::VastAi, None);
    assert!(options.iter().any(|gpu| gpu.contains("A100")));
    assert!(options.iter().any(|gpu| gpu.contains("H100")));
}

#[test]
fn gpu_selector_label_includes_vram_when_known() {
    let label = gpu_selector_label("RTX 5090");
    assert!(label.contains("RTX 5090"));
    assert!(label.contains("(32 GB)"));
}

#[test]
fn vast_cost_estimate_uses_requested_runtime() {
    let cost = estimate_runtime_cost(Cloud::VastAi, 0.5422, 0.1).expect("cost should compute");
    assert!((cost.billed_hours - 0.1).abs() < 1e-9);
    assert!((cost.total_usd - 0.05422).abs() < 1e-9);
}

#[test]
fn vast_autostop_plan_rounds_up_to_hour_boundary() {
    let start_unix = 1_700_000_000u64;
    let requested_hours = 0.1;
    let plan = build_vast_autostop_plan(start_unix, requested_hours).expect("plan");
    assert_eq!(plan.stop_at_unix % 3600, 0);
    assert!(plan.stop_at_unix >= start_unix + required_runtime_seconds(requested_hours));
    assert!(plan.runtime_hours >= requested_hours);
    assert_eq!(plan.schedule_end_unix, plan.stop_at_unix + 60);
}

#[test]
fn vast_autostop_cost_estimate_respects_plan_runtime() {
    let base = estimate_runtime_cost(Cloud::VastAi, 0.5, 0.25).expect("base estimate should work");
    let adjusted = apply_vast_autostop_cost_estimate(base).expect("adjusted estimate");
    assert!(adjusted.billed_hours >= adjusted.requested_hours);
    assert!((adjusted.total_usd - (adjusted.hourly_usd * adjusted.billed_hours)).abs() < 1e-9);
}

#[test]
fn gcp_cost_estimate_rounds_to_second_granularity() {
    let requested = 0.10001;
    let cost = estimate_runtime_cost(Cloud::Gcp, 1.0, requested).expect("cost should compute");
    let expected_billed = required_runtime_seconds(requested) as f64 / 3600.0;
    assert!((cost.billed_hours - expected_billed).abs() < 1e-9);
    assert!(cost.billed_hours >= requested);
}

#[test]
fn gcp_search_requirements_allow_missing_gpu_filter() {
    let mut config = IceConfig::default();
    config.default.gcp.min_cpus = Some(1);
    config.default.gcp.min_ram_gb = Some(1.0);
    config.default.gcp.max_price_per_hr = Some(0.05);

    let req = build_search_requirements(&config, Cloud::Gcp).expect("gcp requirements");
    assert!(req.allowed_gpus.is_empty());
}

#[test]
fn gcp_create_summary_includes_project_before_region_and_zone() {
    let mut config = IceConfig::default();
    config.auth.gcp.project = Some("test-project".to_owned());
    let candidate = CloudMachineCandidate {
        machine: "g2-standard-4".to_owned(),
        vcpus: 4,
        ram_mb: 16_384,
        gpus: vec!["L4".to_owned()],
        hourly_usd: 0.71,
        region: "us-central1".to_owned(),
        zone: Some("us-central1-a".to_owned()),
    };
    let cost = RuntimeCostEstimate {
        requested_hours: 1.0,
        billed_hours: 1.0,
        hourly_usd: 0.71,
        total_usd: 0.71,
    };
    let req = CreateSearchRequirements {
        min_cpus: 1,
        min_ram_gb: 1.0,
        allowed_gpus: Vec::new(),
        max_price_per_hr: 1.0,
    };

    let lines = crate::app::machine_candidate_summary_display_lines(
        &config,
        Cloud::Gcp,
        &candidate,
        &cost,
        &req,
        None,
    )
    .expect("summary lines");
    let project_index = lines
        .iter()
        .position(|line| line == "  Project: test-project")
        .expect("project line");
    let region_index = lines
        .iter()
        .position(|line| line == "  Region: us-central1")
        .expect("region line");
    let zone_index = lines
        .iter()
        .position(|line| line == "  Zone: us-central1-a")
        .expect("zone line");

    assert!(project_index < region_index);
    assert!(project_index < zone_index);
}

#[test]
fn aws_search_requirements_allow_missing_gpu_filter() {
    let mut config = IceConfig::default();
    config.default.aws.min_cpus = Some(1);
    config.default.aws.min_ram_gb = Some(1.0);
    config.default.aws.max_price_per_hr = Some(0.05);

    let req = build_search_requirements(&config, Cloud::Aws).expect("aws requirements");
    assert!(req.allowed_gpus.is_empty());
}

#[test]
fn gcp_catalog_selection_prefers_cpu_only_when_gpu_filter_is_empty() {
    let req = CreateSearchRequirements {
        min_cpus: 1,
        min_ram_gb: 1.0,
        allowed_gpus: Vec::new(),
        max_price_per_hr: 1.0,
    };
    let catalog = vec![
        GcpMachineCatalogEntry {
            machine: "g2-standard-4".to_owned(),
            zone: "us-central1-a".to_owned(),
            region: "us-central1".to_owned(),
            vcpus: 4,
            billable_vcpus: 4.0,
            ram_mb: 16_384,
            accelerators: Vec::new(),
            bundled_local_ssd_partitions: 0,
            gpus: vec!["L4".to_owned()],
            hourly_usd: 0.71,
        },
        GcpMachineCatalogEntry {
            machine: "e2-micro".to_owned(),
            zone: "us-central1-a".to_owned(),
            region: "us-central1".to_owned(),
            vcpus: 2,
            billable_vcpus: 2.0,
            ram_mb: 1_024,
            accelerators: Vec::new(),
            bundled_local_ssd_partitions: 0,
            gpus: Vec::new(),
            hourly_usd: 0.0076,
        },
    ];

    let candidate = crate::providers::gcp::select_cheapest_machine_candidate(
        &catalog,
        &req,
        None,
        Some("us-central1"),
        Some("us-central1-a"),
    )
    .expect("candidate");
    assert_eq!(candidate.machine, "e2-micro");
}

#[test]
fn gcp_catalog_selection_honors_gpu_filter() {
    let req = CreateSearchRequirements {
        min_cpus: 1,
        min_ram_gb: 1.0,
        allowed_gpus: vec!["L4".to_owned()],
        max_price_per_hr: 1.0,
    };
    let catalog = vec![
        GcpMachineCatalogEntry {
            machine: "g2-standard-4".to_owned(),
            zone: "us-central1-a".to_owned(),
            region: "us-central1".to_owned(),
            vcpus: 4,
            billable_vcpus: 4.0,
            ram_mb: 16_384,
            accelerators: Vec::new(),
            bundled_local_ssd_partitions: 0,
            gpus: vec!["Nvidia L4".to_owned()],
            hourly_usd: 0.71,
        },
        GcpMachineCatalogEntry {
            machine: "e2-micro".to_owned(),
            zone: "us-central1-a".to_owned(),
            region: "us-central1".to_owned(),
            vcpus: 2,
            billable_vcpus: 2.0,
            ram_mb: 1_024,
            accelerators: Vec::new(),
            bundled_local_ssd_partitions: 0,
            gpus: Vec::new(),
            hourly_usd: 0.0076,
        },
    ];

    let candidate = crate::providers::gcp::select_cheapest_machine_candidate(
        &catalog,
        &req,
        None,
        Some("us-central1"),
        Some("us-central1-a"),
    )
    .expect("candidate");
    assert_eq!(candidate.machine, "g2-standard-4");
}

#[test]
fn gcp_catalog_selection_treats_614mb_as_point_six_gb() {
    let req = CreateSearchRequirements {
        min_cpus: 1,
        min_ram_gb: 0.6,
        allowed_gpus: Vec::new(),
        max_price_per_hr: 1.0,
    };
    let catalog = vec![
        GcpMachineCatalogEntry {
            machine: "f1-micro".to_owned(),
            zone: "us-central1-a".to_owned(),
            region: "us-central1".to_owned(),
            vcpus: 1,
            billable_vcpus: 1.0,
            ram_mb: 614,
            accelerators: Vec::new(),
            bundled_local_ssd_partitions: 0,
            gpus: Vec::new(),
            hourly_usd: 0.0076,
        },
        GcpMachineCatalogEntry {
            machine: "e2-micro".to_owned(),
            zone: "us-central1-a".to_owned(),
            region: "us-central1".to_owned(),
            vcpus: 2,
            billable_vcpus: 2.0,
            ram_mb: 1_024,
            accelerators: Vec::new(),
            bundled_local_ssd_partitions: 0,
            gpus: Vec::new(),
            hourly_usd: 0.0084,
        },
    ];

    let candidate = crate::providers::gcp::select_cheapest_machine_candidate(
        &catalog,
        &req,
        None,
        Some("us-central1"),
        Some("us-central1-a"),
    )
    .expect("candidate");
    assert_eq!(candidate.machine, "f1-micro");
}

#[test]
fn vast_ssh_args_place_identity_options_before_destination() {
    let args = ssh_args("ssh1.vast.ai", 10135, Some(Path::new("/tmp/id_ed25519")));
    let host_index = args
        .iter()
        .position(|value| value == "root@ssh1.vast.ai")
        .expect("host arg");
    let identity_index = args
        .iter()
        .position(|value| value == "-i")
        .expect("identity flag");
    assert!(identity_index < host_index);
    assert!(args.iter().any(|value| value == "IdentitiesOnly=yes"));
}

#[test]
fn vast_ssh_args_without_identity_keep_destination_last() {
    let args = ssh_args("ssh1.vast.ai", 10135, None);
    assert_eq!(
        args.last().expect("destination argument"),
        "root@ssh1.vast.ai"
    );
    assert!(!args.iter().any(|value| value == "-i"));
    assert!(!args.iter().any(|value| value == "IdentitiesOnly=yes"));
}

#[test]
fn vast_job_termination_unix_recognizes_stop_and_delete_actions() {
    let stop_job = VastScheduledJob {
        instance_id: Some(42),
        api_endpoint: Some("/api/v0/instances/42/".to_owned()),
        request_method: Some("PUT".to_owned()),
        request_body: Some(json!({"state":"stopped"})),
        start_time: Some(1_700_000_000.0),
    };
    let delete_job = VastScheduledJob {
        instance_id: Some(42),
        api_endpoint: Some("/api/v0/instances/42/".to_owned()),
        request_method: Some("DELETE".to_owned()),
        request_body: None,
        start_time: Some(1_700_000_100.0),
    };
    let irrelevant_job = VastScheduledJob {
        instance_id: Some(42),
        api_endpoint: Some("/api/v0/instances/42/".to_owned()),
        request_method: Some("PUT".to_owned()),
        request_body: Some(json!({"state":"running"})),
        start_time: Some(1_700_000_200.0),
    };

    assert_eq!(job_termination_unix(&stop_job), Some(1_700_000_000.0));
    assert_eq!(job_termination_unix(&delete_job), Some(1_700_000_100.0));
    assert_eq!(job_termination_unix(&irrelevant_job), None);
}

#[test]
fn nearest_vast_scheduled_termination_picks_earliest_future_job() {
    let now = now_unix_secs_f64();
    let jobs = vec![
        VastScheduledJob {
            instance_id: Some(42),
            api_endpoint: Some("/api/v0/instances/42/".to_owned()),
            request_method: Some("PUT".to_owned()),
            request_body: Some(json!({"state":"stopped"})),
            start_time: Some(now + 7_200.0),
        },
        VastScheduledJob {
            instance_id: Some(42),
            api_endpoint: Some("/api/v0/instances/42/".to_owned()),
            request_method: Some("DELETE".to_owned()),
            request_body: None,
            start_time: Some(now + 3_600.0),
        },
        VastScheduledJob {
            instance_id: Some(42),
            api_endpoint: Some("/api/v0/instances/42/".to_owned()),
            request_method: Some("DELETE".to_owned()),
            request_body: None,
            start_time: Some(now - 60.0),
        },
    ];

    let nearest = nearest_scheduled_termination_by_instance(&jobs);
    let value = nearest.get(&42).copied().expect("nearest time");
    assert!(value >= now + 3_599.0);
    assert!(value <= now + 3_601.0);
}

#[test]
fn remaining_contract_hours_at_prefers_scheduled_termination_when_sooner() {
    let now = 1_700_000_000.0;
    let instance = test_vast_instance(Some(now + 10.0 * 3600.0));
    let remaining = remaining_contract_hours_at(&instance, Some(now + 2.0 * 3600.0), now);
    assert!((remaining - 2.0).abs() < 1e-9);
}

#[test]
fn remaining_contract_hours_at_uses_scheduled_when_no_contract_end() {
    let now = 1_700_000_000.0;
    let instance = test_vast_instance(None);
    let remaining = remaining_contract_hours_at(&instance, Some(now + 1.5 * 3600.0), now);
    assert!((remaining - 1.5).abs() < 1e-9);
}

#[test]
fn container_image_reference_builds_expected_ref() {
    let container = ContainerImageReference::new(
        "us-docker.pkg.dev/test-project/test-repo".to_owned(),
        "worker:latest".to_owned(),
    )
    .expect("container ref");
    assert_eq!(
        container.container_ref(),
        "us-docker.pkg.dev/test-project/test-repo/worker:latest"
    );
    assert_eq!(container.registry_host(), "us-docker.pkg.dev");
}

#[test]
fn container_image_reference_parses_full_ref() {
    let container =
        ContainerImageReference::from_container_ref("gcr.io/example-project/trainer:dev")
            .expect("container ref");
    assert_eq!(container.registry, "gcr.io/example-project");
    assert_eq!(container.container, "trainer:dev");
}

#[test]
fn parse_workload_metadata_round_trips_container_values() {
    let workload = InstanceWorkload::Container(
        ContainerImageReference::new(
            "gcr.io/example-project".to_owned(),
            "trainer:dev".to_owned(),
        )
        .expect("workload"),
    );
    let metadata = workload_metadata_values(&workload);
    let kind = metadata.iter().find_map(|(key, value)| {
        (*key == ICE_WORKLOAD_KIND_METADATA_KEY).then_some(value.as_str())
    });
    let registry = metadata.iter().find_map(|(key, value)| {
        (*key == ICE_WORKLOAD_REGISTRY_METADATA_KEY).then_some(value.as_str())
    });
    let container = metadata.iter().find_map(|(key, value)| {
        (*key == ICE_WORKLOAD_CONTAINER_METADATA_KEY).then_some(value.as_str())
    });
    let source = metadata.iter().find_map(|(key, value)| {
        (*key == ICE_WORKLOAD_SOURCE_METADATA_KEY).then_some(value.as_str())
    });

    let parsed =
        parse_workload_metadata(kind, registry, container, source).expect("parsed workload");
    assert_eq!(parsed, workload);
}

#[test]
fn parse_workload_metadata_round_trips_unpack_values() {
    let workload = InstanceWorkload::Unpack("/tmp/nonexistent-image.tar".to_owned());
    let metadata = workload_metadata_values(&workload);
    let kind = metadata.iter().find_map(|(key, value)| {
        (*key == ICE_WORKLOAD_KIND_METADATA_KEY).then_some(value.as_str())
    });
    let source = metadata.iter().find_map(|(key, value)| {
        (*key == ICE_WORKLOAD_SOURCE_METADATA_KEY).then_some(value.as_str())
    });

    let parsed = parse_workload_metadata(kind, None, None, source).expect("parsed unpack workload");
    assert_eq!(parsed, workload);
}

#[test]
fn resolve_deploy_workload_defaults_to_local_arca_selector() {
    let workload = resolve_deploy_workload(&DeployTargetRequest {
        positional: Some("test-crate".to_owned()),
        ..DeployTargetRequest::default()
    })
    .expect("workload");

    assert_eq!(
        workload,
        InstanceWorkload::Unpack("arca:test-crate".to_owned())
    );
}

#[test]
fn resolve_deploy_workload_accepts_full_container_ref() {
    let workload = resolve_deploy_workload(&DeployTargetRequest {
        container: Some("gcr.io/example-project/trainer:dev".to_owned()),
        ..DeployTargetRequest::default()
    })
    .expect("workload");

    assert_eq!(
        workload,
        InstanceWorkload::Container(
            ContainerImageReference::new(
                "gcr.io/example-project".to_owned(),
                "trainer:dev".to_owned(),
            )
            .expect("container ref"),
        )
    );
}

#[test]
fn resolve_deploy_workload_rejects_container_mode_for_arca_sources() {
    let err = resolve_deploy_workload(&DeployTargetRequest {
        container: Some("arca:test-crate".to_owned()),
        ..DeployTargetRequest::default()
    })
    .expect_err("arca container mode should be rejected");

    assert!(format!("{err:#}").contains("`--container arca:...` is invalid"));
}

#[test]
fn resolve_deploy_workload_rejects_conflicting_modes() {
    let err = resolve_deploy_workload(&DeployTargetRequest {
        ssh: true,
        unpack: Some("arca:test-crate".to_owned()),
        ..DeployTargetRequest::default()
    })
    .expect_err("conflicting modes should be rejected");

    assert!(format!("{err:#}").contains("Pass exactly one of"));
}

#[test]
fn saved_image_bundle_extracts_entrypoint_layers_and_whiteouts() {
    let temp_dir = create_temp_dir("ice-test-unpack").expect("temp dir");
    let archive_path = temp_dir.join("image.tar");
    write_test_saved_image_archive(&archive_path);

    let bundle = load_saved_image_bundle(&archive_path).expect("bundle");
    assert_eq!(bundle.command, vec!["/usr/local/bin/app", "--flag"]);
    assert_eq!(bundle.working_dir.as_deref(), Some("/workspace"));
    assert_eq!(bundle.env, vec!["EXAMPLE=1"]);
    assert_eq!(bundle.layers.len(), 2);

    let rootfs = temp_dir.join("rootfs");
    fs::create_dir_all(&rootfs).expect("rootfs dir");
    extract_saved_image_layers(&archive_path, &bundle.layers, &rootfs).expect("extract layers");
    assert!(rootfs.join("usr/local/bin/app").is_file());
    assert!(rootfs.join("workspace/new.txt").is_file());
    assert!(!rootfs.join("workspace/obsolete.txt").exists());

    fs::remove_dir_all(temp_dir).expect("cleanup");
}

#[test]
fn infer_vast_workload_treats_default_image_as_shell() {
    let mut instance = test_vast_instance(None);
    instance.image = Some(VAST_DEFAULT_IMAGE.to_owned());
    assert_eq!(infer_workload(&instance), Some(InstanceWorkload::Shell));
}

#[test]
fn infer_vast_workload_uses_image_uuid_when_image_is_missing() {
    let mut instance = test_vast_instance(None);
    instance.image_uuid =
        Some("us-central1-docker.pkg.dev/test-project/test-repo/worker:dev".to_owned());
    assert_eq!(
        infer_workload(&instance),
        Some(InstanceWorkload::Container(
            ContainerImageReference::new(
                "us-central1-docker.pkg.dev/test-project/test-repo".to_owned(),
                "worker:dev".to_owned(),
            )
            .expect("container ref"),
        ))
    );
}

#[test]
fn vast_container_workloads_use_args_runtype() {
    let workload = InstanceWorkload::Container(
        ContainerImageReference::new(
            "us-central1-docker.pkg.dev/test-project/test-repo".to_owned(),
            "worker:dev".to_owned(),
        )
        .expect("container ref"),
    );
    assert_eq!(runtype_for_workload(&workload), "args");
}

#[test]
fn vast_ssh_support_check_accepts_ssh_direct() {
    let mut instance = test_vast_instance(None);
    instance.image_runtype = Some("ssh_direct".to_owned());
    assert!(instance_supports_ssh(&instance));
}

#[test]
fn vast_shell_workloads_use_ssh_direct_runtype() {
    assert_eq!(runtype_for_workload(&InstanceWorkload::Shell), "ssh_direct");
}

#[test]
fn vast_health_hint_reports_loading() {
    let mut instance = test_vast_instance(None);
    instance.intended_status = Some("running".to_owned());
    instance.actual_status = Some("loading".to_owned());
    assert_eq!(instance.health_hint(), "loading");
}

#[test]
fn parse_cloud_accepts_local() {
    assert_eq!(parse_cloud("local").expect("cloud"), Cloud::Local);
}

#[test]
fn parse_local_instance_row_reads_workload_and_runtime() {
    let row = json!({
        "Id": "abc123def456",
        "Name": "/ice-local-test",
        "Created": "2026-03-06T00:00:00Z",
        "Config": {
            "Image": "gcr.io/example-project/trainer:dev",
            "Labels": {
                "ice-managed": "true",
                "ice-cloud": "local",
                "ice-workload-kind": "container",
                "ice-workload-registry": "gcr.io/example-project",
                "ice-workload-container": "trainer:dev",
                "ice-runtime-seconds": "1800"
            }
        },
        "State": {
            "Status": "running",
            "StartedAt": "2026-03-06T00:05:00Z",
            "Health": {
                "Status": "healthy"
            }
        }
    });

    let labels = local_instance_labels(&row);
    assert!(local_instance_is_managed(&labels));

    let instance = parse_local_instance_row(&row, &labels).expect("local instance");
    assert_eq!(instance.id, "abc123def456");
    assert_eq!(instance.name, "ice-local-test");
    assert_eq!(instance.runtime_seconds, Some(1800));
    assert_eq!(
        instance.workload,
        Some(InstanceWorkload::Container(
            ContainerImageReference::new(
                "gcr.io/example-project".to_owned(),
                "trainer:dev".to_owned()
            )
            .expect("workload")
        ))
    );
    assert_eq!(
        local_workload_display(&instance),
        "gcr.io/example-project/trainer:dev"
    );
}

#[test]
fn deploy_hours_use_cli_override_then_config_then_builtin_default() {
    let mut config = IceConfig::default();
    assert_eq!(
        resolve_deploy_hours(&config, None).expect("builtin default"),
        1.0
    );

    set_config_value(&mut config, "default.runtime_hours", "2.5").expect("set runtime hours");
    assert_eq!(
        resolve_deploy_hours(&config, None).expect("config default"),
        2.5
    );
    assert_eq!(
        resolve_deploy_hours(&config, Some(0.25)).expect("cli override"),
        0.25
    );
}

#[test]
fn workload_config_keys_are_rejected() {
    let err = normalize_config_key("default.workload.kind").expect_err("legacy key should fail");
    assert!(format!("{err:#}").contains("Unknown config key"));
}

#[test]
fn unset_config_value_clears_values() {
    let mut config = IceConfig::default();
    set_config_value(&mut config, "default.cloud", "aws").expect("set cloud");
    set_config_value(&mut config, "default.aws.region", "us-west-2").expect("set region");
    set_config_value(&mut config, "auth.aws.access_key_id", "AKIA_TEST").expect("set key");

    unset_config_value(&mut config, "default.cloud").expect("unset cloud");
    unset_config_value(&mut config, "default.aws.region").expect("unset region");
    unset_config_value(&mut config, "auth.aws.access_key_id").expect("unset key");

    assert_eq!(
        get_config_value(&config, "default.cloud").expect("cloud"),
        "<unset>"
    );
    assert_eq!(
        get_config_value(&config, "default.aws.region").expect("region"),
        "<unset>"
    );
    assert_eq!(
        get_config_value(&config, "auth.aws.access_key_id").expect("access key"),
        "<unset>"
    );
}

#[test]
fn config_accepts_fractional_market_ram_filters() {
    let mut config = IceConfig::default();
    assert_eq!(
        set_config_value(&mut config, "default.aws.min_ram_gb", "0.5").expect("aws min ram"),
        "0.5"
    );
    assert_eq!(
        set_config_value(&mut config, "default.gcp.min_ram_gb", "0.6").expect("gcp min ram"),
        "0.6"
    );
    assert_eq!(
        get_config_value(&config, "default.aws.min_ram_gb").expect("aws min ram value"),
        "0.5"
    );
    assert_eq!(
        get_config_value(&config, "default.gcp.min_ram_gb").expect("gcp min ram value"),
        "0.6"
    );
}

#[test]
fn shell_cli_parses_print_creds_flag() {
    let cli = Cli::parse_from([
        "ice",
        "shell",
        "--cloud",
        "aws",
        "--print-creds",
        "ice-test",
    ]);
    let Commands::Shell(args) = cli.command else {
        panic!("expected shell command");
    };
    assert_eq!(args.cloud, Some(Cloud::Aws));
    assert!(args.print_creds);
    assert!(!args.preserve_ephemeral);
    assert_eq!(args.instance, "ice-test");
}

#[test]
fn shell_cli_parses_preserve_ephemeral_flag() {
    let cli = Cli::parse_from([
        "ice",
        "shell",
        "--cloud",
        "vast.ai",
        "--preserve-ephemeral",
        "ice-test",
    ]);
    let Commands::Shell(args) = cli.command else {
        panic!("expected shell command");
    };
    assert_eq!(args.cloud, Some(Cloud::VastAi));
    assert!(!args.print_creds);
    assert!(args.preserve_ephemeral);
    assert_eq!(args.instance, "ice-test");
}

#[test]
fn aws_shell_connect_command_includes_identity_and_destination() {
    let mut config = IceConfig::default();
    config.default.aws.ssh_key_path = Some("/tmp/id_ed25519".to_owned());
    config.default.aws.ssh_user = Some("ubuntu".to_owned());
    let instance = AwsInstance {
        instance_id: "i-1234567890".to_owned(),
        name: Some("ice-test".to_owned()),
        region: "us-west-2".to_owned(),
        state: "running".to_owned(),
        instance_type: "g5.xlarge".to_owned(),
        launch_time: None,
        public_ip: Some("203.0.113.10".to_owned()),
        public_dns: None,
        workload: Some(InstanceWorkload::Shell),
    };

    let command = <crate::providers::aws::Provider as RemoteSshProvider>::shell_connect_command(
        &config, &instance,
    )
    .expect("aws shell command");

    assert!(command.contains("ssh"));
    assert!(command.contains("/tmp/id_ed25519"));
    assert!(command.contains("StrictHostKeyChecking=accept-new"));
    assert!(command.contains("-t"));
    assert!(command.contains("ubuntu@203.0.113.10"));
}

#[test]
fn gcp_shell_connect_command_includes_project_and_zone() {
    let mut config = IceConfig::default();
    config.auth.gcp.project = Some("demo-project".to_owned());
    let instance = GcpInstance {
        name: "ice-test".to_owned(),
        zone: "us-central1-a".to_owned(),
        status: "RUNNING".to_owned(),
        machine_type: "g2-standard-4".to_owned(),
        creation_timestamp: None,
        last_start_timestamp: None,
        workload: Some(InstanceWorkload::Shell),
    };

    let command = <crate::providers::gcp::Provider as RemoteSshProvider>::shell_connect_command(
        &config, &instance,
    )
    .expect("gcp shell command");

    assert!(command.contains("gcloud"));
    assert!(command.contains("compute"));
    assert!(command.contains("ssh"));
    assert!(command.contains("ice-test"));
    assert!(command.contains("us-central1-a"));
    assert!(command.contains("demo-project"));
    assert!(command.contains("--ssh-flag=-t"));
}

#[test]
fn create_cli_parses_aws_dry_run_flags_without_custom_mode() {
    let cli = Cli::parse_from([
        "ice",
        "create",
        "--cloud",
        "aws",
        "--ssh",
        "--dry-run",
        "--hours",
        "1",
    ]);
    let Commands::Create(args) = cli.command else {
        panic!("expected create command");
    };
    assert_eq!(args.cloud, Some(Cloud::Aws));
    assert!(args.ssh);
    assert!(args.dry_run);
    assert!(!args.custom);
}

#[test]
fn create_cli_parses_search_override_flags() {
    let cli = Cli::parse_from([
        "ice",
        "create",
        "--cloud",
        "gcp",
        "--ssh",
        "--dry-run",
        "--min-cpus",
        "1",
        "--min-ram-gb",
        "0.6",
        "--no-gpu",
        "--max-price-per-hr",
        "1",
    ]);
    let Commands::Create(args) = cli.command else {
        panic!("expected create command");
    };
    assert_eq!(args.cloud, Some(Cloud::Gcp));
    assert_eq!(args.min_cpus, Some(1));
    assert_eq!(args.min_ram_gb, Some(0.6));
    assert!(args.no_gpu);
    assert_eq!(args.max_price_per_hr, Some(1.0));
}

#[test]
fn create_cli_parses_repeated_gpu_override_flags() {
    let cli = Cli::parse_from([
        "ice",
        "create",
        "--cloud",
        "aws",
        "--ssh",
        "--dry-run",
        "--gpu",
        "L4",
        "--gpu",
        "H100 SXM",
    ]);
    let Commands::Create(args) = cli.command else {
        panic!("expected create command");
    };
    assert_eq!(args.gpus, vec!["L4".to_owned(), "H100 SXM".to_owned()]);
    assert!(!args.no_gpu);
}

#[test]
fn refresh_catalog_cli_defaults_to_all_supported_clouds() {
    let cli = Cli::parse_from(["ice", "refresh-catalog"]);
    let Commands::RefreshCatalog(args) = cli.command else {
        panic!("expected refresh-catalog command");
    };
    assert_eq!(args.cloud, None);
}

#[test]
fn refresh_catalog_cli_accepts_explicit_cloud_override() {
    let cli = Cli::parse_from(["ice", "refresh-catalog", "--cloud", "aws"]);
    let Commands::RefreshCatalog(args) = cli.command else {
        panic!("expected refresh-catalog command");
    };
    assert_eq!(args.cloud, Some(Cloud::Aws));
}

#[test]
fn render_listed_instance_uses_two_line_system_layout() {
    let instance = listed_instance(
        "falcon".to_owned(),
        "running".to_owned(),
        Color::Green,
        vec![
            "ok".to_owned(),
            "1.25h".to_owned(),
            "$0.7100/hr".to_owned(),
            "g2-standard-4".to_owned(),
            "us-central1-a".to_owned(),
        ],
        vec![
            "gcp://us-central1-a/ice-falcon".to_owned(),
            "shell".to_owned(),
        ],
    );

    assert_eq!(
        render_listed_instance(&instance, &StdoutRenderTarget),
        "● falcon · running · ok · 1.25h · $0.7100/hr · g2-standard-4 · us-central1-a\n    gcp://us-central1-a/ice-falcon · shell"
    );
}

#[test]
fn render_listed_instance_strikes_missing_cached_rows() {
    let instance = missing_remote_cached_instance(
        &listed_instance(
            "falcon".to_owned(),
            "running".to_owned(),
            Color::Green,
            vec!["ok".to_owned(), "1.25h".to_owned()],
            vec!["gcp://us-central1-a/ice-falcon".to_owned()],
        ),
        0,
    );

    assert_eq!(
        render_listed_instance(&instance, &StdoutRenderTarget),
        "● falcon · running · ok · 1.25h · missing remotely (cached previously)\n    gcp://us-central1-a/ice-falcon"
    );
}
