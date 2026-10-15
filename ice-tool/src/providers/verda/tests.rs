use super::*;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

const ID: &str = "11111111-1111-4111-8111-111111111111";
const OS: &str = "22222222-2222-4222-8222-222222222222";
const EXTRA: &str = "33333333-3333-4333-8333-333333333333";
const KEY: &str = "44444444-4444-4444-8444-444444444444";

#[derive(Clone, Debug)]
struct Request {
    method: String,
    path: String,
    body: Value,
    authorization: String,
}
struct Reply {
    status: u16,
    body: Value,
    headers: String,
    raw_body: Option<String>,
}
impl Reply {
    fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            body,
            headers: String::new(),
            raw_body: None,
        }
    }
    fn raw(status: u16, body: &str) -> Self {
        Self {
            raw_body: Some(body.to_owned()),
            ..Self::json(status, Value::Null)
        }
    }
}
struct Server {
    endpoint: String,
    client: Client,
    requests: Arc<Mutex<Vec<Request>>>,
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}
impl Server {
    fn new(handler: impl Fn(&Request) -> Reply + Send + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let client = Client::mock(&endpoint).unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let captured = requests.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let stopping = stop.clone();
        let handle = std::thread::spawn(move || {
            while !stopping.load(Ordering::SeqCst) {
                let Ok((mut stream, _)) = listener.accept() else {
                    std::thread::sleep(Duration::from_millis(2));
                    continue;
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut bytes = Vec::new();
                let header_end = loop {
                    let mut chunk = [0; 4096];
                    let n = stream.read(&mut chunk).unwrap();
                    assert_ne!(n, 0);
                    bytes.extend_from_slice(&chunk[..n]);
                    if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let header = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
                let length = header
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|n| n.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                while bytes.len() < header_end + length {
                    let mut chunk = [0; 4096];
                    let n = stream.read(&mut chunk).unwrap();
                    assert_ne!(n, 0);
                    bytes.extend_from_slice(&chunk[..n]);
                }
                let mut first = header.lines().next().unwrap().split_whitespace();
                let request = Request {
                    method: first.next().unwrap().into(),
                    path: first.next().unwrap().into(),
                    body: if length == 0 {
                        Value::Null
                    } else {
                        serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap()
                    },
                    authorization: header
                        .lines()
                        .find_map(|line| {
                            line.split_once(':')
                                .filter(|(name, _)| name.eq_ignore_ascii_case("authorization"))
                                .map(|(_, v)| v.trim().to_owned())
                        })
                        .unwrap_or_default(),
                };
                captured.lock().unwrap().push(request.clone());
                let response = handler(&request);
                let body = if response.status == 204 {
                    String::new()
                } else {
                    response
                        .raw_body
                        .unwrap_or_else(|| response.body.to_string())
                };
                let _ = write!(
                    stream,
                    "HTTP/1.1 {} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n{}",
                    response.status,
                    body.len(),
                    response.headers,
                    body
                );
            }
        });
        Self {
            endpoint,
            client,
            requests,
            stop,
            handle: Some(handle),
        }
    }
    fn requests(&self) -> Vec<Request> {
        self.requests.lock().unwrap().clone()
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        self.handle.take().unwrap().join().unwrap();
    }
}
fn token() -> Reply {
    Reply::json(200, json!({"access_token":"mock-token","expires_in":3600}))
}

#[test]
fn login_validates_oauth_without_reading_or_creating_resources() {
    for status in [200, 401] {
        let server = Server::new(move |request| {
            assert_eq!(request.method, "POST");
            assert_eq!(request.path, "/oauth2/token");
            assert_eq!(
                request.body,
                json!({"grant_type":"client_credentials", "client_id":"test-client", "client_secret":"test-secret"})
            );
            if status == 200 {
                token()
            } else {
                Reply::json(status, json!({"error":"test-secret"}))
            }
        });
        let result = server.client.authenticate();
        assert_eq!(result.is_ok(), status == 200);
        if let Err(error) = result {
            assert!(!format!("{error:#}").contains("test-secret"));
        }
        assert_eq!(server.requests().len(), 1);
    }
}
fn instance(status: &str) -> Value {
    json!({"id":ID,"hostname":"ice-verda-test","status":status,"ip":"192.0.2.1",
        "os_volume_id":OS,"volume_ids":[OS,EXTRA],"price_per_hour":0.6,
        "jupyter_token":"must-not-appear-in-output"})
}
fn catalog() -> Catalog {
    serde_json::from_value(json!({
        "machines":[{"instance_type":"1RTXA6000.10V","model":"RTX A6000","name":"RTX A6000 48GB","manufacturer":"NVIDIA","currency":"usd","price_per_hour":"0.6",
            "cpu":{"number_of_cores":10},"gpu":{"number_of_gpus":1},"gpu_memory":{"size_in_gigabytes":48},"memory":{"size_in_gigabytes":64},
            "supported_os":["ubuntu-24.04-cuda-12.9-open","ubuntu-26.04-cuda-13.2-open"]}],
        "images":[{"id":OS,"image_type":"ubuntu-24.04-cuda-12.9-open","name":"Ubuntu CUDA","category":"cuda","is_cluster":false},
            {"id":EXTRA,"image_type":"ubuntu-26.04-cuda-13.2-open","name":"Ubuntu CUDA","category":"cuda","is_cluster":false}],
        "availability":[{"location_code":"FIN-01","availabilities":["1RTXA6000.10V"]}],
        "volume_types":[{"type":"NVMe","price":{"currency":"usd","price_per_month_per_gb":0.2}}]
    })).unwrap()
}
fn requirements() -> crate::model::CreateSearchRequirements {
    crate::model::CreateSearchRequirements {
        max_price_per_hr: 0.7,
        ..Default::default()
    }
}
fn image_defaults() -> crate::model::VerdaDefaults {
    crate::model::VerdaDefaults {
        image: Some(EXTRA.into()),
        ..Default::default()
    }
}
fn offer() -> Offer {
    catalog()
        .select(&requirements(), &image_defaults(), None)
        .unwrap()
}

fn ssh_key_record() -> Value {
    // Field names/types match the sanitized 2026-10-01 live detail response.
    // All values are synthetic; no account key or fingerprint is retained.
    json!({"id":KEY, "name":"fixture", "key":"ssh-ed25519 AAAAFixture key-comment",
        "fingerprint":"fixture-fingerprint", "created_by_user_id":"fixture-user"})
}

#[test]
fn registered_key_accepts_object_and_observed_singleton_array() {
    for response in [ssh_key_record(), json!([ssh_key_record()])] {
        let server = Server::new(move |request| {
            if request.path == "/oauth2/token" {
                return token();
            }
            assert_eq!(request.method, "GET");
            assert_eq!(request.path, format!("/ssh-keys/{KEY}"));
            Reply::json(200, response.clone())
        });
        let key = keys::registered_key(&server.client, KEY).unwrap();
        assert_eq!(key.id, KEY);
        assert_eq!(key.key.as_deref(), ssh_key_record()["key"].as_str());
        assert_eq!(server.requests().len(), 2);
    }
}

fn invalid_key_responses() -> Vec<(Value, &'static str)> {
    vec![
        (json!([]), "unexpected_key_count"),
        (
            json!([ssh_key_record(), ssh_key_record()]),
            "unexpected_key_count",
        ),
        (
            json!({"id":ID,"key":"ssh-ed25519 AAAAFixture"}),
            "id_mismatch",
        ),
        (
            json!([{ "id":ID,"key":"ssh-ed25519 AAAAFixture"}]),
            "id_mismatch",
        ),
        (json!([null]), "invalid_schema"),
        (json!([[KEY, "ssh-ed25519 AAAAFixture"]]), "invalid_schema"),
        (
            json!({"id":KEY,"key":{"private":"response-secret"}}),
            "invalid_schema",
        ),
        (json!({"id":77,"key":"response-secret"}), "invalid_schema"),
        (json!({"key":"response-secret"}), "invalid_schema"),
        (json!("response-secret"), "invalid_schema"),
        (Value::Null, "invalid_schema"),
        (json!({"id":KEY}), "unusable_public_key"),
        (json!({"id":KEY,"key":null}), "unusable_public_key"),
        (json!({"id":KEY,"key":""}), "unusable_public_key"),
        (json!({"id":KEY,"key":"ssh-ed25519"}), "unusable_public_key"),
        (
            json!({"id":KEY,"key":"response-secret"}),
            "unusable_public_key",
        ),
    ]
}

#[test]
fn registered_key_rejects_ambiguous_malformed_and_wrong_identity_responses() {
    for (response, reason) in invalid_key_responses() {
        let server = Server::new(move |request| {
            if request.path == "/oauth2/token" {
                return token();
            }
            assert_eq!(request.method, "GET");
            assert_eq!(request.path, format!("/ssh-keys/{KEY}"));
            Reply::json(200, response.clone())
        });
        let error = keys::registered_key(&server.client, KEY).err().unwrap();
        let structured = error
            .downcast_ref::<crate::automation::AgentError>()
            .unwrap();
        assert_eq!(structured.code, "verda_ssh_key_response_invalid");
        assert_eq!(structured.details["reason"], reason);
        assert_eq!(structured.details["endpoint"], format!("/ssh-keys/{KEY}"));
        assert_eq!(structured.details["stage"], "ssh_key_lookup");
        let diagnostic = format!("{error:#} {}", structured.details);
        for secret in [
            "response-secret",
            "AAAAFixture",
            "fixture-fingerprint",
            "fixture-user",
        ] {
            assert!(!diagnostic.contains(secret));
        }
        assert_eq!(server.requests().len(), 2);
    }
}

fn creation_fixture() -> (IceConfig, CreateArgs) {
    use clap::Parser;
    let cli = crate::cli::Cli::try_parse_from([
        "ice",
        "create",
        "--cloud",
        "verda",
        "--ssh",
        "--yes",
        "--manual-cleanup",
    ])
    .unwrap();
    let crate::cli::Commands::Create(args) = cli.command else {
        unreachable!()
    };
    let mut config = IceConfig::default();
    config.default.verda.image = Some(EXTRA.into());
    config.default.verda.ssh_key_id = Some(KEY.into());
    config.default.verda.max_price_per_hr = Some(0.7);
    (config, *args)
}

fn creation_catalog_reply(request: &Request) -> Reply {
    if request.path == "/oauth2/token" {
        return token();
    }
    let data = serde_json::to_value(catalog()).unwrap();
    let field = if request.path.starts_with("/instance-types?") {
        "machines"
    } else if request.path.starts_with("/instance-availability?") {
        "availability"
    } else if request.path.starts_with("/volume-types?") {
        "volume_types"
    } else if request.path.starts_with("/images?") {
        "images"
    } else {
        return Reply::json(404, json!({}));
    };
    Reply::json(200, data[field].clone())
}

#[test]
fn create_key_preflight_reports_stage_and_never_posts_an_instance_on_failure() {
    let mut replies: Vec<Reply> = invalid_key_responses()
        .into_iter()
        .map(|(value, _)| Reply::json(200, value))
        .collect();
    replies.push(Reply::raw(200, "invalid-json-response-secret"));
    replies.push(Reply::json(403, json!({"message":"response-secret"})));
    for reply in replies {
        let server = Server::new(move |request| {
            if request.path == format!("/ssh-keys/{KEY}") {
                return Reply {
                    status: reply.status,
                    body: reply.body.clone(),
                    raw_body: reply.raw_body.clone(),
                    headers: String::new(),
                };
            }
            creation_catalog_reply(request)
        });
        let (mut config, args) = creation_fixture();
        let error =
            Provider::create_with_client(&mut config, &args, |_| Client::mock(&server.endpoint))
                .unwrap_err();
        let structured = error
            .downcast_ref::<crate::automation::AgentError>()
            .unwrap();
        assert_eq!(structured.details["stage"], "ssh_key_preflight");
        assert_eq!(structured.details["endpoint"], format!("/ssh-keys/{KEY}"));
        assert_eq!(structured.details["resource_created"], false);
        assert_eq!(structured.details["instance_request_sent"], false);
        assert_eq!(structured.details["key_changed"], false);
        assert!(!format!("{error:#} {}", structured.details).contains("response-secret"));
        let requests = server.requests();
        assert!(
            requests
                .iter()
                .any(|r| r.path == format!("/ssh-keys/{KEY}"))
        );
        assert!(
            requests
                .iter()
                .all(|r| r.method == "GET" || r.path == "/oauth2/token")
        );
        assert!(!requests.iter().any(|r| r.path.starts_with("/instances")));
    }
}

#[test]
fn dry_run_quotes_without_calling_the_registered_key_endpoint() {
    let server = Server::new(creation_catalog_reply);
    let (mut config, mut args) = creation_fixture();
    args.dry_run = true;
    Provider::create_with_client(&mut config, &args, |_| Client::mock(&server.endpoint)).unwrap();
    assert!(
        server
            .requests()
            .iter()
            .all(|r| !r.path.starts_with("/ssh-keys") && !r.path.starts_with("/instances"))
    );
}

#[cfg(unix)]
#[test]
fn create_with_singleton_key_requires_matching_local_identity_before_instance_post() {
    let root = tempfile::tempdir().unwrap();
    let private = root.path().join("identity");
    let generated = std::process::Command::new("ssh-keygen")
        .args(["-q", "-t", "ed25519", "-N", "", "-C", "fixture", "-f"])
        .arg(&private)
        .output()
        .expect("SSH creation regression requires ssh-keygen");
    assert!(generated.status.success());
    let public = std::fs::read_to_string(private.with_extension("pub")).unwrap();
    for matching in [true, false] {
        let mut key = ssh_key_record();
        if matching {
            key["key"] = json!(public);
        }
        let server = Server::new(move |request| {
            if request.path == format!("/ssh-keys/{KEY}") {
                return Reply::json(200, json!([key]));
            }
            if request.method == "GET" && request.path.starts_with("/instances?") {
                return Reply::json(200, json!([]));
            }
            if request.method == "POST" && request.path == "/instances" {
                assert_eq!(request.body["ssh_key_ids"], json!([KEY]));
                // Stop at the mocked create receipt; no SSH or startup follows.
                return Reply::json(200, json!({"unusable_receipt":true}));
            }
            creation_catalog_reply(request)
        });
        let (mut config, args) = creation_fixture();
        config.default.verda.ssh_key_path = Some(private.to_str().unwrap().into());
        let error =
            Provider::create_with_client(&mut config, &args, |_| Client::mock(&server.endpoint))
                .unwrap_err();
        let structured = error
            .downcast_ref::<crate::automation::AgentError>()
            .unwrap();
        let posts = server
            .requests()
            .iter()
            .filter(|r| r.method == "POST" && r.path == "/instances")
            .count();
        if matching {
            assert_eq!(structured.code, "creation_outcome_unknown");
            assert_eq!(posts, 1);
        } else {
            assert_eq!(structured.code, "ssh_key_mismatch");
            assert_eq!(structured.details["stage"], "ssh_key_preflight");
            assert_eq!(structured.details["instance_request_sent"], false);
            assert_eq!(posts, 0);
        }
    }
}

#[test]
fn recorded_catalog_selects_current_image_and_enforces_per_gpu_memory() {
    let selected_defaults = crate::model::VerdaDefaults {
        image: Some("26.04.cuda13.2".into()),
        ..Default::default()
    };
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/verda-catalog-2026-10-01.json"
    ))
    .unwrap();
    let mut cat: Catalog = serde_json::from_value(fixture["catalog"].clone()).unwrap();
    let mut req = requirements();
    req.max_price_per_hr = 2.50;
    req.gpu_count = Some(2);
    req.min_gpu_memory_gb = Some(48.0);
    let selected = cat
        .select(&req, &selected_defaults, Some("2RTX6000ADA.20V"))
        .unwrap();
    assert_eq!(selected.location, "FIN-03");
    assert_eq!(selected.image.image_type, "26.04.cuda13.2");
    assert_eq!(selected.image.id, "e25c357f-01f7-497d-a3fa-36c9c4e27403");
    assert_eq!(selected.gpu_memory_per_gpu_gb, 48.0);
    assert!((selected.hourly_usd - 2.3714).abs() < 1e-9);
    req.min_gpu_memory_gb = Some(49.0);
    assert!(cat.select(&req, &selected_defaults, None).is_err());
    req.min_gpu_memory_gb = Some(48.0);
    req.max_price_per_hr = 2.35; // Compute alone fits; the required disk does not.
    assert!(cat.select(&req, &selected_defaults, None).is_err());
    req.max_price_per_hr = 2.50;
    let defaults = crate::model::VerdaDefaults {
        image: Some("26.04.cuda13.2.docker".into()),
        ..Default::default()
    };
    assert_eq!(
        cat.select(&req, &defaults, None).unwrap().image.image_type,
        "26.04.cuda13.2.docker"
    );
    cat.images.clear();
    let error = cat.select(&req, &selected_defaults, None).unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<crate::automation::AgentError>()
            .unwrap()
            .code,
        "image_not_found"
    );
}

#[test]
fn image_selection_uses_provider_compatibility_not_cuda_policy() {
    for kind in [
        "debian-12",
        "ubuntu-22.04",
        "26.04.base",
        "24.04.cuda12.8",
        "jupyter.cuda.13.2",
    ] {
        let mut cat = catalog();
        cat.images[1].image_type = kind.into();
        cat.machines[0].supported_os = vec![kind.into()];
        assert_eq!(
            cat.select(&requirements(), &image_defaults(), None)
                .unwrap()
                .image
                .image_type,
            kind
        );
    }
    let mut image = catalog().images[0].clone();
    for kind in [
        "26.04.cuda13.2.cc",
        "26.04.cuda13.2.kubernetes-1.35.8",
        "ubuntu-26.04-cluster",
    ] {
        image.image_type = kind.into();
        assert!(!catalog::ordinary_image(&image));
    }
    image.image_type = "26.04.base".into();
    image.is_cluster = true;
    assert!(!catalog::ordinary_image(&image));
}

#[test]
fn image_is_required_and_ambiguous_aliases_are_rejected() {
    let err = catalog()
        .select(&requirements(), &Default::default(), None)
        .unwrap_err();
    assert_eq!(
        err.downcast_ref::<crate::automation::AgentError>()
            .unwrap()
            .code,
        "image_required"
    );
    let mut cat = catalog();
    cat.images.push(cat.images[1].clone());
    assert!(
        cat.select(&requirements(), &image_defaults(), None)
            .is_err()
    );
}

#[test]
fn storage_is_in_the_ceiling_and_preview_has_no_enforced_deadline() {
    let offer = offer();
    assert_eq!(offer.allocated_disk_gb, 100);
    assert!((offer.storage_hourly_usd - 0.0274).abs() < 1e-12);
    let mut req = requirements();
    req.max_price_per_hr = 0.62;
    assert!(catalog().select(&req, &image_defaults(), None).is_err());
    let preview = preview(&offer, 0.25, 100).unwrap();
    assert_eq!(preview["cleanup"]["planned_deadline_unix"], 1000);
    assert_eq!(preview["cleanup"]["deadline_enforced"], false);
    assert!(preview.get("profiling").is_none());
    assert_eq!(preview["rental_type"], "on_demand");
    assert_eq!(preview["image"]["source"], "provider_catalog");
    assert_eq!(preview["image"]["local_upload"], false);
    assert_eq!(
        preview["cost"]["estimated_total_usd"],
        offer.hourly_usd * 0.25
    );
}

#[test]
fn on_demand_selection_does_not_substitute_spot_prices_or_claim_spot_capacity() {
    let mut data = serde_json::to_value(catalog()).unwrap();
    data["machines"][0]["spot_price"] = json!(0.01);
    let mut catalog: Catalog = serde_json::from_value(data).unwrap();
    let mut requirements = requirements();
    requirements.max_price_per_hr = 0.5;
    let error = catalog
        .select(&requirements, &image_defaults(), None)
        .unwrap_err();
    let details = &error
        .downcast_ref::<crate::automation::AgentError>()
        .unwrap()
        .details;
    assert_eq!(details["rental_type"], "on_demand");
    assert_eq!(details["rejected_machines"]["resources_or_price"], 1);

    requirements.max_price_per_hr = 0.7;
    let offer = catalog
        .select(&requirements, &image_defaults(), None)
        .unwrap();
    assert_eq!(offer.compute_hourly_usd, 0.6);
    catalog.availability.clear();
    let error = catalog
        .select(&requirements, &image_defaults(), None)
        .unwrap_err();
    let details = &error
        .downcast_ref::<crate::automation::AgentError>()
        .unwrap()
        .details;
    assert_eq!(details["rental_type"], "on_demand");
    assert_eq!(details["rejected_machines"]["availability_or_location"], 1);
}

#[test]
fn live_catalog_shapes_and_authentication_are_supported() {
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            return token();
        }
        let data = serde_json::to_value(catalog()).unwrap();
        let field = if request.path.starts_with("/instance-types?currency=usd") {
            "machines"
        } else if request
            .path
            .starts_with("/instance-availability?is_spot=false")
        {
            "availability"
        } else if request.path.starts_with("/volume-types?") {
            "volume_types"
        } else if request.path.starts_with("/images?") {
            "images"
        } else {
            return Reply::json(404, json!({}));
        };
        Reply::json(200, data[field].clone())
    });
    let chosen = Catalog::load(&server.client)
        .unwrap()
        .select(&requirements(), &image_defaults(), None)
        .unwrap();
    assert_eq!(chosen.image.image_type, "ubuntu-26.04-cuda-13.2-open");
    let requests = server.requests();
    assert_eq!(requests[0].body["grant_type"], "client_credentials");
    assert_eq!(requests[0].body["client_secret"], "test-secret");
    assert!(
        requests[1..]
            .iter()
            .all(|r| r.method == "GET" && r.authorization == "Bearer mock-token")
    );
}

#[test]
fn filters_image_pins_and_availability_are_enforced() {
    let mut req = requirements();
    for count in [0, 2] {
        req.gpu_count = Some(count);
        assert!(catalog().select(&req, &image_defaults(), None).is_err());
    }
    req.gpu_count = Some(1);
    req.min_gpu_memory_gb = Some(49.0);
    assert!(catalog().select(&req, &image_defaults(), None).is_err());
    req.min_gpu_memory_gb = Some(48.0);
    req.allowed_gpus = vec!["RTX A6000".into()];
    let mut defaults = crate::model::VerdaDefaults {
        image: Some("ubuntu-24.04-cuda-12.9-open".into()),
        ..Default::default()
    };
    assert_eq!(
        catalog()
            .select(&req, &defaults, None)
            .unwrap()
            .image
            .image_type,
        defaults.image.as_ref().unwrap().as_str()
    );
    defaults.location = Some("FIN-02".into());
    assert!(catalog().select(&req, &defaults, None).is_err());
    defaults.location = None;
    req.min_cpus = 11;
    assert!(catalog().select(&req, &defaults, None).is_err());
    req.min_cpus = 0;
    req.min_ram_gb = 65.0;
    assert!(catalog().select(&req, &defaults, None).is_err());
    req.min_ram_gb = 0.0;
    req.allowed_gpus = vec!["H100".into()];
    assert!(catalog().select(&req, &defaults, None).is_err());
    let mut cat = catalog();
    cat.availability.clear();
    assert!(
        cat.select(&requirements(), &image_defaults(), None)
            .is_err()
    );
    assert!(
        catalog()
            .select(&requirements(), &image_defaults(), Some("missing"))
            .is_err()
    );
}

#[test]
fn unknown_prices_currencies_and_non_vm_types_are_not_accepted() {
    for price in [
        Value::Null,
        json!("NaN"),
        json!(0),
        json!(-1),
        json!("infinity"),
    ] {
        let mut cat = catalog();
        cat.machines[0].price_per_hour = price;
        assert!(
            cat.select(&requirements(), &image_defaults(), None)
                .is_err()
        );
    }
    let mut cat = catalog();
    cat.volume_types[0].price.currency = "eur".into();
    assert!(
        cat.select(&requirements(), &image_defaults(), None)
            .is_err()
    );
    let mut cat = catalog();
    cat.machines[0].currency = "eur".into();
    assert!(
        cat.select(&requirements(), &image_defaults(), None)
            .is_err()
    );
    let mut cat = catalog();
    cat.machines[0].instance_type = "8H100.80S".into();
    assert!(
        cat.select(&requirements(), &image_defaults(), None)
            .is_err()
    );
}

#[test]
fn incompatible_and_confidential_images_are_rejected() {
    let mut cat = catalog();
    cat.machines[0].supported_os.clear();
    assert!(
        cat.select(&requirements(), &image_defaults(), None)
            .is_err()
    );
    let mut cat = catalog();
    for image in &mut cat.images {
        image.category = "confidentialComputing".into();
    }
    assert!(
        cat.select(&requirements(), &image_defaults(), None)
            .is_err()
    );
    let mut cat = catalog();
    cat.machines[0].description = "Confidential Computing".into();
    assert!(
        cat.select(&requirements(), &image_defaults(), None)
            .is_err()
    );
}

#[test]
fn create_sends_one_fixed_price_vm_with_priced_storage_and_no_scheduler_claim() {
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            token()
        } else {
            Reply::json(202, json!(ID))
        }
    });
    assert_eq!(
        create_vm(&server.client, &offer(), "ice-test", KEY).unwrap(),
        ID
    );
    let requests = server.requests();
    let request = &requests[1];
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/instances");
    assert_eq!(request.body["os_volume"]["size"], 100);
    assert_eq!(request.body["contract"], "PAY_AS_YOU_GO");
    assert_eq!(request.body["is_spot"], false);
    assert_eq!(request.body["ssh_key_ids"], json!([KEY]));
    assert!(request.body.get("startup_script_id").is_none());
}

#[test]
fn ambiguous_creation_is_not_replayed_and_preserves_hostname() {
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            token()
        } else {
            Reply::json(503, json!({"message":"test-secret"}))
        }
    });
    let error = create_vm(&server.client, &offer(), "ice-reconcile", KEY).unwrap_err();
    let details = &error
        .downcast_ref::<crate::automation::AgentError>()
        .unwrap()
        .details;
    assert_eq!(details["instance_name"], "ice-reconcile");
    assert_eq!(details["reconcile_before_retry"], true);
    assert!(!format!("{error:#}").contains("test-secret"));
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.path == "/instances")
            .count(),
        1
    );
}

#[test]
fn instance_creation_accepts_only_a_valid_bare_uuid_receipt() {
    for receipt in [
        ID.to_owned(),
        format!("{ID}\n"),
        json!(ID).to_string(),
        json!({"id":ID}).to_string(),
    ] {
        let server = Server::new(move |request| {
            if request.path == "/oauth2/token" {
                token()
            } else {
                Reply::raw(202, &receipt)
            }
        });
        assert_eq!(
            create_vm(&server.client, &offer(), "ice-test", KEY).unwrap(),
            ID
        );
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|r| r.path == "/instances")
                .count(),
            1
        );
    }
    for receipt in [
        "",
        "test-secret",
        "{}",
        "null",
        "{\"id\":\"bad\"}",
        "11111111-1111-4111-8111-11111111111g",
    ] {
        let server = Server::new(move |request| {
            if request.path == "/oauth2/token" {
                token()
            } else {
                Reply::raw(202, receipt)
            }
        });
        let err = create_vm(&server.client, &offer(), "ice-reconcile", KEY).unwrap_err();
        let details = &err
            .downcast_ref::<crate::automation::AgentError>()
            .unwrap()
            .details;
        assert_eq!(details["instance_name"], "ice-reconcile");
        assert_eq!(details["reconcile_before_retry"], true);
        assert!(!format!("{err:#}").contains("test-secret"));
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|r| r.path == "/instances")
                .count(),
            1
        );
    }
}

#[test]
fn bare_uuid_response_is_not_accepted_for_other_operations() {
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            token()
        } else {
            Reply::raw(200, ID)
        }
    });
    assert!(
        server
            .client
            .get::<Value>("/instances", Duration::from_secs(1))
            .is_err()
    );
    assert!(
        server
            .client
            .mutate(Method::PUT, "/instances", &json!({}))
            .is_err()
    );
    assert!(
        server
            .client
            .mutate(Method::POST, "/volumes", &json!({}))
            .is_err()
    );
    assert_eq!(server.requests().len(), 4);
}

#[test]
fn startup_failure_returns_instance_and_volume_cleanup_details() {
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            token()
        } else {
            Reply::json(200, instance("installation_failed"))
        }
    });
    let error = server
        .client
        .wait(ID, true, Duration::from_secs(1))
        .unwrap_err();
    let error = error
        .downcast_ref::<crate::automation::AgentError>()
        .unwrap();
    assert_eq!(error.code, "instance_startup_failed");
    assert_eq!(error.details["instance_id"], ID);
    assert_eq!(error.details["os_volume_id"], OS);
    assert_eq!(error.details["volume_ids"], json!([OS, EXTRA]));
    assert!(
        error.details["cleanup_command"]
            .as_str()
            .unwrap()
            .contains(ID)
    );
}

#[test]
fn readiness_wait_is_bounded_and_unknown_state_does_not_pass() {
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            token()
        } else {
            Reply::json(200, instance("future_state"))
        }
    });
    let start = Instant::now();
    let error = server
        .client
        .wait(ID, true, Duration::from_millis(100))
        .unwrap_err();
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(
        error
            .downcast_ref::<crate::automation::AgentError>()
            .unwrap()
            .code,
        "startup_timeout"
    );
}

#[test]
fn action_partial_failure_is_not_success() {
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            token()
        } else {
            Reply::json(
                207,
                json!([{"instanceId":ID,"action":"shutdown","status":"error","error":"secret"}]),
            )
        }
    });
    let err = server.client.action(ID, "shutdown", None).unwrap_err();
    let details = &err
        .downcast_ref::<crate::automation::AgentError>()
        .unwrap()
        .details;
    assert_eq!(details["receipt"]["rows"][0]["status"], "error");
    assert_eq!(details["receipt"]["rows"][0]["fields"]["error"], "string");
    assert!(!details.to_string().contains("secret"));
}

#[test]
fn action_diagnostics_retain_bounded_shape_without_provider_strings() {
    for response in [
        json!({"secret-key":"secret-value"}),
        json!(vec![
            json!({"instanceId":EXTRA,"action":"secret-action","status":"secret-status",
            "error":"secret-error","statusCode":400,"secret-key":"secret-value"});
            6
        ]),
    ] {
        let summary = deletion::receipt_summary(&response, ID, "delete");
        assert!(!summary.to_string().contains("secret"));
        if response.is_array() {
            assert_eq!(summary["row_count"], 6);
            assert_eq!(summary["rows"].as_array().unwrap().len(), 4);
            assert_eq!(summary["rows"][0]["instance_id_matches"], false);
            assert_eq!(summary["rows"][0]["action_matches"], false);
            assert_eq!(summary["rows"][0]["status_code"], 400);
            assert_eq!(summary["rows"][0]["unknown_field_count"], 1);
        }
    }
}

#[test]
fn start_and_stop_map_to_verda_actions_without_deleting_storage() {
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            token()
        } else {
            Reply::json(204, Value::Null)
        }
    });
    let vm: Instance = serde_json::from_value(instance("running")).unwrap();
    Provider::set_running(&server.client, &vm, false).unwrap();
    Provider::set_running(&server.client, &vm, true).unwrap();
    let actions: Vec<_> = server
        .requests()
        .into_iter()
        .filter(|r| r.path == "/instances")
        .collect();
    assert_eq!(actions[0].body, json!({"id":ID,"action":"shutdown"}));
    assert_eq!(actions[1].body, json!({"id":ID,"action":"start"}));
    let summary = vm.json_summary().to_string();
    assert!(!summary.contains("must-not-appear"));
    assert!(vm.json_summary().get("profiling_access").is_none());
}

#[test]
fn delete_only_removes_boot_volume_and_verifies_absence() {
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            token()
        } else if request.method == "PUT" {
            Reply::json(
                202,
                json!([{"instanceId":ID,"action":"delete","status":"success"}]),
            )
        } else if request.path.starts_with("/volumes/trash?")
            || request.path.starts_with("/instances?")
            || request.path.starts_with("/volumes?")
        {
            Reply::json(200, json!([]))
        } else {
            Reply::json(404, json!({}))
        }
    });
    let vm: Instance = serde_json::from_value(instance("offline")).unwrap();
    let receipt = delete_selected(&server.client, &vm, Duration::from_secs(1)).unwrap();
    assert_eq!(receipt["verification"], "read_back_absent");
    assert_eq!(receipt["retained_volume_ids"], json!([EXTRA]));
    let request = server
        .requests()
        .into_iter()
        .find(|r| r.method == "PUT")
        .unwrap();
    assert_eq!(request.body["volume_ids"], json!([OS]));
    assert_eq!(request.body["delete_permanently"], true);
}

#[test]
fn retained_boot_volume_prevents_claiming_verified_deletion() {
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            token()
        } else if request.method == "PUT" {
            Reply::json(204, Value::Null)
        } else if request.path.starts_with("/volumes/") {
            Reply::json(200, json!({"id":OS,"status":"deleting"}))
        } else {
            Reply::json(404, json!({}))
        }
    });
    let vm: Instance = serde_json::from_value(instance("offline")).unwrap();
    let error = delete_selected(&server.client, &vm, Duration::from_millis(100)).unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<crate::automation::AgentError>()
            .unwrap()
            .code,
        "deletion_unverified"
    );
}

// Model the retained HTTP 200 records from the 2026-10-01 Ada trial.
fn terminal_deletion_reply(request: &Request) -> Reply {
    match request.path.as_str() {
        "/oauth2/token" => token(),
        "/instances" if request.method == "PUT" => Reply::json(204, Value::Null),
        p if p == format!("/instances/{ID}") => {
            Reply::json(200, json!({"id":ID,"status":"discontinued"}))
        }
        p if p == format!("/volumes/{OS}") => Reply::json(
            200,
            json!({"id":OS,"status":"deleted","is_permanently_deleted":true}),
        ),
        p if p.starts_with("/instances?")
            || p.starts_with("/volumes?")
            || p.starts_with("/volumes/trash?") =>
        {
            Reply::json(200, json!([]))
        }
        _ => panic!("Unexpected request: {request:?}"),
    }
}

#[test]
fn deletion_reconciles_terminal_records_after_ambiguous_receipts_without_replay() {
    for body in [
        None,
        Some("{}"),
        Some("not JSON"),
        Some("[{\"status\":\"error\",\"error\":\"secret\"}]"),
    ] {
        let server = Server::new(move |request| {
            if request.method == "PUT"
                && let Some(body) = body
            {
                return Reply::raw(202, body);
            }
            terminal_deletion_reply(request)
        });
        let vm: Instance = serde_json::from_value(instance("running")).unwrap();
        let receipt = delete_selected(&server.client, &vm, Duration::from_secs(1)).unwrap();
        assert_eq!(
            receipt["verification"],
            "terminal_deletion_and_active_absence_verified"
        );
        assert_eq!(receipt["action_receipt_confirmed"], body.is_none());
        assert_eq!(receipt["instance_state"], "discontinued");
        assert_eq!(receipt["os_volume_states"][OS], "permanently_deleted");
        assert_eq!(receipt["retained_volume_ids"], json!([EXTRA]));
        assert_eq!(receipt["retained_storage_charges_continue"], true);
        assert!(!receipt.to_string().contains("secret"));
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|r| r.method == "PUT")
                .count(),
            1
        );
        assert!(server.requests().iter().all(|r| !r.path.contains(EXTRA)));
    }
}

#[test]
fn deletion_reconciles_http_failure_without_replaying_mutation() {
    let server = Server::new(|request| {
        if request.method == "PUT" {
            Reply::json(503, json!({"error":"secret"}))
        } else {
            terminal_deletion_reply(request)
        }
    });
    let vm: Instance = serde_json::from_value(instance("running")).unwrap();
    let receipt = delete_selected(&server.client, &vm, Duration::from_secs(1)).unwrap();
    assert_eq!(receipt["action_receipt_confirmed"], false);
    assert_eq!(receipt["action_error"]["details"]["http_status"], 503);
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.method == "PUT")
            .count(),
        1
    );
}

#[test]
fn deletion_never_confuses_terminal_metadata_with_verified_absence() {
    for fault in [
        "vm_id",
        "volume_id",
        "vm_status",
        "volume_status",
        "soft_deleted",
        "missing_permanent_flag",
        "active_vm",
        "active_volume",
        "trash",
        "malformed_list",
        "failed_read",
    ] {
        let server = Server::new(move |request| {
            if request.method == "PUT" {
                return Reply::json(207, json!([{"status":"error","error":"secret"}]));
            }
            let mut reply = terminal_deletion_reply(request);
            if request.path == format!("/instances/{ID}") {
                match fault {
                    "vm_id" => reply.body["id"] = json!(EXTRA),
                    "vm_status" => reply.body["status"] = json!("offline"),
                    "failed_read" => return Reply::json(403, json!({"error":"secret"})),
                    _ => (),
                }
            } else if request.path == format!("/volumes/{OS}") {
                match fault {
                    "volume_id" => reply.body["id"] = json!(EXTRA),
                    "volume_status" => reply.body["status"] = json!("deleting"),
                    "soft_deleted" => reply.body["is_permanently_deleted"] = json!(false),
                    "missing_permanent_flag" => {
                        reply
                            .body
                            .as_object_mut()
                            .unwrap()
                            .remove("is_permanently_deleted");
                    }
                    _ => (),
                }
            } else if request.path.starts_with("/instances?") && fault == "active_vm" {
                reply.body = json!([{"id":ID}]);
            } else if (request.path.starts_with("/volumes?") && fault == "active_volume")
                || (request.path.starts_with("/volumes/trash?") && fault == "trash")
            {
                reply.body = json!([{"id":OS}]);
            } else if request.path.starts_with("/volumes?") && fault == "malformed_list" {
                reply.body = json!([{"status":"secret"}]);
            }
            reply
        });
        let vm: Instance = serde_json::from_value(instance("running")).unwrap();
        let start = Instant::now();
        let err = delete_selected(&server.client, &vm, Duration::from_millis(150)).unwrap_err();
        assert!(start.elapsed() < Duration::from_secs(1), "{fault}");
        let err = err.downcast_ref::<crate::automation::AgentError>().unwrap();
        assert_eq!(
            err.code,
            if fault == "trash" {
                "storage_cleanup_unverified"
            } else {
                "deletion_unverified"
            },
            "{fault}"
        );
        assert_eq!(err.details["instance_id"], ID);
        assert_eq!(err.details["os_volume_ids"], json!([OS]));
        assert_eq!(err.details["action_error"]["code"], "verda_action_failed");
        assert!(!err.details.to_string().contains("secret"));
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|r| r.method == "PUT")
                .count(),
            1
        );
    }
}

#[test]
fn deletion_checks_all_active_list_pages_before_claiming_absence() {
    let server = Server::new(|request| {
        if request.path.starts_with("/instances?") {
            let id = if request.path.contains("page=1&") {
                EXTRA
            } else {
                ID
            };
            Reply {
                headers: "X-Total-Count: 2\r\n".into(),
                ..Reply::json(200, json!([{"id":id}]))
            }
        } else {
            terminal_deletion_reply(request)
        }
    });
    let vm: Instance = serde_json::from_value(instance("running")).unwrap();
    assert!(delete_selected(&server.client, &vm, Duration::from_millis(150)).is_err());
    assert!(server.requests().iter().any(|r| r.path.contains("page=2&")));
}

#[test]
fn collection_pagination_is_complete_and_bad_metadata_fails() {
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            return token();
        }
        let page = if request.path.contains("page=1&") {
            1
        } else {
            2
        };
        Reply {
            status: 200,
            body: json!([page]),
            headers: "X-Total-Count: 2\r\n".into(),
            raw_body: None,
        }
    });
    assert_eq!(server.client.list::<u32>("/images").unwrap(), vec![1, 2]);
}

#[test]
fn expired_token_refreshes_reads_but_never_replays_a_create() {
    let attempts = Arc::new(AtomicUsize::new(0));
    let count = attempts.clone();
    let server = Server::new(move |request| {
        if request.path == "/oauth2/token" {
            token()
        } else if count.fetch_add(1, Ordering::SeqCst) == 0 {
            Reply::json(401, json!({}))
        } else {
            Reply::json(200, json!([]))
        }
    });
    assert!(server.client.list::<Value>("/images").unwrap().is_empty());
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.path == "/oauth2/token")
            .count(),
        2
    );
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            token()
        } else {
            Reply::json(401, json!({}))
        }
    });
    assert!(create_vm(&server.client, &offer(), "ice-unknown", KEY).is_err());
    assert_eq!(server.requests().len(), 2);
}

#[test]
fn connection_metadata_rejects_host_injection_and_quotes_key_paths() {
    let mut vm: Instance = serde_json::from_value(instance("running")).unwrap();
    let command =
        ssh::connection_command(&vm, Some(std::path::Path::new("/tmp/key with spaces"))).unwrap();
    assert!(command.contains("'/tmp/key with spaces'"));
    assert!(command.contains("'root@192.0.2.1'"));
    vm.ip = Some("-oProxyCommand=bad".into());
    assert!(ssh::connection_command(&vm, None).is_err());
}

#[test]
fn trashed_os_volume_is_not_permanent_deletion() {
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            token()
        } else if request.method == "PUT" {
            Reply::json(204, Value::Null)
        } else if request.path.starts_with("/volumes/trash?") {
            Reply::json(200, json!([{"id": OS}]))
        } else if request.path.starts_with("/instances?") || request.path.starts_with("/volumes?") {
            Reply::json(200, json!([]))
        } else {
            Reply::json(404, json!({}))
        }
    });
    let vm: Instance = serde_json::from_value(instance("offline")).unwrap();
    let error = delete_selected(&server.client, &vm, Duration::from_secs(1)).unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<crate::automation::AgentError>()
            .unwrap()
            .code,
        "storage_cleanup_unverified"
    );
}

#[test]
fn api_errors_do_not_echo_submitted_credentials() {
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            token()
        } else {
            Reply::json(429, json!({"message": "test-secret mock-token"}))
        }
    });
    let error = server.client.list::<Value>("/images").unwrap_err();
    let text = format!("{error:#}");
    assert!(!text.contains("test-secret"));
    assert!(!text.contains("mock-token"));
    assert_eq!(
        error
            .downcast_ref::<crate::automation::AgentError>()
            .unwrap()
            .details["http_status"],
        429
    );
    assert_eq!(server.requests().len(), 7); // One token exchange and six bounded reads.
}

#[test]
fn read_retries_honor_retry_after_without_extending_deadlines() {
    let hits = Arc::new(AtomicUsize::new(0));
    let count = hits.clone();
    let server = Server::new(move |request| {
        if request.path == "/oauth2/token" {
            return token();
        }
        if count.fetch_add(1, Ordering::SeqCst) == 0 {
            Reply {
                status: 429,
                body: json!({}),
                headers: "Retry-After: 1\r\n".into(),
                raw_body: None,
            }
        } else {
            Reply::json(200, json!([]))
        }
    });
    let start = Instant::now();
    assert!(
        server
            .client
            .get::<Vec<Value>>("/images", Duration::from_secs(3))
            .unwrap()
            .is_empty()
    );
    assert!(start.elapsed() >= Duration::from_secs(1));
    assert_eq!(hits.load(Ordering::SeqCst), 2);
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            token()
        } else {
            Reply {
                status: 429,
                body: json!({}),
                headers: "Retry-After: 60\r\n".into(),
                raw_body: None,
            }
        }
    });
    let start = Instant::now();
    let error = server
        .client
        .get::<Value>("/images", Duration::from_millis(500))
        .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<crate::automation::AgentError>()
            .unwrap()
            .code,
        "verda_retry_deadline"
    );
    assert!(start.elapsed() < Duration::from_secs(1));
    assert_eq!(server.requests().len(), 2);
}

#[test]
fn transient_reads_retry_but_permanent_errors_and_mutations_do_not() {
    for status in [400, 403, 404, 408, 429, 500, 502, 503, 504] {
        let hits = Arc::new(AtomicUsize::new(0));
        let count = hits.clone();
        let server = Server::new(move |request| {
            if request.path == "/oauth2/token" {
                return token();
            }
            if count.fetch_add(1, Ordering::SeqCst) == 0 {
                Reply::json(status, json!({}))
            } else {
                Reply::json(200, json!([]))
            }
        });
        let retry = matches!(status, 408 | 429 | 500 | 502 | 503 | 504);
        assert_eq!(
            server
                .client
                .get::<Value>("/images", Duration::from_secs(1))
                .is_ok(),
            retry
        );
        assert_eq!(hits.load(Ordering::SeqCst), if retry { 2 } else { 1 });
    }
    for method in [Method::POST, Method::PUT, Method::DELETE] {
        for status in [429, 503] {
            let server = Server::new(move |request| {
                if request.path == "/oauth2/token" {
                    token()
                } else {
                    Reply::json(status, json!({}))
                }
            });
            assert!(
                server
                    .client
                    .mutate(method.clone(), "/instances", &json!({}))
                    .is_err()
            );
            assert_eq!(server.requests().len(), 2);
        }
    }
}

#[test]
fn mismatched_instance_receipt_cannot_target_another_vm() {
    let server = Server::new(|request| {
        if request.path == "/oauth2/token" {
            token()
        } else {
            let mut vm = instance("running");
            vm["id"] = json!(EXTRA);
            Reply::json(200, vm)
        }
    });
    let error = server
        .client
        .instance(ID, Duration::from_secs(1))
        .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<crate::automation::AgentError>()
            .unwrap()
            .code,
        "verda_response_mismatch"
    );
    assert!(
        server
            .requests()
            .iter()
            .all(|r| r.method == "GET" || r.path == "/oauth2/token")
    );
}

#[test]
fn lifecycle_reconciles_an_ambiguous_receipt_without_replaying_shutdown() {
    let server = Server::new(|request| match request.path.as_str() {
        "/oauth2/token" => token(),
        "/instances" => Reply::json(207, json!([{"status":"unknown", "secret":"never-echo"}])),
        path if path == format!("/instances/{ID}") => Reply::json(200, instance("offline")),
        _ => panic!("Unexpected request"),
    });
    let vm: Instance = serde_json::from_value(instance("running")).unwrap();
    let _budget = crate::lifecycle::Budget::enter(Duration::from_secs(2)).unwrap();
    let result = crate::lifecycle::transition::<Provider>(
        &server.client,
        &vm,
        crate::lifecycle::Action::Stop,
    )
    .unwrap();
    assert_eq!(result["outcome"], "verified");
    assert_eq!(result["state"], "stopped");
    assert_eq!(result["request"], "unconfirmed");
    assert_eq!(result["instance_id"], ID);
    assert!(
        result["billing_note"]
            .as_str()
            .unwrap()
            .contains("compute and storage")
    );
    assert!(!result.to_string().contains("never-echo"));
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.method == "PUT")
            .count(),
        1
    );
}

#[test]
fn lifecycle_deadline_preserves_identity_and_never_repeats_a_mutation() {
    let server = Server::new(|request| match request.path.as_str() {
        "/oauth2/token" => token(),
        "/instances" => Reply::json(204, Value::Null),
        path if path == format!("/instances/{ID}") => Reply::json(200, instance("running")),
        _ => panic!("Unexpected request"),
    });
    let vm: Instance = serde_json::from_value(instance("running")).unwrap();
    let _budget = crate::lifecycle::Budget::enter(Duration::from_millis(150)).unwrap();
    let error = crate::lifecycle::transition::<Provider>(
        &server.client,
        &vm,
        crate::lifecycle::Action::Stop,
    )
    .unwrap_err();
    let error = error
        .downcast_ref::<crate::automation::AgentError>()
        .unwrap();
    assert_eq!(error.code, "operation_unverified");
    assert_eq!(error.details["instance_id"], ID);
    assert_eq!(error.details["instance"]["os_volume_id"], OS);
    assert_eq!(
        error.details["verification_error_code"],
        "operation_timeout"
    );
    assert_eq!(error.details["request"], "acknowledged");
    assert_eq!(
        server
            .requests()
            .iter()
            .filter(|r| r.method == "PUT")
            .count(),
        1
    );
}

#[test]
fn already_running_lifecycle_reuses_the_fresh_lookup_without_another_request() {
    let server = Server::new(|_| panic!("No additional provider request is needed"));
    let vm: Instance = serde_json::from_value(instance("running")).unwrap();
    let _budget = crate::lifecycle::Budget::enter(Duration::from_secs(2)).unwrap();
    let result = crate::lifecycle::transition::<Provider>(
        &server.client,
        &vm,
        crate::lifecycle::Action::Start,
    )
    .unwrap();
    assert_eq!(result["request"], "not_needed");
    assert_eq!(result["verification"], "read_back");
    assert!(server.requests().is_empty());
}
