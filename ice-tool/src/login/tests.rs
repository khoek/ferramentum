use super::*;
use std::cell::RefCell;

fn request(cloud: Cloud) -> CredentialLogin<String> {
    CredentialLogin {
        cloud,
        environment: None,
        cached: None,
        force: false,
        interactive: true,
    }
}

#[test]
fn environment_wins_even_with_force_and_is_never_saved() {
    for cloud in [Cloud::VastAi, Cloud::Verda] {
        for force in [false, true] {
            let login = CredentialLogin {
                environment: Some("environment".into()),
                cached: Some("saved".into()),
                force,
                interactive: false,
                ..request(cloud)
            };
            let outcome = login
                .run(
                    |value| {
                        assert_eq!(value, "environment");
                        Ok(())
                    },
                    || panic!("must not prompt"),
                    |_| panic!("must not save environment credentials"),
                )
                .unwrap();
            assert!(matches!(outcome.method, LoginMethod::AutoDetected));
            assert!(outcome.saved_path.is_none());
        }
    }
}

#[test]
fn invalid_environment_never_falls_back_to_saved_credentials_or_prompt() {
    let login = CredentialLogin {
        environment: Some("invalid environment".into()),
        cached: Some("valid saved".into()),
        ..request(Cloud::Verda)
    };
    let error = login
        .run(
            |value| {
                assert_eq!(value, "invalid environment");
                bail!("validation failed")
            },
            || panic!("must not prompt"),
            |_| panic!("must not save"),
        )
        .unwrap_err();
    assert_eq!(error.to_string(), "validation failed");
}

#[test]
fn saved_credentials_are_validated_and_reused_without_writing() {
    for cloud in [Cloud::VastAi, Cloud::Verda] {
        let outcome = CredentialLogin {
            cached: Some("saved".into()),
            interactive: false,
            ..request(cloud)
        }
        .run(
            |value| {
                assert_eq!(value, "saved");
                Ok(())
            },
            || panic!("must not prompt"),
            |_| panic!("must not rewrite saved credentials"),
        )
        .unwrap();
        assert!(matches!(outcome.method, LoginMethod::Cached));
        assert!(outcome.saved_path.is_none());
    }
}

#[test]
fn force_skips_saved_validation_and_saves_only_the_validated_replacement() {
    let events = RefCell::new(Vec::new());
    let outcome = CredentialLogin {
        cached: Some("old".into()),
        force: true,
        ..request(Cloud::Verda)
    }
    .run(
        |value| {
            assert_eq!(value, "new");
            events.borrow_mut().push("validate");
            Ok(())
        },
        || {
            events.borrow_mut().push("prompt");
            Ok("new".into())
        },
        |value| {
            assert_eq!(value, "new");
            events.borrow_mut().push("save");
            Ok("config.toml".into())
        },
    )
    .unwrap();
    assert_eq!(*events.borrow(), ["prompt", "validate", "save"]);
    assert!(matches!(outcome.method, LoginMethod::Prompted));
    assert_eq!(outcome.saved_path, Some("config.toml".into()));
}

#[test]
fn invalid_saved_credentials_can_be_replaced_interactively() {
    let checked = RefCell::new(Vec::new());
    let outcome = CredentialLogin {
        cached: Some("old".into()),
        ..request(Cloud::Verda)
    }
    .run(
        |value| {
            checked.borrow_mut().push(value.clone());
            if value == "old" {
                bail!("rejected");
            }
            Ok(())
        },
        || Ok("new".into()),
        |value| {
            assert_eq!(value, "new");
            Ok("config.toml".into())
        },
    )
    .unwrap();
    assert_eq!(*checked.borrow(), ["old", "new"]);
    assert!(matches!(outcome.method, LoginMethod::Prompted));
}

#[test]
fn noninteractive_failures_never_prompt_or_save_even_with_force() {
    for cloud in [Cloud::VastAi, Cloud::Verda] {
        for force in [false, true] {
            for cached in [None, Some("old".into())] {
                let validates_cached = cached.is_some() && !force;
                let login = CredentialLogin {
                    cached,
                    force,
                    interactive: false,
                    ..request(cloud)
                };
                let error = login
                    .run(
                        |_| {
                            assert!(validates_cached);
                            bail!("rejected")
                        },
                        || panic!("must not prompt"),
                        |_| panic!("must not save"),
                    )
                    .unwrap_err();
                if validates_cached {
                    assert_eq!(error.to_string(), "rejected");
                } else {
                    assert_eq!(
                        error
                            .downcast_ref::<crate::automation::AgentError>()
                            .unwrap()
                            .code,
                        "authentication_required"
                    );
                }
            }
        }
    }
}

#[test]
fn rejected_or_cancelled_input_never_overwrites_saved_credentials() {
    for cancelled in [false, true] {
        let login = CredentialLogin {
            force: true,
            cached: Some("old".into()),
            ..request(Cloud::Verda)
        };
        let error = login
            .run(
                |_| bail!("rejected"),
                || {
                    if cancelled {
                        bail!("cancelled")
                    } else {
                        Ok("new".into())
                    }
                },
                |_| panic!("must preserve the previous config"),
            )
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            if cancelled { "cancelled" } else { "rejected" }
        );
    }
}

#[test]
fn save_failure_does_not_report_ready() {
    let error = request(Cloud::Verda)
        .run(|_| Ok(()), || Ok("new".into()), |_| bail!("disk full"))
        .unwrap_err();
    assert_eq!(error.to_string(), "disk full");
}
