use super::*;

const PUBLIC: &str = "ssh-ed25519 AAAACorrect registered-key";
const OTHER: &str = "ssh-ed25519 AAAAOther different-key";

fn registered() -> Vec<SshKey> {
    vec![SshKey {
        id: "selected-key".into(),
        key: Some(PUBLIC.into()),
    }]
}

#[test]
fn key_selection_uses_the_matching_private_key_not_the_first_key() {
    let paths = vec![PathBuf::from("first"), PathBuf::from("second")];
    let selected = choose_identity(
        &registered(),
        &paths,
        false,
        |path| {
            Ok(Some(
                if path == paths[0] {
                    OTHER
                } else {
                    "ssh-ed25519 AAAACorrect another-comment"
                }
                .into(),
            ))
        },
        || panic!("a matching private key does not require an agent"),
    )
    .unwrap();
    assert_eq!(selected, Some(paths[1].clone()));
}

#[test]
fn explicit_wrong_key_is_not_silently_replaced_by_an_agent_key() {
    let error = choose_identity(
        &registered(),
        &[PathBuf::from("wrong")],
        true,
        |_| Ok(Some(OTHER.into())),
        || Ok(Some(PUBLIC.into())),
    )
    .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<crate::automation::AgentError>()
            .unwrap()
            .code,
        "ssh_key_mismatch"
    );
}

#[test]
fn agent_only_identity_must_match_the_registered_public_key() {
    assert!(
        choose_identity(
            &registered(),
            &[],
            false,
            |_| panic!("no private paths"),
            || Ok(Some(PUBLIC.into()))
        )
        .unwrap()
        .is_none()
    );
    assert!(
        choose_identity(
            &registered(),
            &[],
            false,
            |_| panic!("no private paths"),
            || Ok(Some(OTHER.into()))
        )
        .is_err()
    );
}

#[test]
fn encrypted_key_requires_matching_public_counterpart_and_loaded_agent_identity() {
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("key with spaces.pem");
    fs::write(&path, "encrypted fixture").unwrap();
    fs::write(root.path().join("key with spaces.pem.pub"), PUBLIC).unwrap();
    for agent in [None, Some(OTHER.into()), Some(PUBLIC.into())] {
        let expected = agent.as_deref() == Some(PUBLIC);
        let result = choose_identity(
            &registered(),
            std::slice::from_ref(&path),
            true,
            |_| Ok(None),
            || Ok(agent),
        );
        assert_eq!(result.is_ok(), expected);
        if let Ok(selected) = result {
            assert_eq!(selected, Some(path.clone()));
        }
    }
}

#[test]
fn candidate_discovery_keeps_custom_key_filenames_and_honors_explicit_paths() {
    let root = tempfile::tempdir().unwrap();
    let private = root.path().join("custom.pem");
    fs::write(&private, "private fixture").unwrap();
    fs::write(root.path().join("custom.pem.pub"), PUBLIC).unwrap();
    let paths = candidate_paths(None, Some(root.path())).unwrap();
    assert_eq!(paths, vec![private.clone()]);
    assert_eq!(candidate_paths(Some(&private), None).unwrap(), paths);
    assert!(candidate_paths(Some(&root.path().join("missing")), Some(root.path())).is_err());
}

#[cfg(unix)]
#[test]
fn local_key_commands_are_bounded_and_capture_only_successful_public_output() {
    let result = local_output(
        Command::new("/bin/sh").args(["-c", "printf 'public-key'"]),
        Instant::now() + Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(result.as_deref(), Some("public-key"));
    let start = Instant::now();
    let result = local_output(
        Command::new("/bin/sh").args(["-c", "exec /bin/sleep 30"]),
        start + Duration::from_millis(50),
    )
    .unwrap();
    assert!(result.is_none());
    assert!(start.elapsed() < Duration::from_secs(1));
}
