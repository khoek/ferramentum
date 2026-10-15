use super::*;
use serde_json::json;
use std::fs;

fn proc_stat(root: &Path, pid: u32, parent: u32, name: &str, start: u64) {
    let dir = root.join(pid.to_string());
    fs::create_dir_all(&dir).unwrap();
    fs::write(
        dir.join("stat"),
        format!(
            "{pid} ({name}) S {parent} {} {start}",
            vec!["0"; 17].join(" ")
        ),
    )
    .unwrap();
    fs::write(dir.join("environ"), b"TERM=foot\0").unwrap();
}

fn fixture() -> (tempfile::TempDir, ProcessIdentity) {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join("self")).unwrap();
    proc_stat(dir.path(), 10, 20, "kai", 100);
    proc_stat(dir.path(), 20, 30, "bash", 90);
    proc_stat(dir.path(), 30, 0, "foot", 80);
    (
        dir,
        ProcessIdentity {
            pid: 10,
            start_ticks: 100,
        },
    )
}

fn tree(focused: bool) -> Value {
    json!({"id":1, "nodes":[{"id":2,"floating_nodes":[{"id":42,"pid":30,"focused":focused}]}]})
}

#[test]
fn observes_focus_read_only_and_rejects_shared_windows_and_reused_processes() {
    let (dir, identity) = fixture();
    let targets = [Target { identity }];
    let query = |value: Value| {
        focused_with(dir.path(), &targets, Backend::Sway, |program, args| {
            assert_eq!(program, "swaymsg");
            assert_eq!(args, ["-r", "-t", "get_tree"]);
            Ok(serde_json::to_vec(&value).unwrap())
        })
        .unwrap()
        .identities
    };
    assert_eq!(query(tree(true)), [identity]);
    assert!(query(tree(false)).is_empty());
    assert!(
        query(json!({"id": 0, "nodes": [
            {"id":42,"pid":30,"focused":true}, {"id":43,"pid":30}
        ]}))
        .is_empty()
    );
    proc_stat(dir.path(), 10, 20, "kai", 101);
    assert!(query(tree(true)).is_empty());
}

#[test]
fn observes_hyprland_x11_and_gnome_without_activation_commands() {
    let (dir, identity) = fixture();
    let targets = [Target { identity }];
    for backend in [Backend::Hyprland, Backend::X11, Backend::Gnome] {
        let observed = focused_with(dir.path(), &targets, backend, |program, args| {
            Ok(match (program, args) {
                ("hyprctl", ["-j", "clients"]) => br#"[{"address":"0x2a","pid":30}]"#.to_vec(),
                ("hyprctl", ["-j", "activewindow"]) => br#"{"address":"0x2a","pid":30}"#.to_vec(),
                ("wmctrl", ["-lp"]) => b"0x2a 0 30 hostname a title\n".to_vec(),
                ("xprop", ["-root", "_NET_ACTIVE_WINDOW"]) => {
                    b"_NET_ACTIVE_WINDOW(WINDOW): window id # 0x2a\n".to_vec()
                }
                ("gdbus", _)
                    if args.last()
                        == Some(&"org.gnome.Shell.Extensions.KaiWindowFocus.GetWindows") =>
                {
                    format!("('{}',)", tree(true)).into_bytes()
                }
                _ => panic!("unexpected or mutating command: {program} {args:?}"),
            })
        })
        .unwrap();
        assert_eq!(observed.identities, [identity]);
    }
    let observed = focused_with(dir.path(), &targets, Backend::Hyprland, |_, args| {
        Ok(if args == ["-j", "clients"] {
            br#"[{"address":"0x2a","pid":30}]"#.to_vec()
        } else {
            b"{}".to_vec()
        })
    })
    .unwrap();
    assert!(observed.identities.is_empty());
}

#[test]
fn focuses_the_exact_window_and_checks_the_desktop_reply() {
    let (dir, identity) = fixture();
    let mut calls = Vec::new();
    focus_sway(dir.path(), identity, |kind, payload| {
        calls.push((kind, payload.to_owned()));
        Ok(if kind == 0 {
            json!([{"success":true}])
        } else {
            tree(calls.len() == 3)
        })
    })
    .unwrap();
    assert_eq!(
        calls,
        [
            (4, "".into()),
            (0, "[con_id=42] focus".into()),
            (4, "".into())
        ]
    );

    for reply in [
        json!([]),
        json!([{"success":false}]),
        json!({"success":true}),
    ] {
        assert!(
            focus_sway(dir.path(), identity, |kind, _| Ok(if kind == 0 {
                reply.clone()
            } else {
                tree(false)
            }))
            .is_err()
        );
    }
    assert!(
        focus_sway(dir.path(), identity, |kind, _| Ok(if kind == 0 {
            json!([{"success":true}])
        } else {
            tree(false)
        }))
        .unwrap_err()
        .to_string()
        .contains("did not receive focus")
    );
}

#[test]
fn refuses_pid_reuse_and_multiplexers_before_dispatch() {
    let (dir, identity) = fixture();
    let mut dispatches = 0;
    let result = focus_sway(dir.path(), identity, |kind, _| {
        if kind == 0 {
            dispatches += 1;
        }
        proc_stat(dir.path(), 30, 0, "foot", 999);
        Ok(tree(false))
    });
    assert!(result.is_err());
    assert_eq!(dispatches, 0);
    fs::write(dir.path().join("10/environ"), b"TMUX=/tmp/tmux\0").unwrap();
    assert!(
        focus_sway(dir.path(), identity, |_, _| panic!(
            "must not contact desktop"
        ))
        .unwrap_err()
        .to_string()
        .contains("tab/pane")
    );
    assert!(
        process::ancestry(
            dir.path(),
            ProcessIdentity {
                start_ticks: 101,
                ..identity
            }
        )
        .is_err()
    );
    proc_stat(dir.path(), 30, 20, "foot", 80);
    assert!(process::ancestry(dir.path(), identity).is_err());
}

#[test]
fn refuses_ambiguous_terminals_and_chooses_nearest_ancestor() {
    let (dir, identity) = fixture();
    let mut chain = process::ancestry(dir.path(), identity).unwrap();
    chain.push(Ancestor {
        identity: ProcessIdentity {
            pid: 50,
            start_ticks: 50,
        },
        name: "alacritty".into(),
    });
    let tree = root(vec![window(50, 50), window(42, 30)]);
    assert_eq!(resolve(&tree, &chain).unwrap().id, 42);
    let ambiguous = root(vec![window(42, 30), window(43, 30)]);
    assert!(
        resolve(&ambiguous, &chain)
            .err()
            .unwrap()
            .to_string()
            .contains("Several windows")
    );
    chain[2].name = "kitty".into();
    assert!(
        resolve(&tree, &chain)
            .err()
            .unwrap()
            .to_string()
            .contains("tab/pane")
    );
}

#[test]
fn parses_hyprland_and_x11_ids_without_titles_or_command_interpolation() {
    let tree = hyprland_tree(br#"[{"address":"0xabc","pid":30,"mapped":true},{"address":"0xdef","pid":30,"mapped":false}]"#).unwrap();
    assert_eq!(tree.windows().count(), 1);
    assert_eq!(tree.nodes[0].id, 0xabc);
    assert!(hyprland_tree(br#"[{"address":"0xabc; exec bad","pid":30}]"#).is_err());
    let tree = x11_tree(b"0x001234ab  2 30 machine Title with spaces\n").unwrap();
    assert_eq!(tree.nodes[0].id, 0x1234ab);
    assert_eq!(tree.nodes[0].pid, Some(30));
    assert!(x11_tree(b"malformed output").is_err());
}

#[test]
fn hyprland_supports_modern_and_classic_dispatch_and_verifies_focus() {
    let (dir, identity) = fixture();
    for modern in [true, false] {
        let mut dispatches = Vec::new();
        focus_hyprland_with(dir.path(), identity, |args| {
            Ok(match args {
                ["-j", "clients"] => br#"[{"address":"0xabc","pid":30,"mapped":true}]"#.to_vec(),
                ["-j", "activewindow"] => br#"{"address":"0xabc","pid":30}"#.to_vec(),
                ["dispatch", expression] => {
                    dispatches.push(expression.to_string());
                    if !modern {
                        bail!("unknown dispatcher");
                    }
                    b"ok\n".to_vec()
                }
                ["dispatch", "focuswindow", address] => {
                    dispatches.push(address.to_string());
                    b"ok\n".to_vec()
                }
                _ => panic!("unexpected command"),
            })
        })
        .unwrap();
        assert_eq!(
            dispatches[0],
            "hl.dsp.focus({ window = \"address:0xabc\" })"
        );
        assert_eq!(dispatches.len(), if modern { 1 } else { 2 });
        if !modern {
            assert_eq!(dispatches[1], "address:0xabc");
        }
    }
    let result = focus_hyprland_with(dir.path(), identity, |args| {
        Ok(match args {
            ["-j", "clients"] => br#"[{"address":"0xabc","pid":30}]"#.to_vec(),
            ["-j", "activewindow"] => br#"{"address":"0xdef","pid":30}"#.to_vec(),
            _ => b"ok".to_vec(),
        })
    });
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("did not receive focus")
    );
}

#[test]
fn desktop_detection_selects_wayland_backends_before_xwayland() {
    assert_eq!(
        Backend::detect(Some("socket".into()), "sway", true, false, true).unwrap(),
        Backend::Sway
    );
    assert_eq!(
        Backend::detect(None, "ubuntu:GNOME", true, false, true).unwrap(),
        Backend::Gnome
    );
    assert_eq!(
        Backend::detect(None, "GNOME", false, false, true).unwrap(),
        Backend::X11
    );
    assert_eq!(
        Backend::detect(None, "Hyprland", true, true, true).unwrap(),
        Backend::Hyprland
    );
    assert!(
        Backend::detect(None, "Weston", true, false, true)
            .unwrap_err()
            .to_string()
            .contains("Supported backends")
    );
    assert!(
        Backend::detect(None, "sway", true, false, true)
            .unwrap_err()
            .to_string()
            .contains("SWAYSOCK")
    );
}
