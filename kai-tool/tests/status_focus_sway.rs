#![cfg(target_os = "linux")]
//! Optional end-to-end test, isolated from the user's desktop. Requires sway, swaymsg, foot.
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use serde_json::{Value, json};
use std::{
    fs,
    io::{Read, Write},
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

const KAI: &str = env!("CARGO_BIN_EXE_kai");

struct Running(Child);
impl Drop for Running {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            // SAFETY: our unreaped child owns this private process group.
            unsafe {
                libc::kill(-(self.0.id() as i32), libc::SIGKILL);
            }
            let _ = self.0.wait();
        }
    }
}

struct Watcher(Box<dyn portable_pty::Child + Send + Sync>);
impl Drop for Watcher {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}

#[track_caller]
fn wait(mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "headless desktop did not reach the expected state"
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn socket(runtime: &Path, prefix: &str, suffix: &str) -> Option<PathBuf> {
    fs::read_dir(runtime)
        .unwrap()
        .flatten()
        .map(|entry| entry.path())
        .find(|path| {
            let name = path.file_name().unwrap().to_string_lossy();
            name.starts_with(prefix) && name.ends_with(suffix) && !name.ends_with(".lock")
        })
}

fn tree(socket: &Path) -> Value {
    let output = Command::new("swaymsg")
        .arg("-s")
        .arg(socket)
        .args(["-r", "-t", "get_tree"])
        .output()
        .unwrap();
    assert!(output.status.success());
    serde_json::from_slice(&output.stdout).unwrap()
}

fn focused(tree: &Value, pid: u32) -> bool {
    if tree["pid"] == pid && tree["focused"] == true {
        return true;
    }
    ["nodes", "floating_nodes"].iter().any(|key| {
        tree[*key]
            .as_array()
            .is_some_and(|nodes| nodes.iter().any(|node| focused(node, pid)))
    })
}

#[test]
#[ignore = "requires headless Sway and Foot; run explicitly on Linux"]
fn enter_focuses_selected_window_without_leaving_the_dashboard() {
    exercise_focus(false, false);
}

#[test]
#[ignore = "requires headless Sway and Foot; run explicitly on Linux"]
fn slow_focus_keeps_keys_responsive_and_escape_or_quit_reaps_helpers() {
    exercise_focus(true, false);
}

#[test]
#[ignore = "requires headless Sway and Foot; run explicitly on Linux"]
fn new_completions_are_unread_until_external_focus_or_enter() {
    exercise_focus(false, true);
}

fn rendered(parser: &mut vt100::Parser, receiver: &std::sync::mpsc::Receiver<Vec<u8>>) -> String {
    for chunk in receiver.try_iter() {
        parser.process(&chunk);
    }
    parser.screen().contents()
}

fn exercise_focus(slow: bool, attention: bool) {
    let root = tempfile::tempdir().unwrap();
    let runtime = root.path().join("runtime");
    fs::create_dir(&runtime).unwrap();
    fs::set_permissions(&runtime, fs::Permissions::from_mode(0o700)).unwrap();
    let config = root.path().join("sway.conf");
    fs::write(&config, "output HEADLESS-1 resolution 800x600\n").unwrap();
    let sway_log = root.path().join("sway.log");
    let mut sway = Running(
        Command::new("sway")
            .arg("-c")
            .arg(&config)
            .env("XDG_RUNTIME_DIR", &runtime)
            .env("WLR_BACKENDS", "headless")
            .env("WLR_RENDERER", "pixman")
            .env("WLR_HEADLESS_OUTPUTS", "1")
            .env_remove("SWAYSOCK")
            .env_remove("WAYLAND_DISPLAY")
            .env_remove("DISPLAY")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(fs::File::create(&sway_log).unwrap())
            .process_group(0)
            .spawn()
            .unwrap(),
    );
    wait(|| {
        assert!(
            sway.0.try_wait().unwrap().is_none(),
            "{}",
            fs::read_to_string(&sway_log).unwrap()
        );
        socket(&runtime, "sway-ipc.", ".sock").is_some()
    });
    let ipc = socket(&runtime, "sway-ipc.", ".sock").unwrap();
    let display = socket(&runtime, "wayland-", "")
        .unwrap()
        .file_name()
        .unwrap()
        .to_owned();

    fs::create_dir(root.path().join("sessions")).unwrap();
    let log = root.path().join("sessions/rollout-focus.jsonl");
    let title = format!(
        "focus-fixture-{}",
        root.path().file_name().unwrap().to_string_lossy()
    );
    fs::write(
        &log,
        format!(
            "{}\n",
            json!({"type":"session_meta","payload":{"id":"focus-fixture","source":"cli"}})
        ),
    )
    .unwrap();
    fs::write(
        root.path().join("session_index.jsonl"),
        format!("{}\n", json!({"id":"focus-fixture","thread_name":title})),
    )
    .unwrap();
    if attention {
        let mut file = fs::OpenOptions::new().append(true).open(&log).unwrap();
        writeln!(
            file,
            "{}",
            json!({"type":"event_msg","payload":{"type":"task_started","turn_id":"first"}})
        )
        .unwrap();
    }
    let codex = root.path().join("codex");
    fs::write(&codex, "#!/bin/sh\nif [ \"$1\" = --version ]; then echo 'codex-cli 0.158.0'; exit; fi\nexec 3< \"$KAI_FOCUS_LOG\"\nwhile :; do /bin/sleep 1; done\n").unwrap();
    fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
    let foot = |args: &[&str]| {
        Running(
            Command::new("foot")
                .args(args)
                .env("XDG_RUNTIME_DIR", &runtime)
                .env("WAYLAND_DISPLAY", &display)
                .env("SWAYSOCK", &ipc)
                .env("PATH", format!("{}:/usr/bin:/bin", root.path().display()))
                .env("KAI_FOCUS_LOG", &log)
                .env("TMUX", "")
                .env("STY", "")
                .env("ZELLIJ", "")
                .env("WEZTERM_PANE", "")
                .env("KITTY_WINDOW_ID", "")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .process_group(0)
                .spawn()
                .unwrap(),
        )
    };
    let first = foot(&[KAI, "--no-auto-restart"]);
    wait(|| focused(&tree(&ipc), first.0.id()));
    let second = foot(&["/bin/sleep", "60"]);
    wait(|| focused(&tree(&ipc), second.0.id()));

    let pair = native_pty_system()
        .openpty(PtySize {
            rows: 30,
            cols: 120,
            ..PtySize::default()
        })
        .unwrap();
    let mut cmd = CommandBuilder::new(KAI);
    cmd.args(["status", "--watch", "--interval", "1"]);
    cmd.env("SWAYSOCK", &ipc);
    cmd.env("XDG_CURRENT_DESKTOP", "sway");
    cmd.env("WAYLAND_DISPLAY", &display);
    cmd.env("XDG_RUNTIME_DIR", &runtime);
    let helper_pid = root.path().join("helper-pid");
    if slow {
        let helper = root.path().join("swaymsg");
        fs::write(
            &helper,
            "#!/bin/sh\nif [ \"$#\" = 3 ]; then exec /usr/bin/swaymsg \"$@\"; fi\necho $$ > \"$KAI_HELPER_PID\"\n/bin/sleep 30\n",
        )
        .unwrap();
        fs::set_permissions(helper, fs::Permissions::from_mode(0o700)).unwrap();
        cmd.env("PATH", root.path());
        cmd.env("KAI_HELPER_PID", &helper_pid);
    }
    let mut watcher = Watcher(pair.slave.spawn_command(cmd).unwrap());
    let mut input = pair.master.take_writer().unwrap();
    let mut output = pair.master.try_clone_reader().unwrap();
    let (send, recv) = std::sync::mpsc::channel();
    let reading = thread::spawn(move || {
        let mut bytes = [0; 4096];
        while let Ok(count) = output.read(&mut bytes) {
            if count == 0 || send.send(bytes[..count].to_vec()).is_err() {
                break;
            }
        }
    });
    let mut screen = Vec::new();
    wait(|| {
        screen.extend(recv.try_iter().flatten());
        String::from_utf8_lossy(&screen).contains(&title)
    });
    input.write_all(title.as_bytes()).unwrap();
    let mut parser = vt100::Parser::new(30, 120, 0);
    parser.process(&screen);
    if attention {
        let finish = |turn| {
            let mut file = fs::OpenOptions::new().append(true).open(&log).unwrap();
            for kind in ["task_started", "task_complete"] {
                writeln!(
                    file,
                    "{}",
                    json!({"type":"event_msg","payload":{"type":kind,"turn_id":turn}})
                )
                .unwrap();
            }
        };
        let activate = |pid| {
            fn id(node: &Value, pid: u32) -> Option<u64> {
                if node["pid"] == pid {
                    return node["id"].as_u64();
                }
                ["nodes", "floating_nodes"].into_iter().find_map(|key| {
                    node[key]
                        .as_array()
                        .and_then(|nodes| nodes.iter().find_map(|node| id(node, pid)))
                })
            }
            let target = id(&tree(&ipc), pid).unwrap();
            assert!(
                Command::new("swaymsg")
                    .arg("-s")
                    .arg(&ipc)
                    .arg(format!("[con_id={target}] focus"))
                    .output()
                    .unwrap()
                    .status
                    .success()
            );
        };
        input.flush().unwrap();
        finish("first");
        wait(|| rendered(&mut parser, &recv).contains(&format!("● {title}")));
        // A desktop shortcut or mouse focus must acknowledge without Enter in Kai.
        activate(first.0.id());
        wait(|| {
            let contents = rendered(&mut parser, &recv);
            contents.contains(&title) && !contents.contains(&format!("● {title}"))
        });
        activate(second.0.id());
        finish("second");
        wait(|| rendered(&mut parser, &recv).contains(&format!("● {title}")));
    }
    input.write_all(b"\r").unwrap();
    input.flush().unwrap();
    if slow {
        let await_helper = || {
            let mut pid = None;
            wait(|| {
                pid = fs::read_to_string(&helper_pid)
                    .ok()
                    .and_then(|value| value.trim().parse::<i32>().ok());
                pid.is_some()
            });
            pid.unwrap()
        };
        let pid = await_helper();
        let began = Instant::now();
        input.write_all(&[5]).unwrap(); // Ctrl-E must work while swaymsg is sleeping.
        input.flush().unwrap();
        wait(|| {
            screen.extend(recv.try_iter().flatten());
            String::from_utf8_lossy(&screen).contains("Thread focus-fixture")
        });
        assert!(
            began.elapsed() < Duration::from_secs(1),
            "details blocked behind focus helper"
        );
        input.write_all(&[27]).unwrap(); // Esc cancels without quitting or clearing search.
        input.flush().unwrap();
        wait(|| {
            // SAFETY: signal 0 only checks whether the cancelled helper is still alive.
            unsafe { libc::kill(pid, 0) == -1 }
        });
        assert!(
            began.elapsed() < Duration::from_secs(1),
            "cancellation waited for helper timeout"
        );
        assert!(watcher.0.try_wait().unwrap().is_none());
        assert!(focused(&tree(&ipc), second.0.id()));
        fs::remove_file(&helper_pid).unwrap();
        // Allow one UI poll to collect the cancelled operation before starting the next.
        thread::sleep(Duration::from_millis(100));
        input.write_all(b"\r").unwrap();
        input.flush().unwrap();
        let pid = await_helper();
        let began = Instant::now();
        input.write_all(&[3]).unwrap();
        input.flush().unwrap();
        wait(|| watcher.0.try_wait().unwrap().is_some());
        assert!(
            began.elapsed() < Duration::from_secs(1),
            "quit waited for helper timeout"
        );
        // SAFETY: the helper should have been reaped before Kai exited.
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert!(focused(&tree(&ipc), second.0.id()));
    } else {
        wait(|| focused(&tree(&ipc), first.0.id()));
        if attention {
            wait(|| !rendered(&mut parser, &recv).contains(&format!("● {title}")));
        }
    }
    if !slow {
        assert!(watcher.0.try_wait().unwrap().is_none());
        input.write_all(&[3]).unwrap();
        input.flush().unwrap();
        wait(|| watcher.0.try_wait().unwrap().is_some());
    }
    drop(pair.slave);
    drop(watcher);
    drop(input);
    drop(pair.master);
    reading.join().unwrap();
}
