#![cfg(target_os = "linux")]

use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::{Duration, Instant},
};
use tempfile::tempdir;

const KAI: &str = env!("CARGO_BIN_EXE_kai");
const TIMEOUT: Duration = Duration::from_secs(15);

// All fixture children have their own process group, including shell sleep children.
struct Running(Child);
impl Running {
    fn stop(&mut self) {
        if self.0.try_wait().unwrap().is_none() {
            // SAFETY: this group belongs to the unreaped test child created with process_group(0).
            unsafe {
                libc::kill(-(self.0.id() as i32), libc::SIGKILL);
            }
            self.0.wait().unwrap();
        }
    }
}
impl Drop for Running {
    fn drop(&mut self) {
        self.stop();
    }
}

fn lifecycle(path: &Path, kind: &str, turn: &str) {
    let mut file = fs::OpenOptions::new().append(true).open(path).unwrap();
    writeln!(
        file,
        "{}",
        json!({"timestamp":"2026-09-30T12:00:00Z", "type":"event_msg",
        "payload":{"type":kind, "turn_id":turn}})
    )
    .unwrap();
}

fn tokens(path: &Path, thread: &str, total: u64) {
    writeln!(
        fs::OpenOptions::new().append(true).open(path).unwrap(),
        "{}",
        json!({"type":"token_usage_record", "payload":{"thread_id":thread,
            "thread_token_usage":{"input_tokens":total-500, "output_tokens":500,
                "total_tokens":total, "cached_input_tokens":600, "reasoning_output_tokens":100}}})
    )
    .unwrap();
}

#[test]
fn watches_existing_windows_through_completion_new_work_and_exit() {
    let root = tempdir().unwrap();
    let sessions = root.path().join("sessions/2026/09/30");
    fs::create_dir_all(&sessions).unwrap();
    let log = sessions.join("rollout-fixture.jsonl");
    fs::write(
        root.path().join("session_index.jsonl"),
        "{\"id\":\"fixture-thread\",\"thread_name\":\"Test dashboard\"}\n",
    )
    .unwrap();
    fs::write(
        &log,
        format!(
            "{}\n",
            json!({"type":"session_meta", "payload":{
                "id":"fixture-thread", "source":"cli", "cwd":root.path()
            }})
        ),
    )
    .unwrap();
    lifecycle(&log, "task_started", "first");
    let child_log = sessions.join("rollout-child.jsonl");
    fs::write(&child_log, format!("{}\n", json!({"type":"session_meta", "payload":{
        "id":"fixture-child", "source":{"subagent":{"thread_spawn":{"parent_thread_id":"fixture-thread"}}}
    }}))).unwrap();
    lifecycle(&child_log, "task_started", "child-turn");
    tokens(&log, "fixture-thread", 1500);
    tokens(&child_log, "fixture-child", 900000);
    let codex = root.path().join("codex");
    fs::write(
        &codex,
        r#"#!/bin/sh
if [ "$1" = --version ]; then
    echo 'codex-cli 0.158.0'
    exit 0
fi
exec 3< "$KAI_STATUS_FIXTURE_LOG"
exec 4< "$KAI_STATUS_FIXTURE_CHILD_LOG"
: > "$KAI_STATUS_FIXTURE_READY"
while :; do /bin/sleep 1; done
"#,
    )
    .unwrap();
    fs::set_permissions(&codex, fs::Permissions::from_mode(0o700)).unwrap();
    let ready = root.path().join("ready");
    let mut launcher = Running(
        Command::new(KAI)
            .env("PATH", root.path())
            .env("KAI_STATUS_FIXTURE_LOG", &log)
            .env("KAI_STATUS_FIXTURE_CHILD_LOG", &child_log)
            .env("KAI_STATUS_FIXTURE_READY", &ready)
            .current_dir(root.path())
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap(),
    );
    let deadline = Instant::now() + TIMEOUT;
    while !ready.exists() {
        assert!(Instant::now() < deadline, "fixture Codex did not start");
        assert!(
            launcher.0.try_wait().unwrap().is_none(),
            "fixture Kai exited early"
        );
        thread::sleep(Duration::from_millis(20));
    }
    let pid = launcher.0.id();
    let mut watcher = Running(
        Command::new(KAI)
            .args(["status", "--watch", "--json", "--interval", "1"])
            .env("PATH", "")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .unwrap(),
    );
    let reader = BufReader::new(watcher.0.stdout.take().unwrap());
    let (sender, receiver) = mpsc::channel();
    let reading = thread::spawn(move || {
        for line in reader.lines() {
            let Ok(line) = line else {
                break;
            };
            let snapshot: Value = serde_json::from_str(&line).unwrap();
            if sender.send(snapshot).is_err() {
                break;
            }
        }
    });
    let wait_for = |state: &str, running: u64, total: u64| -> Value {
        let deadline = Instant::now() + TIMEOUT;
        loop {
            let snapshot = receiver
                .recv_timeout(deadline.saturating_duration_since(Instant::now()))
                .unwrap();
            assert_eq!(snapshot["version"], 1);
            assert!(
                !snapshot["windows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|row| row["pid"] == watcher.0.id())
            );
            if let Some(row) = snapshot["windows"].as_array().unwrap().iter().find(|row| {
                row["pid"] == pid
                    && row["state"] == state
                    && row["agents"]["running"] == running
                    && row["token_usage"]["total_tokens"] == total
            }) {
                return row.clone();
            }
        }
    };
    let working = wait_for("working", 1, 1500);
    assert_eq!(working["thread_id"], "fixture-thread");
    assert_eq!(working["thread_name"], "Test dashboard");
    assert!(working["run_time_ms"].is_u64());
    lifecycle(&log, "task_complete", "first");
    let done = wait_for("ready", 1, 1500);
    assert!(done["last_finished_at"].is_i64());
    lifecycle(&child_log, "task_complete", "child-turn");
    let children_done = wait_for("ready", 0, 1500);
    assert_eq!(children_done["agents"]["ready"], 1);
    fs::write(
        root.path().join("session_index.jsonl"),
        "{\"id\":\"fixture-thread\",\"thread_name\":\"Renamed dashboard\"}\n",
    )
    .unwrap();
    lifecycle(&log, "task_started", "second");
    tokens(&log, "fixture-thread", 1750);
    writeln!(
        fs::OpenOptions::new().append(true).open(&log).unwrap(),
        "{}",
        json!({"type":"compacted", "payload":{"message":"x".repeat(1024 * 1024 + 100)}})
    )
    .unwrap();
    let working = wait_for("working", 0, 1750);
    assert_eq!(working["last_finished_at"], done["last_finished_at"]);
    assert_eq!(working["thread_name"], "Renamed dashboard");
    // An ending immediately followed by exit must not leave a stale/history row.
    lifecycle(&log, "task_complete", "second");
    launcher.stop();
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let snapshot = receiver
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap();
        let rows = snapshot["windows"].as_array().unwrap();
        assert!(
            rows.iter()
                .all(|row| row["exited_at"].is_null() && row["state"] != "exited")
        );
        if !rows.iter().any(|row| row["pid"] == pid) {
            break;
        }
    }
    watcher.stop();
    reading.join().unwrap();
}

#[test]
fn status_runs_without_codex_and_rejects_invalid_options() {
    let output = Command::new(KAI)
        .args(["status", "--json"])
        .env("PATH", "")
        .output()
        .unwrap();
    assert!(output.status.success());
    let snapshot: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(snapshot["version"], 1);
    for args in [
        vec!["status", "--interval", "0"],
        vec!["status", "--fast"],
        vec!["status", "--watch"],
    ] {
        let output = Command::new(KAI)
            .args(args)
            .stdin(Stdio::null())
            .output()
            .unwrap();
        assert!(!output.status.success());
    }
}

#[test]
fn json_watch_exits_cleanly_when_its_pipe_closes() {
    let mut watcher = Running(
        Command::new(KAI)
            .args(["status", "--watch", "--json", "--interval", "1"])
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .process_group(0)
            .spawn()
            .unwrap(),
    );
    // Closing the read end immediately must make even the first JSON write a clean exit.
    drop(watcher.0.stdout.take());
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if let Some(status) = watcher.0.try_wait().unwrap() {
            assert!(status.success());
            let mut stderr = String::new();
            watcher
                .0
                .stderr
                .take()
                .unwrap()
                .read_to_string(&mut stderr)
                .unwrap();
            assert!(stderr.is_empty());
            break;
        }
        assert!(
            Instant::now() < deadline,
            "watcher did not handle the closed pipe"
        );
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn terminal_watch_restores_the_terminal_after_escape_and_control_c() {
    for quit in [27, 3] {
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 30,
                cols: 120,
                ..PtySize::default()
            })
            .unwrap();
        let before = pair.master.get_termios().unwrap();
        let mut command = CommandBuilder::new(KAI);
        command.args(["status", "--watch", "--interval", "1"]);
        let mut child = pair.slave.spawn_command(command).unwrap();
        let mut writer = pair.master.take_writer().unwrap();
        let mut reader = pair.master.try_clone_reader().unwrap();
        let (sender, receiver) = mpsc::channel();
        let reading = thread::spawn(move || {
            let mut bytes = [0; 4096];
            while let Ok(count) = reader.read(&mut bytes) {
                if count == 0 || sender.send(bytes[..count].to_vec()).is_err() {
                    break;
                }
            }
        });
        let deadline = Instant::now() + TIMEOUT;
        let mut output = Vec::new();
        while !String::from_utf8_lossy(&output).contains("Kai windows") {
            match receiver.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                Ok(bytes) => output.extend(bytes),
                Err(error) => {
                    child.kill().unwrap();
                    child.wait().unwrap();
                    panic!("dashboard did not render: {error}");
                }
            }
        }
        writer.write_all(&[quit]).unwrap();
        writer.flush().unwrap();
        let deadline = Instant::now() + TIMEOUT;
        loop {
            if let Some(status) = child.try_wait().unwrap() {
                assert!(status.success());
                break;
            }
            if Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("dashboard did not quit");
            }
            thread::sleep(Duration::from_millis(20));
        }
        let after = pair.master.get_termios().unwrap();
        assert_eq!(before.local_flags, after.local_flags);
        assert_eq!(before.input_flags, after.input_flags);
        assert_eq!(before.output_flags, after.output_flags);
        drop(pair.slave);
        reading.join().unwrap();
        output.extend(receiver.try_iter().flatten());
        assert!(
            output
                .windows(b"\x1b[?1049l".len())
                .any(|part| part == b"\x1b[?1049l")
        );
        assert!(
            output
                .windows(b"\x1b[?25h".len())
                .any(|part| part == b"\x1b[?25h")
        );
    }
}
