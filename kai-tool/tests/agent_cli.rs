#![cfg(unix)]

use std::env;
use std::fs;
use std::os::unix::fs::PermissionsExt;

use assert_cmd::Command;
use tempfile::tempdir;

#[test]
fn supervised_fast_agent_preserves_invocation_directory() {
    let root = tempdir().unwrap();
    let bin_dir = root.path().join("bin");
    let launch_dir = root.path().join("workspace");
    let cwd_path = root.path().join("cwd");
    let args_path = root.path().join("args");
    fs::create_dir_all(&bin_dir).unwrap();
    fs::create_dir(&launch_dir).unwrap();

    let fake_codex = bin_dir.join("codex");
    fs::write(
        &fake_codex,
        concat!(
            "#!/bin/sh\n",
            "set -eu\n",
            "if [ \"${1-}\" = --version ]; then\n",
            "  printf 'codex-cli 0.147.0+k\\n'\n",
            "  exit 0\n",
            "fi\n",
            "printf '%s\\n' \"$PWD\" > \"$KAI_TEST_CWD\"\n",
            "printf '%s\\n' \"$@\" > \"$KAI_TEST_ARGS\"\n",
        ),
    )
    .unwrap();
    fs::set_permissions(&fake_codex, fs::Permissions::from_mode(0o700)).unwrap();
    let path = env::join_paths(
        std::iter::once(bin_dir).chain(env::split_paths(&env::var_os("PATH").unwrap())),
    )
    .unwrap();

    Command::cargo_bin("kai")
        .unwrap()
        .current_dir(&launch_dir)
        .env("PATH", path)
        .env("KAI_TEST_CWD", &cwd_path)
        .env("KAI_TEST_ARGS", &args_path)
        .args(["--quota-auto-restart", "yes", "a", "--fast"])
        .assert()
        .success();

    assert_eq!(
        fs::read_to_string(cwd_path).unwrap(),
        format!("{}\n", launch_dir.display())
    );
    assert_eq!(
        fs::read_to_string(args_path).unwrap(),
        concat!(
            "--dangerously-bypass-approvals-and-sandbox\n",
            "-c\n",
            "service_tier=fast\n",
            "--exit-on-quota-exceeded\n",
        )
    );
}
