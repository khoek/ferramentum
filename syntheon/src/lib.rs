use std::env;
use std::fs::File;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::process::Command;

use flate2::read::GzDecoder;
use tar::Archive;

pub fn manifest_dir() -> PathBuf {
    PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR must be provided"))
}

pub fn vendor_dir() -> PathBuf {
    manifest_dir().join("vendor")
}

pub fn out_dir() -> PathBuf {
    PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR must be provided"))
}

pub fn target_triple() -> String {
    env::var("TARGET").expect("TARGET must be provided by cargo for build scripts")
}

pub fn sanitize_component(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect()
}

pub fn target_root() -> PathBuf {
    if let Ok(dir) = env::var("CARGO_TARGET_DIR") {
        return PathBuf::from(dir);
    }

    let out_dir = out_dir();
    if let Some(target_dir) = out_dir
        .ancestors()
        .find(|p| p.file_name().is_some_and(|name| name == "target"))
    {
        return target_dir.to_path_buf();
    }

    manifest_dir().join("target")
}

pub fn cache_root() -> PathBuf {
    let pkg = sanitize_component(
        &env::var("CARGO_PKG_NAME").expect("CARGO_PKG_NAME must be provided by cargo"),
    );
    let target_triple = sanitize_component(&target_triple());
    target_root()
        .join("build-deps")
        .join(pkg)
        .join(target_triple)
}

pub fn extract_tar_gz(archive_path: &Path, out_dir: &Path) {
    let file = File::open(archive_path)
        .unwrap_or_else(|e| panic!("failed to open {}: {e}", archive_path.display()));
    let gz = GzDecoder::new(file);
    let mut archive = Archive::new(gz);
    archive.unpack(out_dir).unwrap_or_else(|e| {
        panic!(
            "failed to extract archive {} into {}: {e}",
            archive_path.display(),
            out_dir.display()
        )
    });
}

pub fn run(cmd: &mut Command, err: &str) {
    let status = cmd.status().unwrap_or_else(|e| panic!("{err}: {e}"));
    if !status.success() {
        panic!("{err}: status {status}");
    }
}

pub fn parallel_jobs() -> usize {
    env::var("NUM_JOBS")
        .ok()
        .and_then(|s| s.parse::<NonZeroUsize>().ok())
        .map(NonZeroUsize::get)
        .or_else(|| {
            std::thread::available_parallelism()
                .ok()
                .map(NonZeroUsize::get)
        })
        .unwrap_or(1)
}

pub fn apply_parallel(cmd: &mut Command, jobs: usize) {
    if jobs > 1 {
        cmd.arg(format!("-j{jobs}"));
    }
}

pub fn wants_native_cpu_flags() -> bool {
    let Ok(flags) = env::var("CARGO_ENCODED_RUSTFLAGS") else {
        return false;
    };

    let mut last_target_cpu = None;
    let mut saw_dash_c = false;
    for token in flags.split('\u{1f}') {
        if saw_dash_c {
            if let Some(cpu) = token.strip_prefix("target-cpu=") {
                last_target_cpu = Some(cpu);
            }
            saw_dash_c = false;
        }

        if token == "-C" {
            saw_dash_c = true;
            continue;
        }

        if let Some(cpu) = token.strip_prefix("-Ctarget-cpu=") {
            last_target_cpu = Some(cpu);
            continue;
        }
        if let Some(cpu) = token.strip_prefix("target-cpu=") {
            last_target_cpu = Some(cpu);
        }
    }

    last_target_cpu == Some("native")
}

pub fn clang_system_include_dirs() -> Vec<PathBuf> {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("macos") {
        return Vec::new();
    }

    let mut dirs = Vec::new();
    for flag in ["-print-file-name=include", "-print-file-name=include-fixed"] {
        let path = gcc_include_path(flag);
        if path.exists() {
            dirs.push(path);
        }
    }
    if dirs.is_empty() {
        panic!(
            "failed to locate system include directories via gcc; install clang headers or a \
             working gcc toolchain"
        );
    }
    dirs
}

pub fn gcc_include_path(flag: &str) -> PathBuf {
    let output = Command::new("gcc")
        .arg(flag)
        .output()
        .unwrap_or_else(|e| panic!("failed to invoke gcc {flag}: {e}"));
    if !output.status.success() {
        panic!("gcc {flag} exited with {}", output.status);
    }
    let path = String::from_utf8_lossy(&output.stdout);
    let trimmed = path.trim();
    if trimmed.is_empty() {
        panic!("gcc {flag} returned an empty include path");
    }
    PathBuf::from(trimmed)
}

pub fn macos_sdk_root() -> Option<PathBuf> {
    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("macos") {
        return None;
    }

    if let Ok(sdkroot) = env::var("SDKROOT") {
        let sdkroot = PathBuf::from(sdkroot);
        if sdkroot.exists() {
            return Some(sdkroot);
        }
    }
    let output = Command::new("xcrun")
        .args(["--sdk", "macosx", "--show-sdk-path"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sdkroot = String::from_utf8(output.stdout).ok()?;
    let sdkroot = sdkroot.trim();
    if sdkroot.is_empty() {
        return None;
    }
    let sdkroot = PathBuf::from(sdkroot);
    sdkroot.exists().then_some(sdkroot)
}

pub fn clang_system_include_args() -> Vec<String> {
    let mut out = Vec::new();
    for dir in clang_system_include_dirs() {
        out.push("-isystem".to_string());
        out.push(dir.display().to_string());
    }
    out
}

pub fn clang_macos_sysroot_args() -> Vec<String> {
    let Some(sdkroot) = macos_sdk_root() else {
        return Vec::new();
    };
    vec!["-isysroot".to_string(), sdkroot.display().to_string()]
}
