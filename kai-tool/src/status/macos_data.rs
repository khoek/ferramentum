//! Parsers shared by macOS discovery and platform-independent fixture tests.
use super::process::is_rollout;
use anyhow::{Context, Result, ensure};
use std::{
    collections::{BTreeSet, HashMap},
    ffi::OsString,
    os::unix::ffi::OsStringExt,
    path::PathBuf,
};

pub(super) struct Arguments {
    pub command: Vec<u8>,
    pub environment: Vec<u8>,
}

pub(super) fn arguments(bytes: &[u8]) -> Result<Arguments> {
    let count = i32::from_ne_bytes(
        bytes
            .get(..4)
            .context("Missing argument count")?
            .try_into()?,
    );
    ensure!((1..=4096).contains(&count), "Invalid argument count");
    let mut offset = 4;
    let end = bytes[offset..]
        .iter()
        .position(|byte| *byte == 0)
        .context("Missing executable path")?;
    offset += end + 1;
    while bytes.get(offset) == Some(&0) {
        offset += 1;
    }
    let start = offset;
    for _ in 0..count {
        let end = bytes
            .get(offset..)
            .context("Missing arguments")?
            .iter()
            .position(|byte| *byte == 0)
            .context("Incomplete argument")?;
        offset += end + 1;
    }
    Ok(Arguments {
        command: bytes[start..offset].to_vec(),
        environment: bytes[offset..].to_vec(),
    })
}

#[derive(Debug, Default)]
pub(super) struct Files {
    pub cwd: Option<PathBuf>,
    pub tty: Option<String>,
    pub transcripts: BTreeSet<PathBuf>,
}

pub(super) fn file_result(output: super::command::Output) -> Result<HashMap<u32, Files>> {
    ensure!(
        matches!(output.status.code(), Some(0 | 1)),
        "lsof could not inspect session files ({})",
        output.status
    );
    // Exit 1 can mean that just one requested process disappeared. Preserve every
    // returned process and let discovery identify missing PIDs individually.
    open_files(&output.stdout)
}

pub(super) fn take_files(files: &mut HashMap<u32, Files>, pid: u32) -> Result<Files> {
    files.remove(&pid).with_context(|| format!("lsof returned no file records for process {pid}; it may have exited or access was denied"))
}

pub(super) fn open_files(bytes: &[u8]) -> Result<HashMap<u32, Files>> {
    let mut files = HashMap::new();
    let mut pid = None;
    let mut fd = &b""[..];
    for field in bytes.split(|byte| *byte == 0) {
        let field = field.strip_prefix(b"\n").unwrap_or(field);
        match field.first() {
            Some(b'p') => {
                pid = Some(std::str::from_utf8(&field[1..])?.parse::<u32>()?);
                files.entry(pid.unwrap()).or_default();
                fd = b"";
            }
            Some(b'f') => fd = &field[1..],
            Some(b'n') => {
                let Some(pid) = pid else { continue };
                let entry: &mut Files = files.entry(pid).or_default();
                let path = PathBuf::from(OsString::from_vec(field[1..].to_vec()));
                if fd == b"cwd" {
                    entry.cwd = Some(path.clone());
                }
                if fd == b"0"
                    && path
                        .to_str()
                        .is_some_and(|path| path.starts_with("/dev/tty"))
                {
                    entry.tty = path.to_str().map(str::to_owned);
                }
                if is_rollout(&path) {
                    entry.transcripts.insert(path);
                }
            }
            _ => (),
        }
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::status::process::is_launch;
    #[test]
    fn parses_kernel_arguments_without_confusing_environment_for_commands() {
        let mut bytes = 2_i32.to_ne_bytes().to_vec();
        bytes.extend(b"/bin/kai\0\0kai\0resume\0TERM_PROGRAM=Apple_Terminal\0");
        let parsed = arguments(&bytes).unwrap();
        assert!(is_launch(&parsed.command));
        assert_eq!(parsed.environment, b"TERM_PROGRAM=Apple_Terminal\0");
        assert!(arguments(&bytes[..8]).is_err());
        bytes[..4].copy_from_slice(&i32::MAX.to_ne_bytes());
        assert!(arguments(&bytes).is_err());
    }
    #[test]
    fn parses_nul_delimited_lsof_records_and_keeps_files_with_spaces_or_newlines() {
        let records = b"p10\0\nf0\0n/dev/ttys001\0\nf cwd\0n/ignored\0\nf cwd\0\np11\0\nf3\0n/tmp/my dir/sessions/rollout-one\nline.jsonl\0\n";
        let files = open_files(records).unwrap();
        assert_eq!(files[&10].tty.as_deref(), Some("/dev/ttys001"));
        assert!(files[&10].transcripts.is_empty());
        assert_eq!(files[&11].transcripts.len(), 1);
        let files = open_files(b"p10\0\nfcwd\0n/dir with spaces\0\n").unwrap();
        assert_eq!(files[&10].cwd, Some(PathBuf::from("/dir with spaces")));
    }

    #[test]
    fn partial_lsof_failure_preserves_other_processes_and_only_marks_missing_pids() {
        use std::os::unix::process::ExitStatusExt;
        let mut files = file_result(super::super::command::Output {
            stdout: b"p10\0\nfcwd\0n/valid\0\np11\0\nf3\0n/tmp/rollout-main.jsonl\0\np12\0\n"
                .to_vec(),
            status: std::process::ExitStatus::from_raw(1 << 8),
        })
        .unwrap();
        assert!(
            take_files(&mut files, 99)
                .unwrap_err()
                .to_string()
                .contains("99")
        );
        assert_eq!(
            take_files(&mut files, 10).unwrap().cwd,
            Some(PathBuf::from("/valid"))
        );
        assert_eq!(take_files(&mut files, 11).unwrap().transcripts.len(), 1);
        assert!(take_files(&mut files, 12).unwrap().transcripts.is_empty());
        assert!(
            file_result(super::super::command::Output {
                stdout: Vec::new(),
                status: std::process::ExitStatus::from_raw(libc::SIGKILL),
            })
            .is_err()
        );
    }
}
