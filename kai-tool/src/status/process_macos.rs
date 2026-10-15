//! Beta macOS discovery: libproc birth tokens and lsof's machine-readable file records.
use super::{
    command,
    macos_data::{self, Arguments},
    process::{Ancestor, ProcessIdentity, Window, is_launch},
    worker::Cancellation,
};
use anyhow::{Result, ensure};
use std::{
    collections::{BTreeSet, HashMap},
    io,
    mem::{MaybeUninit, size_of},
    time::Duration,
};

fn info(pid: u32) -> io::Result<libc::proc_bsdinfo> {
    let mut info = MaybeUninit::<libc::proc_bsdinfo>::uninit();
    // SAFETY: libproc receives an aligned buffer of exactly its documented structure size.
    let count = unsafe {
        libc::proc_pidinfo(
            pid as i32,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size_of::<libc::proc_bsdinfo>() as i32,
        )
    };
    if count != size_of::<libc::proc_bsdinfo>() as i32 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: a full successful libproc response initialized every field.
    Ok(unsafe { info.assume_init() })
}

fn identity(info: &libc::proc_bsdinfo) -> ProcessIdentity {
    ProcessIdentity {
        pid: info.pbi_pid,
        start_ticks: info.pbi_start_tvsec * 1_000_000 + info.pbi_start_tvusec,
    }
}

fn name(info: &libc::proc_bsdinfo) -> String {
    let bytes: Vec<_> = info
        .pbi_comm
        .iter()
        .take_while(|ch| **ch != 0)
        .map(|ch| *ch as u8)
        .collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

pub(super) fn same_process(expected: ProcessIdentity) -> io::Result<bool> {
    match info(expected.pid) {
        Ok(info) => Ok(identity(&info) == expected && info.pbi_status != libc::SZOMB),
        Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(false),
        Err(error) => Err(error),
    }
}

pub(super) fn process_arguments(pid: u32) -> Result<Arguments> {
    let mut mib = [libc::CTL_KERN, libc::KERN_PROCARGS2, pid as i32];
    let mut bytes = vec![0_u8; 1024 * 1024];
    let mut len = bytes.len();
    // SAFETY: both buffers are valid with their declared lengths; sysctl writes at most len bytes.
    let result = unsafe {
        libc::sysctl(
            mib.as_mut_ptr(),
            mib.len() as u32,
            bytes.as_mut_ptr().cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    ensure!(
        result == 0 && len <= bytes.len(),
        "Cannot inspect process arguments"
    );
    bytes.truncate(len);
    macos_data::arguments(&bytes)
}

pub(super) fn ancestry(expected: ProcessIdentity) -> Result<Vec<Ancestor>> {
    let mut chain = Vec::new();
    let mut seen = BTreeSet::new();
    let mut pid = expected.pid;
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    while pid > 1 && chain.len() < 64 {
        ensure!(seen.insert(pid), "Process ancestry contains a cycle");
        let item = info(pid)?;
        if item.pbi_uid != uid {
            break;
        }
        ensure!(
            item.pbi_status != libc::SZOMB && (!chain.is_empty() || identity(&item) == expected),
            "Session process has changed"
        );
        chain.push(Ancestor {
            identity: identity(&item),
            name: name(&item),
        });
        pid = item.pbi_ppid;
    }
    ensure!(
        !chain.is_empty() && chain.len() < 64,
        "Cannot verify process ancestry"
    );
    Ok(chain)
}

pub(super) fn files(
    pids: &[u32],
    cancel: &Cancellation,
) -> Result<HashMap<u32, macos_data::Files>> {
    if pids.is_empty() {
        return Ok(HashMap::new());
    }
    let pids = pids
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let output = command::capture(
        "/usr/sbin/lsof",
        &["-n", "-P", "-F0pfn", "-p", &pids],
        Duration::from_secs(3),
        cancel,
    )?;
    macos_data::file_result(output)
}

pub(super) fn discover(
    observer_pid: u32,
    cancel: &Cancellation,
) -> Result<(Vec<Window>, Vec<String>)> {
    // PROC_UID_ONLY: ask only for this user's PIDs. An explicit cap bounds each refresh.
    let mut pids = vec![0_u32; 65536];
    // SAFETY: the PID buffer has the provided byte capacity; geteuid has no preconditions.
    let count = unsafe {
        libc::proc_listpids(
            4,
            libc::geteuid(),
            pids.as_mut_ptr().cast(),
            (pids.len() * size_of::<u32>()) as i32,
        )
    };
    ensure!(
        count >= 0 && (count as usize) < pids.len() * size_of::<u32>(),
        "Cannot enumerate macOS processes"
    );
    pids.truncate(count as usize / size_of::<u32>());
    let mut processes = HashMap::new();
    let mut warnings = Vec::new();
    // SAFETY: geteuid has no preconditions.
    let uid = unsafe { libc::geteuid() };
    for pid in pids
        .into_iter()
        .filter(|pid| *pid != 0 && *pid != observer_pid)
    {
        cancel.check()?;
        if let Ok(item) = info(pid)
            && item.pbi_uid == uid
        {
            processes.insert(pid, item);
        }
    }
    let mut candidates = Vec::new();
    for item in processes
        .values()
        .filter(|item| name(item) == "kai" && item.pbi_status != libc::SZOMB)
    {
        cancel.check()?;
        let args = match process_arguments(item.pbi_pid) {
            Ok(args) => args,
            Err(_) => {
                warnings.push("Some Kai processes could not be inspected".into());
                continue;
            }
        };
        if !is_launch(&args.command) {
            continue;
        }
        let children: Vec<_> = processes
            .values()
            .filter(|child| {
                child.pbi_ppid == item.pbi_pid
                    && name(child) == "codex"
                    && child.pbi_status != libc::SZOMB
            })
            .collect();
        candidates.push((item, children));
    }
    // One lsof invocation per snapshot, regardless of how many windows are open.
    let pids: Vec<_> = candidates
        .iter()
        .flat_map(|(item, children)| {
            std::iter::once(item.pbi_pid).chain(children.iter().map(|child| child.pbi_pid))
        })
        .collect();
    let mut read_error = None;
    let mut files = files(&pids, cancel).unwrap_or_else(|error| {
        read_error = Some(format!("Cannot inspect open session logs: {error:#}"));
        HashMap::new()
    });
    let mut windows = Vec::new();
    for (item, children) in candidates {
        cancel.check()?;
        let mut warning = read_error.clone();
        let launcher = macos_data::take_files(&mut files, item.pbi_pid).unwrap_or_else(|error| {
            warning.get_or_insert_with(|| error.to_string());
            macos_data::Files::default()
        });
        let mut transcripts = BTreeSet::new();
        for child in children {
            if matches!(same_process(identity(child)), Ok(true)) {
                match macos_data::take_files(&mut files, child.pbi_pid) {
                    Ok(files) => transcripts.extend(files.transcripts),
                    Err(error) => {
                        warning.get_or_insert_with(|| error.to_string());
                    }
                }
            } else {
                warning =
                    Some("Codex restarted during observation; waiting for the next refresh".into());
            }
        }
        match same_process(identity(item)) {
            Ok(false) => continue,
            Err(_) => warning = Some("Cannot verify the window process during this refresh".into()),
            Ok(true) => (),
        }
        windows.push(Window {
            identity: identity(item),
            tty: launcher.tty,
            cwd: launcher.cwd,
            transcripts: transcripts.into_iter().collect(),
            warning,
        });
    }
    Ok((windows, warnings))
}
