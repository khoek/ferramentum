use std::collections::HashSet;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result, bail};
use clap::Args;
use tempfile::NamedTempFile;
use walkdir::WalkDir;

#[derive(Debug, Args)]
pub(crate) struct LlmGetArgs {
    /// Files and/or directories to include. Files are included regardless of extension.
    #[arg(required = true)]
    paths: Vec<PathBuf>,

    /// File extension to include when traversing directories (repeatable). Default: .py .cpp .rs
    #[arg(long = "ext", action = clap::ArgAction::Append)]
    exts: Vec<String>,

    /// Output file path; '-' prints to stdout. Default: copy to clipboard.
    #[arg(long = "out")]
    out: Option<String>,

    /// Slim output: do not include AGENTS.md and DESIGN.md when present.
    #[arg(long = "slim")]
    slim: bool,

    /// Base directory used to render file names (default: CWD).
    #[arg(long = "relative-to")]
    relative_to: Option<PathBuf>,
}

pub(crate) fn run(args: LlmGetArgs) -> Result<()> {
    let relative_to_input = match args.relative_to {
        Some(p) => p,
        None => std::env::current_dir().context("Failed to read current working directory")?,
    };
    let relative_to_abs = absolute_path(&relative_to_input)?;
    if !relative_to_abs.exists() || !relative_to_abs.is_dir() {
        bail!(
            "--relative-to must be an existing directory: {}",
            relative_to_abs.display()
        );
    }
    let rel_base = relative_to_abs
        .canonicalize()
        .context("Failed to resolve --relative-to")?;

    let exts = normalize_exts(&args.exts);

    let mut files = gather_files(&args.paths, &exts)?;
    if !args.slim {
        files = prepend_special_files(files, &rel_base)?;
    }
    if files.is_empty() {
        bail!("No files matched the criteria.");
    }

    let listing = build_listing(&files, &rel_base)?;

    match args.out.as_deref() {
        None => {
            copy_to_clipboard(&listing)?;
            eprintln!(
                "Copied listing ({} chars) for {} file(s) to clipboard.",
                listing.chars().count(),
                files.len()
            );
        }
        Some("-") => {
            print!("{listing}");
        }
        Some(path) => {
            let out_path = PathBuf::from(path);
            if let Some(parent) = out_path.parent().filter(|p| !p.as_os_str().is_empty()) {
                fs::create_dir_all(parent).with_context(|| {
                    format!("Failed to create output directory: {}", parent.display())
                })?;
            }
            fs::write(&out_path, &listing)
                .with_context(|| format!("Failed to write output file: {}", out_path.display()))?;
            eprintln!(
                "Wrote listing ({} chars) for {} file(s) to {}.",
                listing.chars().count(),
                files.len(),
                out_path.display()
            );
        }
    }

    Ok(())
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(std::env::current_dir()
            .context("Failed to read current working directory")?
            .join(path))
    }
}

fn normalize_exts(user_exts: &[String]) -> HashSet<String> {
    let mut exts: HashSet<String> = HashSet::new();
    for ext in [".py", ".cpp", ".rs"] {
        exts.insert(ext.to_owned());
    }
    for raw in user_exts {
        let lower = raw.to_lowercase();
        if lower.starts_with('.') {
            exts.insert(lower);
        } else {
            exts.insert(format!(".{lower}"));
        }
    }
    exts
}

fn gather_files(paths: &[PathBuf], exts: &HashSet<String>) -> Result<Vec<PathBuf>> {
    let mut files: Vec<PathBuf> = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();

    for raw in paths {
        if !raw.exists() {
            bail!("Path does not exist: {}", raw.display());
        }

        if raw.is_dir() {
            let mut dir_files: Vec<PathBuf> = WalkDir::new(raw)
                .min_depth(1)
                .follow_links(true)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter_map(|entry| {
                    let metadata = entry.metadata().ok()?;
                    if !metadata.is_file() {
                        return None;
                    }
                    let ext = entry.path().extension()?.to_str()?;
                    let suffix = format!(".{ext}");
                    if exts.contains(&suffix) {
                        Some(entry.path().to_path_buf())
                    } else {
                        None
                    }
                })
                .collect();

            dir_files.sort_by_key(|p| p.to_string_lossy().into_owned());

            for fp in dir_files {
                if seen.insert(fp.clone()) {
                    files.push(fp);
                }
            }
        } else if seen.insert(raw.clone()) {
            files.push(raw.clone());
        }
    }

    Ok(files)
}

fn prepend_special_files(mut files: Vec<PathBuf>, rel_base: &Path) -> Result<Vec<PathBuf>> {
    let mut special: Vec<PathBuf> = Vec::new();
    for name in ["AGENTS.md", "DESIGN.md"] {
        let cand = rel_base.join(name);
        if cand.is_file() {
            special.push(
                cand.canonicalize().with_context(|| {
                    format!("Failed to resolve special file: {}", cand.display())
                })?,
            );
        }
    }

    if special.is_empty() {
        return Ok(files);
    }

    let mut seen: HashSet<PathBuf> = HashSet::new();
    let mut ordered: Vec<PathBuf> = Vec::new();

    for fp in special {
        if seen.insert(fp.clone()) {
            ordered.push(fp);
        }
    }

    for fp in &mut files {
        let resolved = fp.canonicalize().with_context(|| {
            format!("Failed to resolve input file for listing: {}", fp.display())
        })?;
        if seen.insert(resolved.clone()) {
            ordered.push(resolved);
        }
    }

    Ok(ordered)
}

fn build_listing(paths: &[PathBuf], rel_base: &Path) -> Result<String> {
    let mut out = String::new();
    out.push_str("***** BEGIN FILE LISTING *****\n\n");

    for fp in paths {
        let resolved = fp
            .canonicalize()
            .with_context(|| format!("Failed to resolve path for listing: {}", fp.display()))?;
        let rel = pathdiff::diff_paths(&resolved, rel_base).unwrap_or(resolved);
        let rel_posix = to_posix_path(&rel);

        out.push_str(&rel_posix);
        out.push_str(":\n\n");

        let content = fs::read_to_string(fp)
            .with_context(|| format!("Failed to read file as UTF-8: {}", fp.display()))?;
        out.push_str(&content);
        out.push_str("\n\n\n\n\n");
    }

    out.push_str("***** END FILE LISTING *****");
    Ok(out)
}

fn to_posix_path(path: &Path) -> String {
    path.to_string_lossy().replace('\\', "/")
}

fn copy_to_clipboard(text: &str) -> Result<()> {
    let mut tmp = NamedTempFile::new().context("Failed to create temporary file")?;
    tmp.write_all(text.as_bytes())
        .context("Failed to write listing to temporary file")?;
    tmp.flush()
        .context("Failed to flush listing to temporary file")?;

    let stdin = tmp
        .reopen()
        .context("Failed to reopen temporary file for reading")?;

    if which::which("wl-copy").is_ok() {
        return spawn_clipboard_command("wl-copy", &[], stdin);
    }
    if which::which("xclip").is_ok() {
        return spawn_clipboard_command("xclip", &["-selection", "clipboard", "-in"], stdin);
    }
    if which::which("xsel").is_ok() {
        return spawn_clipboard_command("xsel", &["--clipboard", "--input"], stdin);
    }

    bail!("No clipboard utility found. Install one of: wl-clipboard, xclip, xsel.");
}

fn spawn_clipboard_command(program: &str, args: &[&str], stdin: fs::File) -> Result<()> {
    let mut command = Command::new(program);
    command
        .args(args)
        .stdin(Stdio::from(stdin))
        .stdout(Stdio::null())
        .stderr(Stdio::piped());
    command.start_new_session();

    let mut child = command
        .spawn()
        .with_context(|| format!("Failed to spawn {program}"))?;

    match child
        .try_wait()
        .context("Failed to poll clipboard process")?
    {
        None => Ok(()),
        Some(status) if status.success() => Ok(()),
        Some(_) => {
            let mut err = String::new();
            if let Some(mut stderr) = child.stderr.take() {
                let _ = stderr.read_to_string(&mut err);
            }
            bail!("{program} failed: {err}");
        }
    }
}

trait CommandExtStartNewSession {
    fn start_new_session(&mut self) -> &mut Self;
}

impl CommandExtStartNewSession for Command {
    fn start_new_session(&mut self) -> &mut Self {
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;

            unsafe {
                self.pre_exec(|| {
                    if libc::setsid() == -1 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(())
                });
            }
        }
        self
    }
}
