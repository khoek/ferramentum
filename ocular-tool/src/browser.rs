use std::fs;
use std::io::{self, Read};
use std::path::Path;
use std::path::PathBuf;
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use headless_chrome::protocol::cdp::Network;
use headless_chrome::{Browser, Tab};
use which::which;

use crate::anyconnect::{self, AuthRequest};
use crate::config;
use crate::error::AppError;

pub struct BrowserConfig {
    pub chrome_path: Option<PathBuf>,
    pub proxy: Option<String>,
    pub timeout: Duration,
    pub cookie_host: Option<String>,
}

const BROWSER_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const BROWSER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const PROCESS_POLL_INTERVAL: Duration = Duration::from_millis(100);

struct BrowserSession {
    browser: Browser,
    tab: Arc<Tab>,
    child: Child,
}

pub fn authenticate_in_browser(
    auth_info: &AuthRequest,
    cfg: &BrowserConfig,
) -> Result<String, AppError> {
    let session = BrowserSession::launch(auth_info, cfg)?;

    tracing::info!(url = %auth_info.login_url, "Opening browser for SSO login");
    session
        .tab
        .navigate_to(&auth_info.login_url)
        .map_err(|e| AppError::Browser(e.to_string()))?;

    let deadline = Instant::now() + cfg.timeout;
    loop {
        let cookies = session
            .tab
            .call_method(Network::GetAllCookies(None))
            .map_err(|e| AppError::Browser(e.to_string()))?
            .cookies;

        if let Some(token) = find_auth_cookie_value(
            cookies,
            &auth_info.token_cookie_name,
            cfg.cookie_host.as_deref(),
        ) {
            return Ok(token);
        }

        if Instant::now() > deadline {
            return Err(AppError::BrowserTimeout);
        }
        thread::sleep(Duration::from_millis(250));
    }
}

impl BrowserSession {
    fn launch(auth_info: &AuthRequest, cfg: &BrowserConfig) -> Result<Self, AppError> {
        let chrome = resolve_chrome_path(cfg.chrome_path.as_ref())?;
        let profile_dir = resolve_profile_dir(auth_info, cfg)?;
        let devtools_port_file = profile_dir.join("DevToolsActivePort");
        remove_stale_devtools_port_file(&devtools_port_file)?;
        let mut child = launch_browser_process(&chrome, &profile_dir, cfg)?;

        let ws_url = match wait_for_devtools_ws_url(&devtools_port_file, &mut child) {
            Ok(ws_url) => ws_url,
            Err(err) => {
                let _ = terminate_browser_process(&mut child);
                return Err(err);
            }
        };

        tracing::info!(
            chrome = %chrome.display(),
            profile = %profile_dir.display(),
            ws_url,
            "Chrome DevTools endpoint is ready"
        );

        let browser = match connect_browser(&ws_url, cfg.timeout + BROWSER_CONNECT_TIMEOUT) {
            Ok(browser) => browser,
            Err(err) => {
                let _ = terminate_browser_process(&mut child);
                return Err(err);
            }
        };

        let tab = match browser.new_tab() {
            Ok(tab) => tab,
            Err(err) => {
                let _ = terminate_browser_process(&mut child);
                return Err(AppError::Browser(err.to_string()));
            }
        };

        Ok(Self {
            browser,
            tab,
            child,
        })
    }
}

impl Drop for BrowserSession {
    fn drop(&mut self) {
        let auth_target = self.tab.get_target_id().clone();
        let mut tabs = self.browser.get_tabs().lock().unwrap().clone();
        if !tabs.iter().any(|tab| tab.get_target_id() == &auth_target) {
            tabs.push(self.tab.clone());
        }

        for tab in tabs {
            match tab.close_with_unload() {
                Ok(true) => {
                    tracing::info!(target = %tab.get_target_id(), "Closed browser tab cleanly")
                }
                Ok(false) => tracing::debug!(
                    target = %tab.get_target_id(),
                    "Browser tab close request was ignored"
                ),
                Err(err) => tracing::debug!(
                    target = %tab.get_target_id(),
                    error = %err,
                    "Failed to close browser tab cleanly"
                ),
            }
        }

        if let Err(err) = terminate_browser_process(&mut self.child) {
            tracing::warn!(error = %err, "Failed to shut down Chrome cleanly");
        }
    }
}

fn remove_stale_devtools_port_file(path: &Path) -> Result<(), AppError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(err.into()),
    }
}

fn launch_browser_process(
    chrome: &Path,
    profile_dir: &Path,
    cfg: &BrowserConfig,
) -> Result<Child, AppError> {
    tracing::info!(
        chrome = %chrome.display(),
        profile = %profile_dir.display(),
        "Launching Chrome for interactive authentication"
    );

    let mut command = Command::new(chrome);
    command
        .arg("--remote-debugging-port=0")
        .arg(format!("--user-data-dir={}", profile_dir.display()))
        .arg("--no-first-run")
        .arg("--no-default-browser-check")
        .arg("about:blank")
        .stdout(Stdio::null())
        .stderr(Stdio::piped());

    if let Some(proxy) = &cfg.proxy {
        command.arg(format!("--proxy-server={proxy}"));
    }

    if running_as_root() {
        command.args(["--no-sandbox", "--disable-setuid-sandbox"]);
    }

    command.spawn().map_err(AppError::from)
}

fn wait_for_devtools_ws_url(
    devtools_port_file: &Path,
    child: &mut Child,
) -> Result<String, AppError> {
    let deadline = Instant::now() + BROWSER_CONNECT_TIMEOUT;
    loop {
        if let Some(ws_url) = read_devtools_ws_url(devtools_port_file)? {
            return Ok(ws_url);
        }

        if let Some(status) = child.try_wait()? {
            let stderr = read_child_stderr(child);
            let detail = render_browser_exit_detail(status, &stderr);
            return Err(AppError::Browser(format!(
                "Chrome exited before opening DevTools: {detail}"
            )));
        }

        if Instant::now() >= deadline {
            return Err(AppError::Browser(format!(
                "timed out waiting for Chrome DevTools at `{}`",
                devtools_port_file.display()
            )));
        }

        thread::sleep(PROCESS_POLL_INTERVAL);
    }
}

fn read_devtools_ws_url(devtools_port_file: &Path) -> Result<Option<String>, AppError> {
    let contents = match fs::read_to_string(devtools_port_file) {
        Ok(contents) => contents,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err.into()),
    };

    let mut lines = contents.lines();
    let port = match lines.next().map(str::trim) {
        Some(port) if !port.is_empty() => port,
        _ => return Ok(None),
    };
    let browser_path = match lines.next().map(str::trim) {
        Some(browser_path) if browser_path.starts_with('/') => browser_path,
        _ => return Ok(None),
    };

    Ok(Some(format!("ws://127.0.0.1:{port}{browser_path}")))
}

fn connect_browser(ws_url: &str, idle_timeout: Duration) -> Result<Browser, AppError> {
    let deadline = Instant::now() + BROWSER_CONNECT_TIMEOUT;
    let mut last_error = None;
    loop {
        match Browser::connect_with_timeout(ws_url.to_string(), idle_timeout) {
            Ok(browser) => return Ok(browser),
            Err(err) => {
                last_error = Some(err.to_string());
                if Instant::now() >= deadline {
                    let detail = last_error
                        .unwrap_or_else(|| "unknown browser connection error".to_string());
                    return Err(AppError::Browser(format!(
                        "failed to connect to Chrome DevTools at `{ws_url}`: {detail}"
                    )));
                }
                thread::sleep(PROCESS_POLL_INTERVAL);
            }
        }
    }
}

fn terminate_browser_process(child: &mut Child) -> io::Result<()> {
    if child.try_wait()?.is_some() {
        return Ok(());
    }

    #[cfg(unix)]
    {
        tracing::info!(pid = child.id(), "Sending SIGTERM to Chrome");
        let rc = unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
        if rc != 0 {
            let err = io::Error::last_os_error();
            if child.try_wait()?.is_none() {
                return Err(err);
            }
            return Ok(());
        }

        if wait_for_process_exit(child, BROWSER_SHUTDOWN_TIMEOUT)?.is_some() {
            return Ok(());
        }

        tracing::warn!(pid = child.id(), "Chrome ignored SIGTERM; forcing shutdown");
    }

    #[cfg(not(unix))]
    tracing::warn!(pid = child.id(), "Falling back to forced Chrome shutdown");

    child.kill()?;
    let _ = child.wait()?;
    Ok(())
}

fn wait_for_process_exit(child: &mut Child, timeout: Duration) -> io::Result<Option<ExitStatus>> {
    let deadline = Instant::now() + timeout;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(Some(status));
        }

        if Instant::now() >= deadline {
            return Ok(None);
        }

        thread::sleep(PROCESS_POLL_INTERVAL);
    }
}

fn render_browser_exit_detail(status: ExitStatus, stderr: &str) -> String {
    if stderr.trim().is_empty() {
        return format!("status {status}");
    }
    format!("status {status}: {}", stderr.trim())
}

fn read_child_stderr(child: &mut Child) -> String {
    let mut stderr = String::new();
    if let Some(mut pipe) = child.stderr.take() {
        let _ = pipe.read_to_string(&mut stderr);
    }
    stderr
}

fn find_auth_cookie_value(
    cookies: Vec<Network::Cookie>,
    cookie_name: &str,
    expected_host: Option<&str>,
) -> Option<String> {
    let candidates: Vec<Network::Cookie> = cookies
        .into_iter()
        .filter(|cookie| cookie.name == cookie_name && !cookie.value.is_empty())
        .collect();
    if candidates.is_empty() {
        return None;
    }

    if let Some(host) = expected_host {
        if let Some(cookie) = candidates
            .iter()
            .find(|cookie| cookie_domain_matches(&cookie.domain, host))
        {
            return Some(cookie.value.clone());
        }
        if candidates.len() > 1 {
            return None;
        }
    }

    candidates.into_iter().next().map(|cookie| cookie.value)
}

fn cookie_domain_matches(cookie_domain: &str, host: &str) -> bool {
    let cookie = cookie_domain
        .trim()
        .trim_start_matches('.')
        .to_ascii_lowercase();
    let host = host.trim().trim_start_matches('.').to_ascii_lowercase();
    if cookie.is_empty() || host.is_empty() {
        return false;
    }
    host == cookie || host.ends_with(&format!(".{cookie}"))
}

fn resolve_profile_dir(auth_info: &AuthRequest, cfg: &BrowserConfig) -> Result<PathBuf, AppError> {
    let root = config::app_dir()?.join("browser-profiles");
    fs::create_dir_all(&root)?;
    tighten_dir_permissions(&root);

    let host = cfg
        .cookie_host
        .clone()
        .or_else(|| anyconnect::host_from_url(&auth_info.login_url))
        .unwrap_or_else(|| "default".to_string());
    let profile = root.join(sanitize_component(&host));
    fs::create_dir_all(&profile)?;
    tighten_dir_permissions(&profile);
    Ok(profile)
}

fn sanitize_component(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for ch in input.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '-' | '_') {
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        return "default".to_string();
    }
    out
}

#[cfg(unix)]
fn tighten_dir_permissions(dir: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let perms = fs::Permissions::from_mode(0o700);
    let _ = fs::set_permissions(dir, perms);
}

#[cfg(not(unix))]
fn tighten_dir_permissions(_dir: &Path) {}

fn resolve_chrome_path(explicit: Option<&PathBuf>) -> Result<PathBuf, AppError> {
    if let Some(p) = explicit {
        return Ok(p.clone());
    }

    for name in [
        "google-chrome",
        "google-chrome-stable",
        "chromium",
        "chromium-browser",
        "brave-browser",
        "microsoft-edge",
        "msedge",
        "chrome",
    ] {
        if let Ok(path) = which(name) {
            return Ok(path);
        }
    }

    Err(AppError::Browser(
        "could not find a Chrome/Chromium executable in PATH (use --chrome-path)".to_string(),
    ))
}

#[cfg(unix)]
fn running_as_root() -> bool {
    (unsafe { libc::geteuid() }) == 0
}

#[cfg(not(unix))]
fn running_as_root() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::read_devtools_ws_url;

    #[test]
    fn reads_devtools_ws_url_from_active_port_file() {
        let test_dir = std::env::temp_dir().join(format!(
            "ocular-browser-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_nanos()
        ));
        fs::create_dir_all(&test_dir).expect("create test dir");
        let port_file = test_dir.join("DevToolsActivePort");
        fs::write(
            &port_file,
            "34537\n/devtools/browser/a45de08f-fe17-4234-8faf-bab61f48daa9\n",
        )
        .expect("write port file");

        let ws_url = read_devtools_ws_url(&port_file)
            .expect("read ws url")
            .expect("ws url must exist");

        assert_eq!(
            ws_url,
            "ws://127.0.0.1:34537/devtools/browser/a45de08f-fe17-4234-8faf-bab61f48daa9"
        );

        fs::remove_dir_all(&test_dir).expect("remove test dir");
    }

    #[test]
    fn returns_none_for_incomplete_active_port_file() {
        let test_dir = std::env::temp_dir().join(format!(
            "ocular-browser-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("time went backwards")
                .as_nanos()
        ));
        fs::create_dir_all(&test_dir).expect("create test dir");
        let port_file = test_dir.join("DevToolsActivePort");
        fs::write(&port_file, "34537\n").expect("write incomplete port file");

        let ws_url = read_devtools_ws_url(&port_file).expect("read ws url");

        assert!(ws_url.is_none());

        fs::remove_dir_all(&test_dir).expect("remove test dir");
    }
}
