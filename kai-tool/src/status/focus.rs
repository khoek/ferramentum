//! Beta: explicit desktop activation. Keep platform limitations out of normal picker chrome.
#[cfg(target_os = "linux")]
pub(super) use super::focus_linux::focus;
#[cfg(target_os = "linux")]
pub(super) use super::focus_linux::focused;
#[cfg(target_os = "macos")]
pub(super) use super::focus_macos::focus;
#[cfg(target_os = "macos")]
pub(super) use super::focus_macos::focused;
