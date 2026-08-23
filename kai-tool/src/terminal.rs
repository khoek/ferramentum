use std::io::{self, IsTerminal, Write};

#[cfg(unix)]
use std::mem::MaybeUninit;
#[cfg(unix)]
use std::os::fd::AsRawFd;

/// Whether writes to stderr need an explicit carriage return before their newline.
///
/// A parent terminal application can leave its pty in raw mode (for example, when Kai is
/// launched from another terminal UI).  In that mode the pty no longer performs the usual
/// NL-to-CRLF output translation, so line-oriented status output must supply the carriage return
/// itself.
pub(crate) fn stderr_needs_crlf() -> bool {
    let stderr = io::stderr();
    if !stderr.is_terminal() {
        return false;
    }

    #[cfg(unix)]
    {
        terminal_output_needs_crlf(stderr.as_raw_fd()).unwrap_or(true)
    }

    #[cfg(not(unix))]
    {
        // A terminal handle accepts CRLF directly; unlike Unix, there is no portable termios
        // query here that can distinguish an inherited raw console mode.
        true
    }
}

pub(crate) fn stdin_is_raw() -> io::Result<bool> {
    let stdin = io::stdin();

    #[cfg(unix)]
    {
        terminal_is_raw(stdin.as_raw_fd())
    }

    #[cfg(not(unix))]
    {
        crossterm::terminal::is_raw_mode_enabled()
    }
}

pub(crate) fn write_line(mut writer: impl Write, message: &str, crlf: bool) -> io::Result<()> {
    writer.write_all(message.as_bytes())?;
    writer.write_all(if crlf { b"\r\n" } else { b"\n" })?;
    writer.flush()
}

pub(crate) fn write_stderr_line(message: &str) -> io::Result<()> {
    let stderr = io::stderr();
    let crlf = stderr.is_terminal() && stderr_needs_crlf();
    write_line(stderr.lock(), message, crlf)
}

#[cfg(unix)]
fn terminal_is_raw(fd: std::os::fd::RawFd) -> io::Result<bool> {
    let attributes = terminal_attributes(fd)?;
    Ok(input_is_raw(attributes.c_oflag, attributes.c_lflag))
}

#[cfg(unix)]
fn terminal_output_needs_crlf(fd: std::os::fd::RawFd) -> io::Result<bool> {
    let attributes = terminal_attributes(fd)?;
    Ok(output_needs_crlf(attributes.c_oflag))
}

#[cfg(unix)]
fn input_is_raw(output_flags: libc::tcflag_t, local_flags: libc::tcflag_t) -> bool {
    output_flags & libc::OPOST == 0 && local_flags & libc::ICANON == 0
}

#[cfg(unix)]
fn output_needs_crlf(output_flags: libc::tcflag_t) -> bool {
    output_flags & libc::OPOST == 0 || output_flags & libc::ONLCR == 0
}

#[cfg(unix)]
fn terminal_attributes(fd: std::os::fd::RawFd) -> io::Result<libc::termios> {
    let mut attributes = MaybeUninit::<libc::termios>::uninit();
    if unsafe { libc::tcgetattr(fd, attributes.as_mut_ptr()) } == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { attributes.assume_init() })
}

#[cfg(test)]
mod tests {
    use super::write_line;

    #[test]
    fn line_writer_uses_crlf_for_raw_terminals() {
        let mut output = Vec::new();
        write_line(&mut output, "status", true).unwrap();
        assert_eq!(output, b"status\r\n");
    }

    #[test]
    fn line_writer_keeps_pipe_output_as_lf() {
        let mut output = Vec::new();
        write_line(&mut output, "status", false).unwrap();
        assert_eq!(output, b"status\n");
    }

    #[cfg(unix)]
    #[test]
    fn detects_newline_translation_from_terminal_flags() {
        use super::{input_is_raw, output_needs_crlf};

        assert!(output_needs_crlf(0));
        assert!(output_needs_crlf(libc::OPOST));
        assert!(!output_needs_crlf(libc::OPOST | libc::ONLCR));
        assert!(input_is_raw(0, 0));
        assert!(!input_is_raw(libc::OPOST, libc::ICANON));
    }
}
