//! One in-flight operation per worker. Cancellation also bounds helper lifetimes.
use anyhow::{Context, Result, bail, ensure};
use std::process::{Child, ExitStatus};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc::{self, Receiver, Sender},
};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

#[derive(Clone, Default)]
pub(super) struct Cancellation(Arc<Operation>);

#[derive(Default)]
struct Operation {
    cancelled: AtomicBool,
    // Helpers are tracked independently of the worker so shutdown can stop them
    // even when that worker cannot return from a filesystem operation.
    helper: Mutex<Option<Child>>,
}

impl Cancellation {
    pub(super) fn cancel(&self) {
        self.0.cancelled.store(true, Ordering::Release);
        self.stop_helper();
    }

    pub(super) fn check(&self) -> Result<()> {
        ensure!(!self.cancelled(), "Operation cancelled");
        Ok(())
    }

    pub(super) fn cancelled(&self) -> bool {
        self.0.cancelled.load(Ordering::Acquire)
    }

    pub(super) fn track_helper(&self, child: Child) -> Result<()> {
        let mut helper = self
            .0
            .helper
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if self.cancelled() || helper.is_some() {
            drop(helper);
            terminate(child);
            bail!("Operation cancelled or already running a helper");
        }
        *helper = Some(child);
        Ok(())
    }

    pub(super) fn helper_status(&self) -> Result<Option<ExitStatus>> {
        self.0
            .helper
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .as_mut()
            .context("Operation cancelled")?
            .try_wait()
            .map_err(Into::into)
    }

    pub(super) fn stop_helper(&self) {
        let helper = self
            .0
            .helper
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        if let Some(child) = helper {
            terminate(child);
        }
    }
}

fn terminate(mut child: Child) {
    match child.try_wait() {
        Ok(Some(_)) | Err(_) => return,
        Ok(None) => (),
    }
    // SAFETY: this unreaped child was spawned into its own private process group.
    // Only this owned Child can reap the PID; no concurrent waiter can reuse it.
    unsafe {
        libc::kill(-(child.id() as libc::pid_t), libc::SIGKILL);
    }
    let deadline = Instant::now() + Duration::from_millis(50);
    loop {
        match child.try_wait() {
            Ok(Some(_)) | Err(_) => return,
            Ok(None) => (),
        }
        if Instant::now() >= deadline {
            // SIGKILL is already pending. A kernel-blocked child must not prevent
            // leaving the dashboard; reap it when the kernel lets it finish.
            thread::spawn(move || {
                let _ = child.wait();
            });
            return;
        }
        thread::sleep(Duration::from_millis(1));
    }
}

struct Running {
    cancel: Cancellation,
    deadline: Instant,
    timeout: Duration,
}

pub(super) struct Worker<I, O> {
    requests: Option<Sender<(I, Cancellation)>>,
    responses: Receiver<(Instant, Result<O>)>,
    thread: Option<JoinHandle<()>>,
    current: Option<Running>,
}

impl<I: Send + 'static, O: Send + 'static> Worker<I, O> {
    pub(super) fn new(
        mut operation: impl FnMut(I, &Cancellation) -> Result<O> + Send + 'static,
    ) -> Self {
        let (requests, receive) = mpsc::channel::<(I, Cancellation)>();
        let (send, responses) = mpsc::channel();
        let thread = thread::spawn(move || {
            while let Ok((input, cancel)) = receive.recv() {
                let result = cancel.check().and_then(|()| operation(input, &cancel));
                if send.send((Instant::now(), result)).is_err() {
                    break;
                }
            }
        });
        Self {
            requests: Some(requests),
            responses,
            thread: Some(thread),
            current: None,
        }
    }

    pub(super) fn busy(&self) -> bool {
        self.current.is_some()
    }

    pub(super) fn start(&mut self, input: I, timeout: Duration) -> Result<()> {
        // Ignore repeated Enter/refreshes until the previous operation has been collected.
        if self.busy() {
            return Ok(());
        }
        let cancel = Cancellation::default();
        let deadline = Instant::now() + timeout;
        self.requests
            .as_ref()
            .unwrap()
            .send((input, cancel.clone()))
            .map_err(|_| anyhow::anyhow!("Worker disconnected"))
            .context("Status worker stopped unexpectedly")?;
        self.current = Some(Running {
            cancel,
            deadline,
            timeout,
        });
        Ok(())
    }

    pub(super) fn poll(&mut self) -> Option<Result<O>> {
        let (completed, result) = match self.responses.try_recv() {
            Ok(reply) => reply,
            Err(mpsc::TryRecvError::Empty) => {
                let current = self.current.as_ref()?;
                if !current.cancel.cancelled() && Instant::now() >= current.deadline {
                    current.cancel.cancel();
                    // Keep this job occupied until its read returns. Never accumulate
                    // abandoned threads by starting replacements for a stuck refresh.
                    return Some(Err(anyhow::anyhow!(
                        "Operation timed out after {:?}; waiting for the current operation to stop",
                        current.timeout
                    )));
                }
                return None;
            }
            Err(mpsc::TryRecvError::Disconnected) if self.busy() => (
                Instant::now(),
                Err(anyhow::anyhow!("Status worker stopped unexpectedly")),
            ),
            Err(_) => return None,
        };
        let current = self.current.take()?;
        if current.cancel.cancelled() {
            return None;
        }
        if completed >= current.deadline {
            current.cancel.cancel();
            return Some(Err(anyhow::anyhow!(
                "Operation timed out after {:?}",
                current.timeout
            )));
        }
        Some(result)
    }

    pub(super) fn cancel(&self) {
        if let Some(current) = &self.current {
            current.cancel.cancel();
        }
    }
}

impl<I, O> Drop for Worker<I, O> {
    fn drop(&mut self) {
        if let Some(current) = &self.current {
            current.cancel.cancel();
        }
        self.requests.take();
        // Helper cleanup is bounded and independent of the thread. Detach a worker
        // stuck in filesystem I/O; cancellation prevents it from starting more work.
        if let Some(thread) = self.thread.take().filter(|thread| thread.is_finished()) {
            let _ = thread.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn a_busy_worker_does_not_queue_requests_and_cancellation_discards_late_results() {
        let (started, wait_started) = mpsc::channel();
        let mut worker = Worker::new(move |value: u32, cancel: &Cancellation| {
            started.send(value).unwrap();
            while !cancel.cancelled() {
                thread::sleep(Duration::from_millis(1));
            }
            Ok(value)
        });
        worker.start(1, Duration::from_secs(2)).unwrap();
        assert_eq!(
            wait_started.recv_timeout(Duration::from_secs(2)).unwrap(),
            1
        );
        worker.start(2, Duration::from_secs(2)).unwrap();
        worker.cancel();
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while worker.busy() {
            assert!(worker.poll().is_none());
            assert!(std::time::Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        assert!(wait_started.try_recv().is_err());
        worker.start(3, Duration::from_secs(2)).unwrap();
        assert_eq!(
            wait_started.recv_timeout(Duration::from_secs(2)).unwrap(),
            3
        );
        drop(worker);
    }

    #[test]
    fn deadline_reports_once_discards_late_results_and_allows_recovery() {
        let (release, wait) = mpsc::channel();
        let (started, ready) = mpsc::channel();
        let mut worker = Worker::new(move |input: u32, _cancel| {
            started.send(input).unwrap();
            if input == 1 {
                wait.recv().unwrap();
            } // Model an uninterruptible read.
            Ok(input)
        });
        worker.start(1, Duration::from_millis(20)).unwrap();
        ready.recv_timeout(Duration::from_secs(2)).unwrap();
        thread::sleep(Duration::from_millis(30));
        assert!(
            worker
                .poll()
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("timed out")
        );
        assert!(worker.busy());
        assert!(worker.poll().is_none());
        worker.start(2, Duration::from_secs(2)).unwrap();
        assert!(ready.try_recv().is_err());
        release.send(()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while worker.busy() {
            assert!(
                worker.poll().is_none(),
                "a stale result must never be published"
            );
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
        worker.start(3, Duration::from_secs(2)).unwrap();
        ready.recv_timeout(Duration::from_secs(2)).unwrap();
        loop {
            if let Some(result) = worker.poll() {
                assert_eq!(result.unwrap(), 3);
                break;
            }
            assert!(Instant::now() < deadline);
            thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn shutdown_does_not_wait_for_a_blocked_read() {
        let (release, wait) = mpsc::channel();
        let (started, ready) = mpsc::channel();
        let mut worker = Worker::new(move |(), _cancel| {
            started.send(()).unwrap();
            let _ = wait.recv_timeout(Duration::from_secs(2));
            Ok(())
        });
        worker.start((), Duration::from_secs(5)).unwrap();
        ready.recv_timeout(Duration::from_secs(2)).unwrap();
        let start = Instant::now();
        drop(worker);
        let elapsed = start.elapsed();
        release.send(()).unwrap();
        assert!(
            elapsed < Duration::from_millis(100),
            "shutdown waited {elapsed:?}"
        );
    }

    #[test]
    fn shutdown_reaps_a_helper_even_when_the_worker_cannot_check_cancellation() {
        use std::{
            os::unix::process::CommandExt,
            process::{Command, Stdio},
        };
        let (release, wait) = mpsc::channel();
        let (started, ready) = mpsc::channel();
        let mut worker = Worker::new(move |(), cancel| {
            let child = Command::new("sleep")
                .arg("30")
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .process_group(0)
                .spawn()?;
            let pid = child.id();
            cancel.track_helper(child)?;
            started.send(pid).unwrap();
            let _ = wait.recv_timeout(Duration::from_secs(2));
            cancel.check()
        });
        worker.start((), Duration::from_secs(5)).unwrap();
        let pid = ready.recv_timeout(Duration::from_secs(2)).unwrap();
        let start = Instant::now();
        drop(worker);
        let elapsed = start.elapsed();
        let _ = release.send(());
        assert!(
            elapsed < Duration::from_millis(100),
            "shutdown waited {elapsed:?}"
        );
        // SAFETY: signal 0 only checks whether our reaped helper still exists.
        assert_eq!(unsafe { libc::kill(pid as libc::pid_t, 0) }, -1);
    }
}
