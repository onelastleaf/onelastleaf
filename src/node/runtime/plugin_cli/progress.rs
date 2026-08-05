use std::{
    io::{self, IsTerminal, Write},
    sync::mpsc,
    thread,
    time::Duration,
};

/// Best-effort terminal feedback for commands whose legitimate duration is
/// unbounded. Machine output and redirected stderr never start a worker.
pub(super) struct CommandProgress {
    stop: Option<mpsc::Sender<()>>,
    worker: Option<thread::JoinHandle<()>>,
}

impl CommandProgress {
    pub(super) fn start(label: &'static str, enabled: bool) -> Self {
        if !enabled || !io::stderr().is_terminal() {
            return Self {
                stop: None,
                worker: None,
            };
        }

        let (stop, receiver) = mpsc::channel();
        let worker = thread::spawn(move || {
            let frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
            let mut index = 0_usize;
            loop {
                let mut stderr = io::stderr().lock();
                let _ = write!(stderr, "\r{} {label}", frames[index % frames.len()]);
                let _ = stderr.flush();
                drop(stderr);
                index += 1;
                match receiver.recv_timeout(Duration::from_millis(80)) {
                    Ok(()) | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                }
            }
            let mut stderr = io::stderr().lock();
            let _ = write!(stderr, "\r{:width$}\r", "", width = label.len() + 3);
            let _ = stderr.flush();
        });
        Self {
            stop: Some(stop),
            worker: Some(worker),
        }
    }
}

impl Drop for CommandProgress {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}
