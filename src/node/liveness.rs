//! Parent-liveness pipe support for independently spawned plugins.

use std::{
    fs::File,
    io::{self, Read},
    os::unix::io::{AsRawFd, FromRawFd},
};

use super::runtime::NodeError;

/// The daemon keeps the write end alive. Each plugin child receives a duplicated
/// read end and treats EOF as proof that its oll parent has exited.
pub struct ParentLivenessPipe {
    reader_template: File,
    _writer: File,
}

impl ParentLivenessPipe {
    pub fn create() -> Result<Self, NodeError> {
        let mut descriptors = [-1; 2];
        let result = unsafe { libc::pipe2(descriptors.as_mut_ptr(), libc::O_CLOEXEC) };
        if result == -1 {
            return Err(NodeError::io(
                "create parent-liveness pipe",
                io::Error::last_os_error(),
            ));
        }

        // pipe2 returned two owned descriptors that are transferred to File.
        let reader_template = unsafe { File::from_raw_fd(descriptors[0]) };
        let writer = unsafe { File::from_raw_fd(descriptors[1]) };
        Ok(Self {
            reader_template,
            _writer: writer,
        })
    }

    /// Duplicate a reader for one child. The caller deliberately clears
    /// close-on-exec in its child-spawn setup before handing the FD to that child.
    pub fn reader_for_child(&self) -> Result<File, NodeError> {
        let descriptor =
            unsafe { libc::fcntl(self.reader_template.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
        if descriptor == -1 {
            return Err(NodeError::io(
                "duplicate parent-liveness reader",
                io::Error::last_os_error(),
            ));
        }
        Ok(unsafe { File::from_raw_fd(descriptor) })
    }
}

/// Block until the parent closes its writer. Plugin implementations can use
/// this helper when they are written in Rust; other languages observe EOF on
/// their inherited stdin directly.
pub fn wait_for_parent_exit(mut reader: File) -> io::Result<()> {
    let mut buffer = [0_u8; 1];
    loop {
        match reader.read(&mut buffer) {
            Ok(0) => return Ok(()),
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        process::{Command, Stdio},
        sync::mpsc,
        thread,
        time::{Duration, Instant},
    };

    use super::*;

    #[test]
    fn child_reader_observes_eof_when_the_parent_owner_drops() {
        let pipe = ParentLivenessPipe::create().unwrap();
        let reader = pipe.reader_for_child().unwrap();
        let (sender, receiver) = mpsc::channel();
        let task = thread::spawn(move || {
            wait_for_parent_exit(reader).unwrap();
            sender.send(()).unwrap();
        });

        assert!(receiver.recv_timeout(Duration::from_millis(20)).is_err());
        drop(pipe);
        receiver.recv_timeout(Duration::from_secs(1)).unwrap();
        task.join().unwrap();
    }

    #[test]
    fn spawned_child_observes_parent_liveness_eof_and_is_reaped() {
        let pipe = ParentLivenessPipe::create().unwrap();
        let reader = pipe.reader_for_child().unwrap();
        let mut child = Command::new("cat")
            .stdin(Stdio::from(reader))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();

        thread::sleep(Duration::from_millis(20));
        assert!(
            child.try_wait().unwrap().is_none(),
            "child exited before the parent-liveness writer closed"
        );
        drop(pipe);

        let deadline = Instant::now() + Duration::from_secs(2);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if Instant::now() >= deadline {
                let _ = child.kill();
                let _ = child.wait();
                panic!("child did not exit after parent-liveness EOF");
            }
            thread::sleep(Duration::from_millis(10));
        };
        assert!(status.success(), "child failed after observing stdin EOF");
    }
}
