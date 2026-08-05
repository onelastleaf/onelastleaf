use std::{
    fs::{self, OpenOptions},
    io,
    os::unix::fs::OpenOptionsExt,
    path::Path,
    process::{ExitStatus, Stdio},
    sync::Arc,
    time::Duration,
};

use tokio::{process::Command, sync::watch, time::Instant};

use super::PackageError;
use super::manager::owner::PackageTaskOwner;

const PROCESS_GRACE: Duration = Duration::from_secs(2);
const KILL_REAP_RESERVE: Duration = Duration::from_millis(250);

#[derive(Clone)]
pub struct ProcessCancellation {
    receiver: watch::Receiver<Option<Instant>>,
    cleanup_after_drop: Vec<std::path::PathBuf>,
    owner: Option<Arc<PackageTaskOwner>>,
    #[cfg(test)]
    panic_message: Option<&'static str>,
}

impl ProcessCancellation {
    #[cfg(test)]
    pub fn channel() -> (watch::Sender<Option<Instant>>, Self) {
        let (sender, receiver) = watch::channel(None);
        (
            sender,
            Self {
                receiver,
                cleanup_after_drop: Vec::new(),
                owner: None,
                panic_message: None,
            },
        )
    }

    pub fn with_cleanup_after_drop(
        mut self,
        paths: impl IntoIterator<Item = std::path::PathBuf>,
    ) -> Self {
        self.cleanup_after_drop.extend(paths);
        self
    }

    pub(in crate::plugin::package) fn with_owner(mut self, owner: Arc<PackageTaskOwner>) -> Self {
        self.owner = Some(owner);
        self
    }

    #[cfg(test)]
    fn panic_for_test(mut self, message: &'static str) -> Self {
        self.panic_message = Some(message);
        self
    }

    pub fn from_receiver(receiver: watch::Receiver<Option<Instant>>) -> Self {
        Self {
            receiver,
            cleanup_after_drop: Vec::new(),
            owner: None,
            #[cfg(test)]
            panic_message: None,
        }
    }

    async fn cancelled(&mut self) -> Option<Instant> {
        if let Some(deadline) = *self.receiver.borrow() {
            return Some(deadline);
        }
        let _ = self.receiver.changed().await;
        *self.receiver.borrow()
    }
}

#[derive(Debug)]
pub enum ProcessOutcome {
    Exited(ExitStatus),
    Cancelled,
}

pub async fn run_process_group(
    argv: &[String],
    working_directory: &Path,
    build_log_path: &Path,
    cancellation: ProcessCancellation,
) -> Result<ProcessOutcome, PackageError> {
    let argv = argv.to_vec();
    let working_directory = working_directory.to_owned();
    let build_log_path = build_log_path.to_owned();
    let owner = cancellation.owner.clone();
    let (drop_sender, drop_receiver) = watch::channel(false);
    let process = run_owned_process_group(
        argv,
        working_directory,
        build_log_path,
        cancellation,
        drop_receiver,
    );
    let mut call = ProcessCallGuard {
        drop_sender: Some(drop_sender),
    };
    let result = match owner {
        Some(owner) => {
            let receiver = owner.spawn_process(process).await.map_err(|_| {
                PackageError::new(
                    "recipe_step_failed",
                    "process",
                    "package process task could not be admitted",
                )
            })?;
            receiver.await.map_err(|_| {
                PackageError::new(
                    "recipe_step_failed",
                    "process",
                    "package process task failed unexpectedly",
                )
            })??
        }
        None => tokio::spawn(process).await.map_err(|_| {
            PackageError::new(
                "recipe_step_failed",
                "process",
                "package process task failed unexpectedly",
            )
        })??,
    };
    call.drop_sender = None;
    Ok(result)
}

async fn run_owned_process_group(
    argv: Vec<String>,
    working_directory: std::path::PathBuf,
    build_log_path: std::path::PathBuf,
    mut cancellation: ProcessCancellation,
    mut dropped: watch::Receiver<bool>,
) -> Result<ProcessOutcome, PackageError> {
    #[cfg(test)]
    if let Some(message) = cancellation.panic_message {
        panic!("{message}");
    }
    if *dropped.borrow() {
        return Ok(ProcessOutcome::Cancelled);
    }
    let Some(executable) = argv.first().filter(|value| !value.is_empty()) else {
        return Err(PackageError::manifest(
            "process argv requires a nonempty executable",
        ));
    };
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(&build_log_path)
        .map_err(|error| {
            PackageError::io(
                "recipe_step_failed",
                "process",
                "cannot open package build log",
                error,
            )
        })?;
    let stderr = log.try_clone().map_err(|error| {
        PackageError::io(
            "recipe_step_failed",
            "process",
            "cannot duplicate package build log",
            error,
        )
    })?;
    let mut command = Command::new(executable);
    command
        .args(&argv[1..])
        .current_dir(&working_directory)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(stderr))
        .kill_on_drop(true);
    unsafe {
        command.pre_exec(|| {
            if libc::setpgid(0, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = command.spawn().map_err(|error| {
        PackageError::io(
            "recipe_step_failed",
            "process",
            "cannot spawn package process",
            error,
        )
        .with_build_log(build_log_path.clone())
    })?;
    let pid = child.id().ok_or_else(|| {
        PackageError::new(
            "recipe_step_failed",
            "process",
            "spawned package process has no process ID",
        )
    })?;
    let mut owned = OwnedProcessGroup {
        child,
        pid: i32::try_from(pid).map_err(|_| {
            PackageError::new(
                "recipe_step_failed",
                "process",
                "spawned package process ID is out of range",
            )
        })?,
        reaped: false,
    };
    let cleanup_after_drop = cancellation.cleanup_after_drop.clone();

    tokio::select! {
        status = owned.child.wait() => {
            let status = status.map_err(|error| PackageError::io(
                "recipe_step_failed",
                "process",
                "cannot wait for package process",
                error,
            ).with_build_log(build_log_path.clone()))?;
            owned.reaped = true;
            if process_group_exists(owned.pid)? {
                terminate_process_group(&mut owned, None).await?;
                return Err(PackageError::new(
                    "recipe_step_failed",
                    "process",
                    "package process exited while descendants remained in its process group",
                ).with_build_log(build_log_path));
            }
            Ok(ProcessOutcome::Exited(status))
        }
        deadline = cancellation.cancelled() => {
            terminate_process_group(&mut owned, deadline).await?;
            Ok(ProcessOutcome::Cancelled)
        }
        _ = dropped.changed() => {
            terminate_process_group(&mut owned, None).await?;
            for path in cleanup_after_drop {
                let _ = fs::remove_dir_all(path);
            }
            Ok(ProcessOutcome::Cancelled)
        }
    }
}

struct ProcessCallGuard {
    drop_sender: Option<watch::Sender<bool>>,
}

impl Drop for ProcessCallGuard {
    fn drop(&mut self) {
        if let Some(sender) = self.drop_sender.take() {
            let _ = sender.send(true);
        }
    }
}

struct OwnedProcessGroup {
    child: tokio::process::Child,
    pid: i32,
    reaped: bool,
}

impl Drop for OwnedProcessGroup {
    fn drop(&mut self) {
        if !self.reaped {
            unsafe {
                libc::kill(-self.pid, libc::SIGKILL);
            }
            let _ = self.child.start_kill();
        }
    }
}

async fn terminate_process_group(
    process: &mut OwnedProcessGroup,
    deadline: Option<Instant>,
) -> Result<(), PackageError> {
    signal_group(process.pid, libc::SIGTERM)?;
    let grace = deadline
        .map(|deadline| {
            deadline
                .checked_sub(KILL_REAP_RESERVE)
                .unwrap_or_else(Instant::now)
                .min(Instant::now() + PROCESS_GRACE)
        })
        .unwrap_or_else(|| Instant::now() + PROCESS_GRACE);
    if !process.reaped {
        match tokio::time::timeout_at(grace, process.child.wait()).await {
            Ok(Ok(_)) => process.reaped = true,
            Ok(Err(error)) => {
                return Err(PackageError::io(
                    "recipe_step_failed",
                    "process",
                    "cannot reap cancelled package process",
                    error,
                ));
            }
            Err(_) => {}
        }
    }
    if wait_for_group_exit(process.pid, grace).await? {
        return Ok(());
    }
    signal_group(process.pid, libc::SIGKILL)?;
    let kill_deadline = deadline.unwrap_or_else(|| Instant::now() + PROCESS_GRACE);
    if !process.reaped {
        tokio::time::timeout_at(kill_deadline, process.child.wait())
            .await
            .map_err(|_| {
                PackageError::new(
                    "recipe_step_failed",
                    "process",
                    "cancelled package process could not be reaped before shutdown deadline",
                )
            })?
            .map_err(|error| {
                PackageError::io(
                    "recipe_step_failed",
                    "process",
                    "cannot reap cancelled package process",
                    error,
                )
            })?;
        process.reaped = true;
    }
    if !wait_for_group_exit(process.pid, kill_deadline).await? {
        return Err(PackageError::new(
            "recipe_step_failed",
            "process",
            "package process group remained alive after SIGKILL",
        ));
    }
    Ok(())
}

async fn wait_for_group_exit(pid: i32, deadline: Instant) -> Result<bool, PackageError> {
    loop {
        if !process_group_exists(pid)? {
            return Ok(true);
        }
        if Instant::now() >= deadline {
            return Ok(false);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn process_group_exists(pid: i32) -> Result<bool, PackageError> {
    let result = unsafe { libc::kill(-pid, 0) };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    match error.raw_os_error() {
        Some(libc::ESRCH) => Ok(false),
        Some(libc::EPERM) => Ok(true),
        _ => Err(PackageError::io(
            "recipe_step_failed",
            "process",
            "cannot inspect package process group",
            error,
        )),
    }
}

fn signal_group(pid: i32, signal: i32) -> Result<(), PackageError> {
    let result = unsafe { libc::kill(-pid, signal) };
    if result == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.raw_os_error() == Some(libc::ESRCH) {
        Ok(())
    } else {
        Err(PackageError::io(
            "recipe_step_failed",
            "process",
            "cannot signal package process group",
            error,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cancellation_terminates_and_reaps_the_complete_process_group() {
        let directory = tempfile::TempDir::new().unwrap();
        let log = directory.path().join("build.log");
        let (cancel, cancellation) = ProcessCancellation::channel();
        let argv = vec![
            "/bin/sh".to_owned(),
            "-c".to_owned(),
            "trap 'exit 0' TERM; while :; do sleep 1; done".to_owned(),
        ];
        let working_directory = directory.path().to_owned();
        let task = tokio::spawn(async move {
            run_process_group(&argv, &working_directory, &log, cancellation).await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        cancel
            .send(Some(Instant::now() + Duration::from_secs(3)))
            .unwrap();
        let outcome = tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(outcome, ProcessOutcome::Cancelled));
    }

    #[tokio::test]
    async fn dropping_the_caller_cleans_files_written_during_process_shutdown() {
        let directory = tempfile::TempDir::new().unwrap();
        let log = directory.path().join("build.log");
        let workspace = directory.path().join("candidate");
        fs::create_dir(&workspace).unwrap();
        let script = format!(
            "trap 'mkdir -p {0}; echo late > {0}/late; exit 0' TERM; while :; do sleep 1; done",
            workspace.display()
        );
        let logger = crate::node::logging::NodeLogger::open(
            &directory.path().join("logs"),
            crate::node::identity::NodeIdentity::generate("process-tests".parse().unwrap()),
            None,
        )
        .unwrap();
        let owner = PackageTaskOwner::new(logger);
        let process_owner = Arc::clone(&owner);
        let (_cancel, cancellation) = ProcessCancellation::channel();
        let argv = vec!["/bin/sh".to_owned(), "-c".to_owned(), script];
        let working_directory = directory.path().to_owned();
        let cleanup_workspace = workspace.clone();
        let task = tokio::spawn(async move {
            run_process_group(
                &argv,
                &working_directory,
                &log,
                cancellation
                    .with_owner(process_owner)
                    .with_cleanup_after_drop([cleanup_workspace]),
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        task.abort();
        let _ = task.await;
        owner
            .shutdown(Instant::now() + Duration::from_secs(3))
            .await
            .unwrap();
        assert!(!workspace.exists());
    }

    #[tokio::test]
    async fn process_task_panics_return_a_stable_redacted_diagnostic() {
        let directory = tempfile::TempDir::new().unwrap();
        let secret = "do-not-return-this-process-panic-payload";
        let (_cancel, cancellation) = ProcessCancellation::channel();
        let error = run_process_group(
            &["unused".to_owned()],
            directory.path(),
            &directory.path().join("build.log"),
            cancellation.panic_for_test(secret),
        )
        .await
        .unwrap_err();

        assert_eq!(error.code(), "recipe_step_failed");
        assert_eq!(error.message(), "package process task failed unexpectedly");
        assert!(!error.to_string().contains(secret));
    }

    #[tokio::test]
    async fn owner_deadline_abort_kills_the_complete_process_group() {
        let directory = tempfile::TempDir::new().unwrap();
        let log = directory.path().join("build.log");
        let pid_path = directory.path().join("leader.pid");
        let script = format!(
            "echo $$ > '{}'; trap '' TERM; while :; do sleep 1; done",
            pid_path.display()
        );
        let logger = crate::node::logging::NodeLogger::open(
            &directory.path().join("logs"),
            crate::node::identity::NodeIdentity::generate("deadline-tests".parse().unwrap()),
            None,
        )
        .unwrap();
        let owner = PackageTaskOwner::new(logger);
        let process_owner = Arc::clone(&owner);
        let (_cancel, cancellation) = ProcessCancellation::channel();
        let working_directory = directory.path().to_owned();
        let task = tokio::spawn(async move {
            run_process_group(
                &["/bin/sh".to_owned(), "-c".to_owned(), script],
                &working_directory,
                &log,
                cancellation.with_owner(process_owner),
            )
            .await
        });
        let spawn_deadline = Instant::now() + Duration::from_secs(2);
        while !pid_path.exists() && Instant::now() < spawn_deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let pid: i32 = fs::read_to_string(&pid_path)
            .expect("package process did not record its PID")
            .trim()
            .parse()
            .unwrap();

        let shutdown = owner.shutdown(Instant::now()).await;
        assert!(matches!(
            shutdown,
            Err(crate::plugin::PluginError::FailedPrecondition(_))
        ));
        assert!(task.await.unwrap().is_err());

        let exit_deadline = Instant::now() + Duration::from_secs(2);
        while unsafe { libc::kill(-pid, 0) } == 0 && Instant::now() < exit_deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(unsafe { libc::kill(-pid, 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
    }

    #[tokio::test]
    async fn leader_exit_cleans_a_stubborn_descendant_process_group() {
        let directory = tempfile::TempDir::new().unwrap();
        let log = directory.path().join("build.log");
        let child_pid = directory.path().join("child.pid");
        let script = format!(
            "(trap '' TERM; while :; do sleep 1; done) & echo $! > '{}'; exit 0",
            child_pid.display()
        );
        let (_cancel, cancellation) = ProcessCancellation::channel();
        let result = tokio::time::timeout(
            Duration::from_secs(6),
            run_process_group(
                &["/bin/sh".to_owned(), "-c".to_owned(), script],
                directory.path(),
                &log,
                cancellation,
            ),
        )
        .await
        .expect("stubborn descendant cleanup exceeded its bounded grace")
        .unwrap_err();
        assert_eq!(result.code(), "recipe_step_failed");
        assert!(result.message().contains("descendants remained"));

        let pid: i32 = fs::read_to_string(child_pid)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        while unsafe { libc::kill(pid, 0) } == 0 && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::ESRCH));
    }
}
