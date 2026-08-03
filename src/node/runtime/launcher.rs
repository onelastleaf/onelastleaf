use std::{io, path::Path, process::Stdio, time::Duration};

use getrandom::fill as fill_random;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    process::Child,
    time::{Instant, sleep, timeout},
};

use crate::node::{
    admin,
    lock::{DeploymentLock, admin_socket_path},
    logging::new_correlation_id,
};

use super::{
    LAUNCHER_TERMINATION_GRACE, NodeError, SHUTDOWN_DEADLINE, STARTUP_DEADLINE,
    blocking::in_runtime,
};

pub(super) fn start(config_root: &Path) -> Result<(), NodeError> {
    let deadline = Instant::now() + STARTUP_DEADLINE;
    DeploymentLock::preflight(config_root)?;
    let admin_socket = admin_socket_path(config_root);
    if std::os::unix::net::UnixStream::connect(&admin_socket).is_ok() {
        return Err(NodeError::Unavailable(
            "an Admin endpoint already answers for this deployment".to_owned(),
        ));
    }
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0")
        .map_err(|error| NodeError::io("bind startup pingback listener", error))?;
    std_listener
        .set_nonblocking(true)
        .map_err(|error| NodeError::io("configure startup pingback listener", error))?;
    let address = std_listener
        .local_addr()
        .map_err(|error| NodeError::io("read startup pingback address", error))?;
    let mut nonce = [0_u8; 32];
    fill_random(&mut nonce)
        .map_err(|error| NodeError::Internal(format!("cannot generate startup nonce: {error}")))?;

    let executable = std::env::current_exe()
        .map_err(|error| NodeError::io("locate the oll executable", error))?;
    let mut command = tokio::process::Command::new(executable);
    command
        .arg("run")
        .arg("--config")
        .arg(config_root)
        .arg("--pingback")
        .arg(address.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    unsafe {
        command.pre_exec(|| {
            if libc::setsid() == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }

    in_runtime(start_async(std_listener, command, nonce, deadline))
}

async fn start_async(
    std_listener: std::net::TcpListener,
    mut command: tokio::process::Command,
    nonce: [u8; 32],
    deadline: Instant,
) -> Result<(), NodeError> {
    let listener = TcpListener::from_std(std_listener)
        .map_err(|error| NodeError::io("adopt startup pingback listener", error))?;
    let mut child = command
        .spawn()
        .map_err(|error| NodeError::io("spawn detached oll run", error))?;
    let write_result = async {
        let stdin = child.stdin.as_mut().ok_or_else(|| {
            NodeError::Internal("detached oll run did not expose its stdin pipe".to_owned())
        })?;
        stdin
            .write_all(&nonce)
            .await
            .map_err(|error| NodeError::io("write startup nonce", error))?;
        stdin
            .shutdown()
            .await
            .map_err(|error| NodeError::io("close startup nonce pipe", error))
    }
    .await;
    if let Err(error) = write_result {
        terminate_child(&mut child).await;
        return Err(error);
    }

    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        terminate_child(&mut child).await;
        return Err(NodeError::Unavailable(
            "oll start timed out before daemon readiness".to_owned(),
        ));
    }
    let handshake = tokio::select! {
        result = async {
            timeout(remaining, async {
                let (mut stream, _) = listener.accept().await
                    .map_err(|error| NodeError::io("accept startup pingback", error))?;
                let mut reply = [0_u8; 32];
                stream.read_exact(&mut reply).await
                    .map_err(|error| NodeError::io("read startup pingback", error))?;
                Ok::<[u8; 32], NodeError>(reply)
            }).await
        } => match result {
            Ok(result) => result,
            Err(_) => Err(NodeError::Unavailable("oll start timed out before daemon readiness".to_owned())),
        },
        status = child.wait() => match status {
            Ok(status) => Err(NodeError::Unavailable(format!("oll run exited before readiness: {status}"))),
            Err(error) => Err(NodeError::io("wait for detached oll run", error)),
        },
    };
    match handshake {
        Ok(reply) if constant_time_eq(&nonce, &reply) => Ok(()),
        Ok(_) => {
            terminate_child(&mut child).await;
            Err(NodeError::Unavailable(
                "oll start received an invalid readiness pingback".to_owned(),
            ))
        }
        Err(error) => {
            terminate_child(&mut child).await;
            Err(error)
        }
    }
}

async fn terminate_child(child: &mut Child) {
    if let Some(process_id) = child.id() {
        unsafe {
            libc::kill(process_id as i32, libc::SIGTERM);
        }
    }
    if timeout(LAUNCHER_TERMINATION_GRACE, child.wait())
        .await
        .is_err()
    {
        let _ = child.start_kill();
        let _ = child.wait().await;
    }
}

fn constant_time_eq(left: &[u8; 32], right: &[u8; 32]) -> bool {
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

pub(super) async fn stop(config_root: &Path) -> Result<(), NodeError> {
    let socket = admin_socket_path(config_root);
    // A daemon that has already accepted another Shutdown no longer serves
    // GetStatus, but Shutdown itself remains idempotently available until the
    // Admin server begins closing its listener.
    let process_id = match admin::get_status(&socket, new_correlation_id()).await {
        Ok(status) => Some(status.process_id),
        Err(NodeError::Unavailable(_)) => None,
        Err(error) => return Err(error),
    };
    admin::request_shutdown(&socket, new_correlation_id()).await?;

    let deadline = Instant::now() + SHUTDOWN_DEADLINE;
    loop {
        let lock_free = match DeploymentLock::preflight(config_root) {
            Ok(()) => true,
            Err(NodeError::Unavailable(_)) => false,
            Err(error) => return Err(error),
        };
        let socket_gone = !socket.exists();
        let process_exited = process_id.is_some_and(process_has_exited);
        if lock_free && (socket_gone || process_exited) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(NodeError::Unavailable(
                "oll stop timed out waiting for the daemon to exit".to_owned(),
            ));
        }
        sleep(Duration::from_millis(25)).await;
    }
}

fn process_has_exited(process_id: u32) -> bool {
    let result = unsafe { libc::kill(process_id as i32, 0) };
    result == -1 && io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
}
