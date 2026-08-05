use super::*;
use socket2::{SockRef, TcpKeepalive};

const TCP_KEEPALIVE_IDLE: Duration = Duration::from_secs(60);
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const TCP_KEEPALIVE_RETRIES: u32 = 3;
#[cfg(target_os = "linux")]
const TCP_USER_TIMEOUT: Duration = Duration::from_secs(120);

#[cfg(not(test))]
pub(super) const IDLE_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
#[cfg(test)]
pub(super) const IDLE_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(500);
#[cfg(not(test))]
pub(super) const HEARTBEAT_RESPONSE_DEADLINE: Duration = Duration::from_secs(10);
#[cfg(test)]
pub(super) const HEARTBEAT_RESPONSE_DEADLINE: Duration = Duration::from_millis(150);
#[cfg(not(test))]
pub(super) const SESSION_CLOSE_DEADLINE: Duration = Duration::from_secs(1);
#[cfg(test)]
pub(super) const SESSION_CLOSE_DEADLINE: Duration = Duration::from_millis(100);

pub(super) struct PendingPing {
    pub(super) sent_message_id: u64,
    pub(super) started: Instant,
    pub(super) deadline: Instant,
    pub(super) correlation_id: String,
    pub(super) response: Option<oneshot::Sender<Result<Duration, SyncError>>>,
}

pub(super) fn configure_tcp_liveness(stream: &TcpStream) -> std::io::Result<()> {
    let socket = SockRef::from(stream);
    socket.set_tcp_keepalive(
        &TcpKeepalive::new()
            .with_time(TCP_KEEPALIVE_IDLE)
            .with_interval(TCP_KEEPALIVE_INTERVAL)
            .with_retries(TCP_KEEPALIVE_RETRIES),
    )?;
    #[cfg(target_os = "linux")]
    socket.set_tcp_user_timeout(Some(TCP_USER_TIMEOUT))?;
    Ok(())
}

pub(super) fn random_ping_nonce() -> Result<u64, getrandom::Error> {
    let mut bytes = [0_u8; 8];
    fill_random(&mut bytes)?;
    Ok(u64::from_be_bytes(bytes))
}

pub(super) fn make_pending_pings_transparent(
    channel: &mut SessionChannel<TcpStream>,
    pings: &mut HashMap<u64, PendingPing>,
) {
    for (nonce, pending) in pings.drain() {
        channel.track_transparent_ping(
            nonce,
            pending.sent_message_id,
            Some(Instant::now() + PING_RESPONSE_DEADLINE),
        );
        if let Some(response) = pending.response {
            let _ = response.send(Err(SyncError::Unavailable(
                "sync round began while ping was in flight".to_owned(),
            )));
        }
    }
}

pub(super) fn direction_name(direction: Direction) -> &'static str {
    match direction {
        Direction::Inbound => "inbound",
        Direction::Outbound => "outbound",
    }
}

pub(super) fn log_session_liveness_failure(
    runtime: &SyncRuntime,
    observation: SyncObservation<'_>,
    failure_stage: &'static str,
    error_code: &'static str,
    idle: Duration,
) {
    runtime.logger.emit(
        LogLevel::Warn,
        "oll::sync",
        "sync_session_liveness_failed",
        observation.correlation_id,
        json!({
            "connection_id": observation.connection_id.to_string(),
            "peer_node_id": observation.peer_node_id.to_string(),
            "direction": observation.direction,
            "failure_stage": failure_stage,
            "failure_source": "transport",
            "error_code": error_code,
            "idle_ms": u64::try_from(idle.as_millis()).unwrap_or(u64::MAX),
        }),
    );
}

pub(super) fn log_round_session_failure(
    runtime: &SyncRuntime,
    observation: SyncObservation<'_>,
    error: &SyncError,
) {
    match error {
        SyncError::ProgressTimeout { failure_stage } => runtime.logger.emit(
            LogLevel::Warn,
            "oll::sync",
            "sync_round_progress_timeout",
            observation.correlation_id,
            json!({
                "connection_id": observation.connection_id.to_string(),
                "peer_node_id": observation.peer_node_id.to_string(),
                "direction": observation.direction,
                "failure_stage": failure_stage,
                "failure_source": if failure_stage.contains("inventory_capture") {
                    "local_store"
                } else {
                    "transport"
                },
                "error_code": "round_progress_timeout",
                "idle_ms": u64::try_from(ROUND_PROGRESS_DEADLINE.as_millis())
                    .unwrap_or(u64::MAX),
            }),
        ),
        SyncError::SessionLost(error) => {
            let mut fields =
                session_failure_fields(error, observation.direction, "round_transport", None);
            fields["connection_id"] = observation.connection_id.to_string().into();
            fields["peer_node_id"] = observation.peer_node_id.to_string().into();
            runtime.logger.emit(
                LogLevel::Warn,
                "oll::sync",
                "sync_session_liveness_failed",
                observation.correlation_id,
                fields,
            );
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn tcp_liveness_options_are_applied_to_established_streams() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let client = TcpStream::connect(address);
        let server = async { listener.accept().await.unwrap().0 };
        let (client, _server) = tokio::join!(client, server);
        let client = client.unwrap();

        configure_tcp_liveness(&client).unwrap();
        let socket = SockRef::from(&client);
        assert!(socket.keepalive().unwrap());
        assert_eq!(socket.tcp_keepalive_time().unwrap(), TCP_KEEPALIVE_IDLE);
        assert_eq!(
            socket.tcp_keepalive_interval().unwrap(),
            TCP_KEEPALIVE_INTERVAL
        );
        assert_eq!(
            socket.tcp_keepalive_retries().unwrap(),
            TCP_KEEPALIVE_RETRIES
        );
        #[cfg(target_os = "linux")]
        assert_eq!(socket.tcp_user_timeout().unwrap(), Some(TCP_USER_TIMEOUT));
    }
}
