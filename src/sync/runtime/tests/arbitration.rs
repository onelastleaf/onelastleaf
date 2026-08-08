use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_outbound_waits_for_the_winning_session_then_recovers() {
    let first = SyncDeployment::new("arbitration-first");
    let (first_replica, first_logger) = first.start_replica().await;
    let second = SyncDeployment::new("arbitration-second");
    let (second_replica, second_logger) = second.start_replica().await;
    let first_listen = unused_loopback_address();
    let second_listen = unused_loopback_address();
    let first_sync = SyncRuntime::start(
        &first.sync_config(
            Some(first_listen),
            vec![ConnectUrl::from_str(&format!("oll://{second_listen}")).unwrap()],
        ),
        Arc::clone(&first.identities),
        Arc::clone(&first_replica),
        first_logger,
    )
    .await
    .unwrap();
    let second_sync = SyncRuntime::start(
        &second.sync_config(
            Some(second_listen),
            vec![ConnectUrl::from_str(&format!("oll://{first_listen}")).unwrap()],
        ),
        Arc::clone(&second.identities),
        Arc::clone(&second_replica),
        second_logger,
    )
    .await
    .unwrap();

    let (expected_first_direction, expected_second_direction) =
        if first.identity.node_id() < second.identity.node_id() {
            (
                PeerConnectionDirection::Outbound as i32,
                PeerConnectionDirection::Inbound as i32,
            )
        } else {
            (
                PeerConnectionDirection::Inbound as i32,
                PeerConnectionDirection::Outbound as i32,
            )
        };
    let ready_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let first_status = first_sync.status().await;
        let second_status = second_sync.status().await;
        if first_status.len() == 1
            && second_status.len() == 1
            && first_status[0].connection_state == PeerConnectionState::WaitingForReplica as i32
            && second_status[0].connection_state == PeerConnectionState::WaitingForReplica as i32
            && first_status[0].direction == expected_first_direction
            && second_status[0].direction == expected_second_direction
        {
            break;
        }
        assert!(
            Instant::now() < ready_deadline,
            "duplicate-session arbitration did not stabilize"
        );
        sleep(Duration::from_millis(25)).await;
    }

    let (preferred_sync, suppressed_sync, suppressed_replica, suppressed_log_dir) =
        if first.identity.node_id() < second.identity.node_id() {
            (&first_sync, &second_sync, &second_replica, &second.log_dir)
        } else {
            (&second_sync, &first_sync, &first_replica, &first.log_dir)
        };
    let read_events = || {
        fs::read_to_string(suppressed_log_dir.join("sync.log"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
            .collect::<Vec<_>>()
    };
    let outbound_attempts = |events: &[serde_json::Value]| {
        events
            .iter()
            .filter(|event| {
                (event["event"] == "sync_session_started" && event["direction"] == "outbound")
                    || event["event"] == "sync_connect_failed"
            })
            .count()
    };
    let suppression_deadline = Instant::now() + Duration::from_secs(2);
    let attempts_while_suppressed = loop {
        suppressed_replica
            .logger
            .flush_until(std::time::Instant::now() + Duration::from_secs(1))
            .unwrap();
        let events = read_events();
        if events
            .iter()
            .any(|event| event["event"] == "sync_duplicate_outbound_suppressed")
        {
            break outbound_attempts(&events);
        }
        assert!(
            Instant::now() < suppression_deadline,
            "losing outbound owner did not enter duplicate-session suppression"
        );
        sleep(Duration::from_millis(25)).await;
    };
    sleep(Duration::from_secs(1)).await;
    suppressed_replica
        .logger
        .flush_until(std::time::Instant::now() + Duration::from_secs(1))
        .unwrap();
    assert_eq!(
        outbound_attempts(&read_events()),
        attempts_while_suppressed,
        "losing outbound owner retried while the winning session remained active"
    );

    preferred_sync
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let reconnect_deadline = Instant::now() + Duration::from_secs(3);
    loop {
        suppressed_replica
            .logger
            .flush_until(std::time::Instant::now() + Duration::from_secs(1))
            .unwrap();
        if outbound_attempts(&read_events()) > attempts_while_suppressed {
            break;
        }
        assert!(
            Instant::now() < reconnect_deadline,
            "suppressed outbound owner did not reconnect after the winning session disappeared"
        );
        sleep(Duration::from_millis(25)).await;
    }

    suppressed_sync
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    second_replica
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    first_replica
        .shutdown(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
}
