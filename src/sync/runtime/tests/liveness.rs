use super::*;

async fn accept_test_session(
    listener: &TcpListener,
    key: &NoisePsk,
    identity: &NodeIdentity,
    status: ReplicaStatus,
    correlation_id: &str,
) -> PendingSession<TcpStream> {
    let (stream, _) = listener.accept().await.unwrap();
    let deadline = Instant::now() + HANDSHAKE_DEADLINE;
    let transport = NoiseTransport::accept(stream, key, deadline).await.unwrap();
    let mut session = PendingSession::begin(transport, identity, status, correlation_id, deadline)
        .await
        .unwrap();
    session
        .exchange_ready(correlation_id, deadline)
        .await
        .unwrap();
    session
}

async fn silent_heartbeat_reconnects(initialized: bool) {
    let client = SyncDeployment::new(if initialized {
        "heartbeat-ready-client"
    } else {
        "heartbeat-waiting-client"
    });
    if initialized {
        fs::write(client.root.join("ready.md"), "ready").unwrap();
    }
    let (client_replica, client_logger) = client.start_replica().await;
    let local_status = client_replica.status().await;
    let server_identity = NodeIdentity::generate(
        if initialized {
            "heartbeat-ready-server"
        } else {
            "heartbeat-waiting-server"
        }
        .parse()
        .unwrap(),
    );
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let listen = listener.local_addr().unwrap();
    let server_key = derive_noise_psk(&NetworkKey::new_for_test(vec![7; 32]));
    let (reconnected, reconnected_rx) = oneshot::channel();
    let server = tokio::spawn({
        let server_identity = server_identity.clone();
        async move {
            let mut first = accept_test_session(
                &listener,
                &server_key,
                &server_identity,
                local_status,
                "heartbeat-first-handshake",
            )
            .await;
            let heartbeat = first.channel.receive(None).await.unwrap();
            assert!(matches!(
                heartbeat.payload,
                Some(sync_envelope::Payload::Ping(_))
            ));
            assert!(matches!(
                first.channel.receive(None).await,
                Err(SessionError::RemoteClosed {
                    code: SyncCloseCode::InternalError,
                    ..
                })
            ));

            let mut second = accept_test_session(
                &listener,
                &server_key,
                &server_identity,
                local_status,
                "heartbeat-second-handshake",
            )
            .await;
            reconnected.send(()).unwrap();
            loop {
                let envelope = match second.channel.receive(None).await {
                    Ok(envelope) => envelope,
                    Err(SessionError::RemoteClosed { .. }) => break,
                    Err(error) => panic!("unexpected second-session error: {error}"),
                };
                if let Some(sync_envelope::Payload::Ping(ping)) = envelope.payload {
                    second
                        .channel
                        .send(
                            sync_envelope::Payload::Pong(SyncPong { nonce: ping.nonce }),
                            &envelope.correlation_id,
                            Some(envelope.message_id),
                            None,
                        )
                        .await
                        .unwrap();
                }
            }
        }
    });
    let client_sync = SyncRuntime::start(
        &client.sync_config(
            None,
            vec![ConnectUrl::from_str(&format!("oll://{listen}")).unwrap()],
        ),
        Arc::clone(&client.identities),
        Arc::clone(&client_replica),
        client_logger,
    )
    .await
    .unwrap();

    let first_deadline = Instant::now() + Duration::from_secs(2);
    let first_session_id = loop {
        if let Some(session_id) = client_sync
            .sessions
            .lock()
            .await
            .values()
            .next()
            .map(|session| session.session_id)
        {
            break session_id;
        }
        assert!(
            Instant::now() < first_deadline,
            "first session was not ready"
        );
        sleep(Duration::from_millis(10)).await;
    };
    tokio::time::timeout(Duration::from_secs(4), reconnected_rx)
        .await
        .expect("silent heartbeat did not cause a reconnect")
        .unwrap();
    let second_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let current = client_sync
            .sessions
            .lock()
            .await
            .values()
            .next()
            .map(|session| session.session_id);
        if current.is_some_and(|session_id| session_id != first_session_id) {
            break;
        }
        assert!(
            Instant::now() < second_deadline,
            "reconnected session was not registered"
        );
        sleep(Duration::from_millis(10)).await;
    }

    client_replica
        .logger
        .flush_until(std::time::Instant::now() + Duration::from_secs(2))
        .unwrap();
    let events = fs::read_to_string(client.log_dir.join("sync.log"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let failure = events
        .iter()
        .find(|event| event["event"] == "sync_session_liveness_failed")
        .expect("heartbeat failure must be observable");
    assert_eq!(failure["failure_stage"], "heartbeat_response");
    assert_eq!(failure["error_code"], "heartbeat_timeout");
    assert_eq!(failure["direction"], "outbound");
    assert!(failure["connection_id"].is_string());
    assert!(failure["peer_node_id"].is_string());
    assert!(failure["idle_ms"].as_u64().unwrap() >= 500);

    let shutdown_deadline = Instant::now() + Duration::from_secs(3);
    client_sync.shutdown(shutdown_deadline).await.unwrap();
    server.await.unwrap();
    client_replica.shutdown(shutdown_deadline).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn round_progress_timeout_unregisters_before_retrying_on_a_new_connection() {
    let client = SyncDeployment::new("round-timeout-client");
    fs::write(client.root.join("ready.md"), "ready").unwrap();
    let (client_replica, client_logger) = client.start_replica().await;
    let replica_id = match client_replica.status().await {
        ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
        status => panic!("unexpected initialized state: {status:?}"),
    };
    let server_identity = NodeIdentity::generate("round-timeout-server".parse().unwrap());
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let listen = listener.local_addr().unwrap();
    let server_key = derive_noise_psk(&NetworkKey::new_for_test(vec![7; 32]));
    let server = tokio::spawn({
        let server_identity = server_identity.clone();
        async move {
            for attempt in 1..=2 {
                let mut session = accept_test_session(
                    &listener,
                    &server_key,
                    &server_identity,
                    ReplicaStatus::InitializedPopulated { replica_id },
                    &format!("round-timeout-handshake-{attempt}"),
                )
                .await;
                let request = session.channel.receive(None).await.unwrap();
                assert!(matches!(
                    request.payload,
                    Some(sync_envelope::Payload::RoundRequest(_))
                ));
                assert!(matches!(
                    session.channel.receive(None).await,
                    Err(SessionError::RemoteClosed {
                        code: SyncCloseCode::InternalError,
                        ..
                    })
                ));
            }
        }
    });
    let client_sync = SyncRuntime::start(
        &client.sync_config(
            None,
            vec![ConnectUrl::from_str(&format!("oll://{listen}")).unwrap()],
        ),
        Arc::clone(&client.identities),
        Arc::clone(&client_replica),
        client_logger,
    )
    .await
    .unwrap();
    let ready_deadline = Instant::now() + Duration::from_secs(2);
    while client_sync.sessions.lock().await.is_empty() {
        assert!(
            Instant::now() < ready_deadline,
            "initial session was not ready"
        );
        sleep(Duration::from_millis(10)).await;
    }

    let results = tokio::time::timeout(
        Duration::from_secs(6),
        client_sync.synchronize(
            Some(server_identity.node_name()),
            2,
            "round-timeout-correlation",
        ),
    )
    .await
    .expect("round retries did not finish")
    .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].outcome, PeerSyncOutcome::Failed as i32);
    assert_eq!(results[0].attempts_used, 2);
    server.await.unwrap();

    client_replica
        .logger
        .flush_until(std::time::Instant::now() + Duration::from_secs(2))
        .unwrap();
    let events = fs::read_to_string(client.log_dir.join("sync.log"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
        .collect::<Vec<_>>();
    let sent_connections = events
        .iter()
        .filter(|event| {
            event["event"] == "sync_round_request_sent"
                && event["correlation_id"] == "round-timeout-correlation"
        })
        .map(|event| event["connection_id"].as_str().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(sent_connections.len(), 2);
    let timeout_connections = events
        .iter()
        .filter(|event| {
            event["event"] == "sync_round_progress_timeout"
                && event["correlation_id"] == "round-timeout-correlation"
                && event["failure_stage"] == "round_start_receive"
        })
        .map(|event| event["connection_id"].as_str().unwrap())
        .collect::<BTreeSet<_>>();
    assert_eq!(timeout_connections, sent_connections);

    let shutdown_deadline = Instant::now() + Duration::from_secs(3);
    client_sync.shutdown(shutdown_deadline).await.unwrap();
    client_replica.shutdown(shutdown_deadline).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_drains_an_active_round_until_its_terminal_boundary() {
    let client = SyncDeployment::new("round-shutdown-client");
    fs::write(client.root.join("ready.md"), "ready").unwrap();
    let (client_replica, client_logger) = client.start_replica().await;
    let replica_id = match client_replica.status().await {
        ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
        status => panic!("unexpected initialized state: {status:?}"),
    };
    let server_identity = NodeIdentity::generate("round-shutdown-server".parse().unwrap());
    let server_name = server_identity.node_name().clone();
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let listen = listener.local_addr().unwrap();
    let server_key = derive_noise_psk(&NetworkKey::new_for_test(vec![7; 32]));
    let (round_started, round_started_rx) = oneshot::channel();
    let (finish_round, finish_round_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut session = accept_test_session(
            &listener,
            &server_key,
            &server_identity,
            ReplicaStatus::InitializedPopulated { replica_id },
            "round-shutdown-handshake",
        )
        .await;
        let request = session.channel.receive(None).await.unwrap();
        assert!(matches!(
            request.payload,
            Some(sync_envelope::Payload::RoundRequest(_))
        ));
        round_started.send(()).unwrap();
        finish_round_rx.await.unwrap();
        session
            .channel
            .close(
                SyncCloseCode::Normal,
                "test round reached its terminal boundary",
                &request.correlation_id,
                Some(Instant::now() + Duration::from_secs(1)),
            )
            .await;
    });
    let client_sync = SyncRuntime::start(
        &client.sync_config(
            None,
            vec![ConnectUrl::from_str(&format!("oll://{listen}")).unwrap()],
        ),
        Arc::clone(&client.identities),
        Arc::clone(&client_replica),
        client_logger,
    )
    .await
    .unwrap();
    let ready_deadline = Instant::now() + Duration::from_secs(2);
    while client_sync.sessions.lock().await.is_empty() {
        assert!(
            Instant::now() < ready_deadline,
            "initial session was not ready"
        );
        sleep(Duration::from_millis(10)).await;
    }

    let synchronize = tokio::spawn({
        let client_sync = Arc::clone(&client_sync);
        async move {
            client_sync
                .synchronize(Some(&server_name), 1, "round-shutdown-correlation")
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(2), round_started_rx)
        .await
        .expect("sync round did not start")
        .unwrap();
    let shutdown = tokio::spawn({
        let client_sync = Arc::clone(&client_sync);
        async move {
            client_sync
                .shutdown(Instant::now() + Duration::from_secs(2))
                .await
        }
    });
    sleep(Duration::from_millis(100)).await;
    assert!(
        !shutdown.is_finished(),
        "shutdown cancelled the active round before its terminal boundary"
    );
    assert!(
        !synchronize.is_finished(),
        "active round returned before the peer completed it"
    );

    finish_round.send(()).unwrap();
    let results = tokio::time::timeout(Duration::from_secs(2), synchronize)
        .await
        .expect("synchronize waiter did not finish")
        .unwrap()
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].outcome, PeerSyncOutcome::Failed as i32);
    tokio::time::timeout(Duration::from_secs(2), shutdown)
        .await
        .expect("sync runtime did not drain after the round ended")
        .unwrap()
        .unwrap();
    server.await.unwrap();
    client_replica
        .shutdown(Instant::now() + Duration::from_secs(3))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn silent_ready_session_is_removed_and_reconnected_after_heartbeat_timeout() {
    silent_heartbeat_reconnects(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn silent_waiting_session_is_removed_and_reconnected_after_heartbeat_timeout() {
    silent_heartbeat_reconnects(false).await;
}
