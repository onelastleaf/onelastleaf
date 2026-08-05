use super::*;

#[test]
fn duplicate_arbitration_rank_prefers_the_canonical_initiator_then_hash() {
    let preferred = (false, [9_u8; 32]);
    let nonpreferred = (true, [0_u8; 32]);
    assert!(preferred < nonpreferred);
    assert!((false, [1_u8; 32]) < (false, [2_u8; 32]));
}

#[test]
fn reconnect_backoff_is_bounded_even_after_saturation() {
    let mut backoff = INITIAL_BACKOFF;
    for _ in 0..100 {
        backoff = backoff.saturating_mul(2).min(MAXIMUM_BACKOFF);
    }
    assert_eq!(backoff, MAXIMUM_BACKOFF);
}

#[test]
fn session_failure_diagnostics_preserve_specific_causes_without_remote_text() {
    let transport = session_failure_fields(
        &SessionError::Transport(TransportError::NoiseHandshake),
        "outbound",
        "transport_handshake",
        Some("oll://peer.example:17384"),
    );
    assert_eq!(transport["direction"], "outbound");
    assert_eq!(transport["failure_stage"], "transport_handshake");
    assert_eq!(transport["failure_source"], "transport");
    assert_eq!(transport["error_code"], "noise_handshake_failed");
    assert_eq!(transport["message"], "Noise handshake failed");
    assert_eq!(transport["connect_target"], "oll://peer.example:17384");

    let local = session_failure_fields(
        &SessionError::LocalProtocol {
            code: SyncCloseCode::ReplicaMismatch,
            error_code: "replica_mismatch",
            message: "peer ReplicaId differs from the local replica",
        },
        "inbound",
        "sync_hello",
        None,
    );
    assert_eq!(local["failure_source"], "local_validation");
    assert_eq!(local["error_code"], "replica_mismatch");
    assert_eq!(local["sync_close_code"], "replica_mismatch");
    assert_eq!(
        local["message"],
        "peer ReplicaId differs from the local replica"
    );

    let remote = session_failure_fields(
        &SessionError::RemoteClosed {
            code: SyncCloseCode::SchemaMismatch,
            message: "attacker-controlled network key material".to_owned(),
        },
        "outbound",
        "sync_hello",
        Some("oll://peer.example:17384"),
    );
    assert_eq!(remote["failure_source"], "remote_close");
    assert_eq!(remote["error_code"], "schema_mismatch");
    assert_eq!(remote["sync_close_code"], "schema_mismatch");
    assert!(remote.get("message").is_none());
    assert!(!remote.to_string().contains("network key material"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_aborts_a_silent_handshake_at_the_supplied_absolute_deadline() {
    let deployment = SyncDeployment::new("shutdown-listener");
    fs::write(deployment.root.join("ready.md"), "ready").unwrap();
    let (replica, logger) = deployment.start_replica().await;
    let listen = unused_loopback_address();
    let sync = SyncRuntime::start(
        &deployment.sync_config(Some(listen), Vec::new()),
        Arc::clone(&deployment.identities),
        Arc::clone(&replica),
        logger,
    )
    .await
    .unwrap();

    let mut silent = TcpStream::connect(listen).await.unwrap();
    silent.write_all(b"OLLSYNC\x01\x00\x20").await.unwrap();
    sleep(Duration::from_millis(25)).await;
    let started = Instant::now();
    let deadline = started + Duration::from_millis(100);
    sync.shutdown(deadline).await.unwrap();
    assert!(started.elapsed() < Duration::from_millis(500));
    let mut byte = [0_u8; 1];
    let closed = tokio::time::timeout(Duration::from_millis(500), silent.read(&mut byte))
        .await
        .expect("shutdown did not close the in-progress handshake");
    assert!(matches!(closed, Ok(0) | Err(_)));

    replica
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_epoch_change_closes_an_existing_ready_session() {
    let client = SyncDeployment::new("identity-change-client");
    fs::write(client.root.join("ready.md"), "ready").unwrap();
    let (client_replica, client_logger) = client.start_replica().await;
    let replica_id = match client_replica.status().await {
        ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
        state => panic!("unexpected client state: {state:?}"),
    };
    let server_identity = NodeIdentity::generate("identity-change-server".parse().unwrap());
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let listen = listener.local_addr().unwrap();
    let server_key = derive_noise_psk(&NetworkKey::new_for_test(vec![7; 32]));
    let server = tokio::spawn({
        let server_identity = server_identity.clone();
        async move {
            let (stream, _) = listener.accept().await.unwrap();
            let deadline = Instant::now() + HANDSHAKE_DEADLINE;
            let transport = NoiseTransport::accept(stream, &server_key, deadline)
                .await
                .unwrap();
            let mut session = PendingSession::begin(
                transport,
                &server_identity,
                ReplicaStatus::InitializedPopulated { replica_id },
                "identity-change-server-handshake",
                deadline,
            )
            .await
            .unwrap();
            session
                .exchange_ready("identity-change-server-handshake", deadline)
                .await
                .unwrap();
            assert!(matches!(
                session.channel.receive(None).await,
                Err(SessionError::RemoteClosed {
                    code: SyncCloseCode::Normal,
                    ..
                })
            ));
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
    loop {
        if client_sync
            .status()
            .await
            .iter()
            .any(|peer| peer.connection_state == PeerConnectionState::Ready as i32)
        {
            break;
        }
        assert!(
            Instant::now() < ready_deadline,
            "sync session was not ready"
        );
        sleep(Duration::from_millis(10)).await;
    }

    client.identities.advance_epoch().unwrap();
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .expect("identity change did not close the ready session")
        .unwrap();
    let removed_deadline = Instant::now() + Duration::from_secs(2);
    while !client_sync.sessions.lock().await.is_empty() {
        assert!(
            Instant::now() < removed_deadline,
            "identity-invalidated session remained registered"
        );
        sleep(Duration::from_millis(10)).await;
    }

    client_sync
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    client_replica
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_hard_deadline_aborts_a_stalled_round_and_clears_its_registry_entry() {
    let client = SyncDeployment::new("shutdown-round-client");
    fs::write(client.root.join("ready.md"), "ready").unwrap();
    let (client_replica, client_logger) = client.start_replica().await;
    let replica_id = match client_replica.status().await {
        ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
        state => panic!("unexpected client state: {state:?}"),
    };
    let server_identity = NodeIdentity::generate("shutdown-round-server".parse().unwrap());
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let listen = listener.local_addr().unwrap();
    let server_key = derive_noise_psk(&NetworkKey::new_for_test(vec![7; 32]));
    let (round_started, round_started_rx) = oneshot::channel();
    let server = tokio::spawn({
        let server_identity = server_identity.clone();
        async move {
            let (stream, _) = listener.accept().await.unwrap();
            let deadline = Instant::now() + HANDSHAKE_DEADLINE;
            let transport = NoiseTransport::accept(stream, &server_key, deadline)
                .await
                .unwrap();
            let mut session = PendingSession::begin(
                transport,
                &server_identity,
                ReplicaStatus::InitializedPopulated { replica_id },
                "shutdown-server-handshake",
                deadline,
            )
            .await
            .unwrap();
            session
                .exchange_ready("shutdown-server-handshake", deadline)
                .await
                .unwrap();
            let request = session.channel.receive(None).await.unwrap();
            assert!(matches!(
                request.payload,
                Some(sync_envelope::Payload::RoundRequest(_))
            ));
            round_started.send(()).unwrap();
            assert!(session.channel.receive(None).await.is_err());
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
    loop {
        if client_sync
            .status()
            .await
            .iter()
            .any(|peer| peer.connection_state == PeerConnectionState::Ready as i32)
        {
            break;
        }
        assert!(
            Instant::now() < ready_deadline,
            "sync session was not ready"
        );
        sleep(Duration::from_millis(10)).await;
    }
    let round_runtime = Arc::clone(&client_sync);
    let server_name = server_identity.node_name().clone();
    let round = tokio::spawn(async move {
        round_runtime
            .synchronize(Some(&server_name), 1, "shutdown-round-correlation")
            .await
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(2), round_started_rx)
        .await
        .unwrap()
        .unwrap();

    let started = Instant::now();
    let shutdown = client_sync
        .shutdown(Instant::now() + Duration::from_millis(150))
        .await;
    assert!(matches!(shutdown, Err(SyncError::Unavailable(_))));
    assert!(started.elapsed() < Duration::from_secs(1));
    assert!(client_sync.sessions.lock().await.is_empty());
    let result = round.await.unwrap();
    assert_eq!(result[0].outcome, PeerSyncOutcome::Failed as i32);
    server.await.unwrap();

    client_replica
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_hard_deadline_aborts_a_stalled_bootstrap() {
    let source = SyncDeployment::new("shutdown-bootstrap-source");
    fs::write(source.root.join("ready.md"), "ready").unwrap();
    let (source_replica, source_logger) = source.start_replica().await;
    let replica_id = match source_replica.status().await {
        ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
        state => panic!("unexpected source state: {state:?}"),
    };
    let receiver_identity = NodeIdentity::generate("shutdown-bootstrap-receiver".parse().unwrap());
    let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
    let listen = listener.local_addr().unwrap();
    let receiver_key = derive_noise_psk(&NetworkKey::new_for_test(vec![7; 32]));
    let (inventory_received, inventory_received_rx) = oneshot::channel();
    let receiver = tokio::spawn({
        let receiver_identity = receiver_identity.clone();
        async move {
            let (stream, _) = listener.accept().await.unwrap();
            let deadline = Instant::now() + HANDSHAKE_DEADLINE;
            let transport = NoiseTransport::accept(stream, &receiver_key, deadline)
                .await
                .unwrap();
            let mut session = PendingSession::begin(
                transport,
                &receiver_identity,
                ReplicaStatus::Uninitialized,
                "shutdown-bootstrap-receiver-handshake",
                deadline,
            )
            .await
            .unwrap();
            assert_eq!(session.replica_id, Some(replica_id));
            session
                .exchange_ready("shutdown-bootstrap-receiver-handshake", deadline)
                .await
                .unwrap();
            loop {
                let envelope = session.channel.receive(None).await.unwrap();
                if matches!(
                    envelope.payload,
                    Some(sync_envelope::Payload::RoundInventoryComplete(_))
                ) {
                    break;
                }
            }
            inventory_received.send(()).unwrap();
            assert!(session.channel.receive(None).await.is_err());
        }
    });
    let source_sync = SyncRuntime::start(
        &source.sync_config(
            None,
            vec![ConnectUrl::from_str(&format!("oll://{listen}")).unwrap()],
        ),
        Arc::clone(&source.identities),
        Arc::clone(&source_replica),
        source_logger,
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), inventory_received_rx)
        .await
        .unwrap()
        .unwrap();

    let started = Instant::now();
    source_sync
        .shutdown(Instant::now() + Duration::from_millis(150))
        .await
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(100));
    assert!(started.elapsed() < Duration::from_secs(1));
    receiver.await.unwrap();

    source_replica
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}
