use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unanswered_ping_expires_without_poisoning_the_session() {
    let client = SyncDeployment::new("ping-client");
    fs::write(client.root.join("shared.md"), "shared").unwrap();
    let (client_replica, client_logger) = client.start_replica().await;
    let replica_id = match client_replica.status().await {
        ReplicaStatus::InitializedPopulated { replica_id } => replica_id,
        state => panic!("unexpected client state: {state:?}"),
    };
    let server_identity = NodeIdentity::generate("ping-server".parse().unwrap());
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
                "ping-server-handshake",
                deadline,
            )
            .await
            .unwrap();
            session
                .exchange_ready("ping-server-handshake", deadline)
                .await
                .unwrap();

            let first = session.channel.receive(None).await.unwrap();
            let Some(sync_envelope::Payload::Ping(first_ping)) = first.payload else {
                panic!("expected the first ping");
            };
            sleep(Duration::from_millis(120)).await;
            session
                .channel
                .send(
                    sync_envelope::Payload::Pong(SyncPong {
                        nonce: first_ping.nonce,
                    }),
                    &first.correlation_id,
                    Some(first.message_id),
                    None,
                )
                .await
                .unwrap();
            let second = session.channel.receive(None).await.unwrap();
            let Some(sync_envelope::Payload::Ping(ping)) = second.payload else {
                panic!("expected the second ping after the first timed out");
            };
            session
                .channel
                .send(
                    sync_envelope::Payload::Pong(SyncPong { nonce: ping.nonce }),
                    &second.correlation_id,
                    Some(second.message_id),
                    None,
                )
                .await
                .unwrap();
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
        if client_sync.status().await.iter().any(|peer| {
            peer.connection_state == PeerConnectionState::Ready as i32
                && peer
                    .node
                    .as_ref()
                    .and_then(|node| node.node_name.as_ref())
                    .is_some_and(|name| name.value == "ping-server")
        }) {
            break;
        }
        assert!(
            Instant::now() < ready_deadline,
            "ping session was not ready"
        );
        sleep(Duration::from_millis(10)).await;
    }
    let server_name = server_identity.node_name().clone();
    assert!(matches!(
        client_sync
            .ping(&server_name, "unanswered-ping-correlation")
            .await,
        Err(SyncError::Unavailable(message)) if message == "sync ping timed out"
    ));
    client_sync
        .ping(&server_name, "answered-ping-correlation")
        .await
        .unwrap();
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
    assert!(events.iter().any(|event| {
        event["event"] == "sync_ping_failed"
            && event["correlation_id"] == "unanswered-ping-correlation"
    }));
    assert!(events.iter().any(|event| {
        event["event"] == "sync_ping_completed"
            && event["correlation_id"] == "answered-ping-correlation"
    }));

    let shutdown_deadline = Instant::now() + Duration::from_secs(2);
    client_sync.shutdown(shutdown_deadline).await.unwrap();
    client_replica.shutdown(shutdown_deadline).await.unwrap();
}
