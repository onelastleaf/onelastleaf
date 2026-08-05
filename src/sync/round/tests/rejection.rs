use super::*;

#[tokio::test]
async fn blob_hash_mismatch_receives_a_typed_rejection() {
    let (mut sender, mut receiver) = test_channels().await;
    let transfer_id = Uuid::new_v4().to_string();
    let round_id = Uuid::new_v4().to_string();
    let expected = Sha256::digest(b"good").to_vec();
    let expected_hex = crate::replica::lower_hex(&expected);
    let sender_task = async {
        let start_id = sender
            .send(
                sync_envelope::Payload::BlobTransferStart(BlobTransferStart {
                    transfer_id: transfer_id.clone(),
                    round_id: round_id.clone(),
                    sha256: expected,
                    size_bytes: 4,
                    chunk_count: 1,
                }),
                "hash-correlation",
                Some(17),
                None,
            )
            .await
            .unwrap();
        sender
            .send(
                sync_envelope::Payload::BlobTransferChunk(BlobTransferChunk {
                    transfer_id: transfer_id.clone(),
                    chunk_index: 0,
                    data: b"evil".to_vec(),
                }),
                "hash-correlation",
                Some(start_id),
                None,
            )
            .await
            .unwrap();
        sender
            .send(
                sync_envelope::Payload::BlobTransferComplete(BlobTransferComplete {
                    transfer_id: transfer_id.clone(),
                }),
                "hash-correlation",
                Some(start_id),
                None,
            )
            .await
            .unwrap();
        sender.receive(None).await.unwrap()
    };
    let receiver_task = receive_blob_transfer(
        &mut receiver,
        &expected_hex,
        4,
        &round_id,
        8,
        "hash-correlation",
        17,
    );
    let (rejection, result) = tokio::join!(sender_task, receiver_task);
    assert!(matches!(
        result,
        Err(RoundError::Protocol("blob transfer SHA-256 mismatch"))
    ));
    let Some(sync_envelope::Payload::BlobTransferReject(rejection)) = rejection.payload else {
        panic!("expected BlobTransferReject");
    };
    assert_eq!(rejection.transfer_id, transfer_id);
    assert_eq!(rejection.code, BlobTransferRejectCode::HashMismatch as i32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_and_incomplete_loro_payloads_receive_typed_rejections() {
    let deployment = TempDir::new().unwrap();
    let root = deployment.path().join("working");
    let config_root = deployment.path().join("config");
    let log_dir = deployment.path().join("logs");
    fs::create_dir(&root).unwrap();
    fs::create_dir(&config_root).unwrap();
    let identity = NodeIdentity::generate("decode-receiver".parse().unwrap());
    let identities = IdentityCoordinator::new(identity.clone());
    let logger = NodeLogger::open(&log_dir, identity, None).unwrap();
    let replica = ReplicaRuntime::start(
        config_root,
        root,
        &ReplicaStoreConfig::Sqlite {
            path: deployment.path().join("store/replica.sqlite3"),
        },
        identities,
        logger,
    )
    .await
    .unwrap();

    let (mut sender, mut receiver) = test_channels().await;
    let transfer_id = Uuid::new_v4().to_string();
    let round_id = Uuid::new_v4().to_string();
    let payload = b"not a Loro update".to_vec();
    let payload_hash = Sha256::digest(&payload).to_vec();
    let sender_task = async {
        let start_id = sender
            .send(
                sync_envelope::Payload::ReplicaTransferStart(ReplicaTransferStart {
                    transfer_id: transfer_id.clone(),
                    round_id: round_id.clone(),
                    object: Some(replica_object_to_proto(ReplicaObject::Catalog)),
                    payload_size: payload.len() as u64,
                    chunk_count: 1,
                    resulting_loro_version_vector: Some(version_vector_to_proto(
                        &VersionVector::default(),
                    )),
                    payload_sha256: payload_hash,
                }),
                "decode-correlation",
                Some(23),
                None,
            )
            .await
            .unwrap();
        sender
            .send(
                sync_envelope::Payload::ReplicaTransferChunk(ReplicaTransferChunk {
                    transfer_id: transfer_id.clone(),
                    chunk_index: 0,
                    data: payload,
                }),
                "decode-correlation",
                Some(start_id),
                None,
            )
            .await
            .unwrap();
        sender
            .send(
                sync_envelope::Payload::ReplicaTransferComplete(ReplicaTransferComplete {
                    transfer_id: transfer_id.clone(),
                }),
                "decode-correlation",
                Some(start_id),
                None,
            )
            .await
            .unwrap();
        sender.receive(None).await.unwrap()
    };
    let receiver_task = receive_replica_transfer(
        &mut receiver,
        &replica,
        ReplicaObject::Catalog,
        &round_id,
        1024,
        "decode-correlation",
        23,
        true,
    );
    let (rejection, result) = tokio::join!(sender_task, receiver_task);
    assert!(matches!(
        result,
        Err(RoundError::Protocol(
            "replica transfer Loro payload could not be decoded"
        ))
    ));
    let Some(sync_envelope::Payload::ReplicaTransferReject(rejection)) = rejection.payload else {
        panic!("expected ReplicaTransferReject");
    };
    assert_eq!(rejection.transfer_id, transfer_id);
    assert_eq!(
        rejection.code,
        ReplicaTransferRejectCode::LoroDecodeFailed as i32
    );

    let document_id = Uuid::new_v4();
    let dependency_document = LoroDoc::new();
    dependency_document.set_peer_id(42).unwrap();
    let _ = dependency_document.get_map("data");
    let content = dependency_document.get_text("content");
    content
        .update("required history", UpdateOptions::default())
        .unwrap();
    dependency_document.commit();
    let first_version = dependency_document.oplog_vv();
    content
        .update("increment without its dependency", UpdateOptions::default())
        .unwrap();
    dependency_document.commit();
    let payload = dependency_document
        .export(ExportMode::updates(&first_version))
        .unwrap();
    let resulting_version = dependency_document.oplog_vv();
    let payload_hash = Sha256::digest(&payload).to_vec();
    let (mut sender, mut receiver) = test_channels().await;
    let transfer_id = Uuid::new_v4().to_string();
    let round_id = Uuid::new_v4().to_string();
    let sender_task = async {
        let start_id = sender
            .send(
                sync_envelope::Payload::ReplicaTransferStart(ReplicaTransferStart {
                    transfer_id: transfer_id.clone(),
                    round_id: round_id.clone(),
                    object: Some(replica_object_to_proto(ReplicaObject::Document(
                        document_id,
                    ))),
                    payload_size: payload.len() as u64,
                    chunk_count: chunk_count(payload.len(), 1024).unwrap(),
                    resulting_loro_version_vector: Some(version_vector_to_proto(
                        &resulting_version,
                    )),
                    payload_sha256: payload_hash,
                }),
                "import-correlation",
                Some(29),
                None,
            )
            .await
            .unwrap();
        for (index, chunk) in payload.chunks(1024).enumerate() {
            sender
                .send(
                    sync_envelope::Payload::ReplicaTransferChunk(ReplicaTransferChunk {
                        transfer_id: transfer_id.clone(),
                        chunk_index: index as u32,
                        data: chunk.to_vec(),
                    }),
                    "import-correlation",
                    Some(start_id),
                    None,
                )
                .await
                .unwrap();
        }
        sender
            .send(
                sync_envelope::Payload::ReplicaTransferComplete(ReplicaTransferComplete {
                    transfer_id: transfer_id.clone(),
                }),
                "import-correlation",
                Some(start_id),
                None,
            )
            .await
            .unwrap();
        sender.receive(None).await.unwrap()
    };
    let receiver_task = receive_replica_transfer(
        &mut receiver,
        &replica,
        ReplicaObject::Document(document_id),
        &round_id,
        1024,
        "import-correlation",
        29,
        true,
    );
    let (rejection, result) = tokio::join!(sender_task, receiver_task);
    assert!(matches!(
        result,
        Err(RoundError::Protocol(
            "replica transfer Loro payload could not be imported"
        ))
    ));
    let Some(sync_envelope::Payload::ReplicaTransferReject(rejection)) = rejection.payload else {
        panic!("expected ReplicaTransferReject");
    };
    assert_eq!(rejection.transfer_id, transfer_id);
    assert_eq!(
        rejection.code,
        ReplicaTransferRejectCode::LoroImportFailed as i32
    );

    replica
        .shutdown(tokio::time::Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
}
