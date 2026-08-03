use super::*;

#[tokio::test]
async fn chunk_staging_classifies_sequence_and_size_rejections() {
    let (mut sender, mut receiver) = test_channels().await;
    sender
        .send(
            sync_envelope::Payload::BlobTransferChunk(BlobTransferChunk {
                transfer_id: "sequence-transfer".to_owned(),
                chunk_index: 1,
                data: vec![1],
            }),
            "sequence-correlation",
            Some(7),
            None,
        )
        .await
        .unwrap();
    let sequence = receive_chunks(
        &mut receiver,
        "sequence-transfer",
        1,
        1,
        8,
        false,
        "sequence-correlation",
        7,
    )
    .await
    .unwrap_err();
    assert!(matches!(sequence, ChunkError::Sequence));
    assert_eq!(
        sequence.blob_reject_code(),
        Some(BlobTransferRejectCode::ChunkSequence)
    );

    let (mut sender, mut receiver) = test_channels().await;
    for _ in 0..2 {
        sender
            .send(
                sync_envelope::Payload::BlobTransferChunk(BlobTransferChunk {
                    transfer_id: "duplicate-transfer".to_owned(),
                    chunk_index: 0,
                    data: vec![1],
                }),
                "duplicate-correlation",
                Some(8),
                None,
            )
            .await
            .unwrap();
    }
    assert!(matches!(
        receive_chunks(
            &mut receiver,
            "duplicate-transfer",
            2,
            2,
            1,
            false,
            "duplicate-correlation",
            8,
        )
        .await,
        Err(ChunkError::Sequence)
    ));

    let (mut sender, mut receiver) = test_channels().await;
    sender
        .send(
            sync_envelope::Payload::BlobTransferComplete(BlobTransferComplete {
                transfer_id: "missing-transfer".to_owned(),
            }),
            "missing-correlation",
            Some(10),
            None,
        )
        .await
        .unwrap();
    assert!(matches!(
        receive_chunks(
            &mut receiver,
            "missing-transfer",
            1,
            1,
            1,
            false,
            "missing-correlation",
            10,
        )
        .await,
        Err(ChunkError::Sequence)
    ));

    let (sender, mut receiver) = test_channels().await;
    drop(sender);
    assert!(matches!(
        receive_chunks(
            &mut receiver,
            "interrupted-transfer",
            1,
            1,
            1,
            false,
            "interrupted-correlation",
            11,
        )
        .await,
        Err(ChunkError::Session(_))
    ));

    let (mut sender, mut receiver) = test_channels().await;
    let chunk_message_id = sender
        .send(
            sync_envelope::Payload::BlobTransferChunk(BlobTransferChunk {
                transfer_id: "size-transfer".to_owned(),
                chunk_index: 0,
                data: vec![1, 2],
            }),
            "size-correlation",
            Some(9),
            None,
        )
        .await
        .unwrap();
    assert_eq!(chunk_message_id, 1);
    sender
        .send(
            sync_envelope::Payload::BlobTransferComplete(BlobTransferComplete {
                transfer_id: "size-transfer".to_owned(),
            }),
            "size-correlation",
            Some(9),
            None,
        )
        .await
        .unwrap();
    let size = receive_chunks(
        &mut receiver,
        "size-transfer",
        3,
        1,
        8,
        false,
        "size-correlation",
        9,
    )
    .await
    .unwrap_err();
    assert!(matches!(size, ChunkError::Size));
    assert_eq!(
        size.replica_reject_code(),
        Some(ReplicaTransferRejectCode::SizeMismatch)
    );
}
