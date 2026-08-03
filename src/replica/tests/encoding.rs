use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrepresentable_legacy_text_is_durably_promoted_to_utf8() {
    let deployment = Deployment::new();
    fs::write(deployment.native("/legacy.txt"), b"caf\xe9").unwrap();
    let runtime = deployment.start().await;
    let before = runtime
        .inspect_document(&deployment.native("/legacy.txt"))
        .await
        .unwrap();
    assert_ne!(before.encoding, "UTF-8");

    runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "promote-legacy-encoding".to_owned(),
                preconditions: vec![document_revision_precondition(&before)],
                mutations: vec![replace_mutation("/legacy.txt", "café \u{1f343}")],
            },
            OperationSource::Plugin,
            "encoding-promotion-correlation",
        )
        .await
        .unwrap();
    let after = runtime
        .inspect_document(&deployment.native("/legacy.txt"))
        .await
        .unwrap();
    assert_eq!(after.encoding, "UTF-8");
    assert!(!after.has_byte_order_mark);
    assert_ne!(after.catalog_revision, before.catalog_revision);
    assert_eq!(
        fs::read(deployment.native("/legacy.txt")).unwrap(),
        "café \u{1f343}".as_bytes()
    );
    shutdown_runtime(&runtime).await;
    let log = fs::read_to_string(deployment.log_dir.join("oll.log")).unwrap();
    assert!(log.contains("\"event\":\"document_encoding_promoted\""));
    assert!(log.contains("\"correlation_id\":\"encoding-promotion-correlation\""));
    assert!(!log.contains("café"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn extended_text_encodings_survive_commit_snapshot_and_restart() {
    let deployment = Deployment::new();
    let mut utf32 = vec![0xFF, 0xFE, 0x00, 0x00];
    for character in "UTF-32 叶子\n".chars() {
        utf32.extend_from_slice(&u32::from(character).to_le_bytes());
    }
    fs::write(deployment.native("/utf32.txt"), utf32).unwrap();
    fs::write(
        deployment.native("/page.html"),
        "<!DOCTYPE HTML><title>oll</title>\n",
    )
    .unwrap();
    fs::write(
        deployment.native("/ebcdic.txt"),
        [0x88, 0x85, 0x93, 0x93, 0x96],
    )
    .unwrap();

    let runtime = deployment.start().await;
    let utf32_before = runtime
        .inspect_document(&deployment.native("/utf32.txt"))
        .await
        .unwrap();
    assert_eq!(utf32_before.encoding, "UTF-32LE");
    assert!(utf32_before.has_byte_order_mark);
    assert_eq!(
        runtime
            .inspect_document(&deployment.native("/page.html"))
            .await
            .unwrap()
            .media_type,
        "text/html"
    );
    assert_eq!(
        runtime
            .inspect_document(&deployment.native("/ebcdic.txt"))
            .await
            .unwrap()
            .encoding,
        "IBM037"
    );

    runtime
        .commit_documents(
            oll::CommitDocumentsRequest {
                operation_id: "replace-utf32-content".to_owned(),
                preconditions: vec![document_revision_precondition(&utf32_before)],
                mutations: vec![replace_mutation("/utf32.txt", "updated UTF-32 🍃\n")],
            },
            OperationSource::Plugin,
            "utf32-replacement-correlation",
        )
        .await
        .unwrap();
    let super::super::classification::ClassifiedFile::Text(projected) =
        super::super::classification::classify_bytes(
            fs::read(deployment.native("/utf32.txt")).unwrap(),
        )
        .unwrap()
    else {
        panic!("projected UTF-32 document became binary")
    };
    assert_eq!(projected.text, "updated UTF-32 🍃\n");
    assert_eq!(projected.encoding, "UTF-32LE");

    let snapshot = deployment._directory.path().join("encodings.ollsnap");
    runtime
        .export_snapshot(&snapshot, "encoding-snapshot-correlation")
        .await
        .unwrap();
    super::super::verify_snapshot(&snapshot).unwrap();
    shutdown_runtime(&runtime).await;
    drop(runtime);

    let restarted = deployment.start().await;
    let utf32_after = restarted
        .inspect_document(&deployment.native("/utf32.txt"))
        .await
        .unwrap();
    assert_eq!(utf32_after.encoding, "UTF-32LE");
    assert!(utf32_after.has_byte_order_mark);
    assert_eq!(
        read_content(
            restarted
                .read_document(oll::ReadDocumentRequest {
                    path: document_path("/utf32.txt"),
                    projection: oll::DocumentProjection::Content as i32,
                })
                .await
                .unwrap()
        ),
        "updated UTF-32 🍃\n"
    );
    shutdown_runtime(&restarted).await;
}
