use super::*;

#[test]
fn strict_manifest_rejects_unknown_and_duplicate_fields() {
    let duplicate = br#"{
          "format":"onelastleaf-replica-snapshot",
          "format":"onelastleaf-replica-snapshot"
        }"#;
    assert!(parse_manifest(duplicate).is_err());

    let unknown = br#"{
          "format":"onelastleaf-replica-snapshot",
          "format_version":1,
          "snapshot_id":"00000000-0000-4000-8000-000000000001",
          "replica_id":"00000000-0000-4000-8000-000000000002",
          "created_at":"2026-01-01T00:00:00Z",
          "catalog":{"entry":"catalog.loro","size_bytes":0,"sha256":"0000000000000000000000000000000000000000000000000000000000000000"},
          "documents":[],
          "blobs":[],
          "unknown":true
        }"#;
    assert!(parse_manifest(unknown).is_err());

    let wrong_type = br#"{
          "format":"onelastleaf-replica-snapshot",
          "format_version":"1"
        }"#;
    assert!(parse_manifest(wrong_type).is_err());
}
