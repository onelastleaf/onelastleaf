use std::{fs, io::Write};

use tempfile::TempDir;
use uuid::Uuid;

use super::{
    initialize_from_disk,
    loro::{import_loro_doc, map_i64},
    scan_working_tree,
};

#[test]
fn initial_scan_creates_fixed_loro_roots_and_stable_objects() {
    let directory = TempDir::new().unwrap();
    fs::create_dir(directory.path().join("notes")).unwrap();
    fs::File::create(directory.path().join("notes/a.md"))
        .unwrap()
        .write_all(b"hello")
        .unwrap();
    let disk = scan_working_tree(directory.path()).unwrap();
    let change = initialize_from_disk(&disk, Uuid::new_v4(), "test-correlation").unwrap();
    let catalog =
        import_loro_doc(&change.replica.catalog_loro, change.replica.loro_peer_id).unwrap();
    assert_eq!(
        map_i64(&catalog.get_map("catalog"), "format_version").unwrap(),
        1
    );
    assert_eq!(change.replica.documents.len(), 1);
    let document = change.replica.documents.values().next().unwrap();
    let doc = import_loro_doc(&document.loro, change.replica.loro_peer_id).unwrap();
    assert_eq!(doc.get_text("content").to_string(), "hello");
    assert!(doc.get_map("data").is_empty());
}
