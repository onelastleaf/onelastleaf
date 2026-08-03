use super::*;

#[test]
fn loro_versions_require_sorted_unique_entries_and_round_trip() {
    let vector = vec![(7_u64, 4_i32), (2, 3)].into_iter().collect();
    let encoded = version_vector_to_proto(&vector);
    assert_eq!(encoded.entries[0].peer_id, 2);
    assert_eq!(version_vector_from_proto(Some(&encoded)).unwrap(), vector);

    let mut invalid = encoded;
    invalid.entries.reverse();
    assert!(version_vector_from_proto(Some(&invalid)).is_err());
}

#[test]
fn chunk_counts_cover_exact_and_partial_final_chunks() {
    assert_eq!(expected_chunk_count(0, 10).unwrap(), 0);
    assert_eq!(expected_chunk_count(10, 10).unwrap(), 1);
    assert_eq!(expected_chunk_count(11, 10).unwrap(), 2);
    assert!(expected_chunk_count(1, 0).is_err());
}

#[test]
fn inventory_batches_are_split_by_the_encrypted_frame_limit() {
    let version_vector = (1_u64..=500).map(|peer| (peer, 1)).collect();
    let mut objects = vec![ReplicaObjectSummary {
        object: ReplicaObject::Catalog,
        version_vector,
        frontier: Frontiers::default(),
    }];
    for _ in 0..80 {
        objects.push(ReplicaObjectSummary {
            object: ReplicaObject::Document(Uuid::new_v4()),
            version_vector: (1_u64..=500).map(|peer| (peer, 1)).collect(),
            frontier: Frontiers::default(),
        });
    }
    let inventory = ReplicaInventory {
        generation_id: Uuid::new_v4(),
        state_token: [0; 32],
        replica_id: Uuid::new_v4(),
        objects,
        blobs: BTreeMap::new(),
    };
    let round_id = Uuid::new_v4().to_string();
    let batches = inventory_batches(&inventory, &round_id, "inventory-correlation").unwrap();
    assert!(batches.len() > 1);
    assert!(batches.iter().all(|batch| inventory_batch_fits(
        &round_id,
        "inventory-correlation",
        batch
    )));
    assert_eq!(
        batches.iter().map(|batch| batch.0.len()).sum::<usize>(),
        inventory.objects.len()
    );

    let oversized = ReplicaInventory {
        generation_id: Uuid::new_v4(),
        state_token: [0; 32],
        replica_id: Uuid::new_v4(),
        objects: vec![ReplicaObjectSummary {
            object: ReplicaObject::Catalog,
            version_vector: (1_u64..=20_000).map(|peer| (peer, 1)).collect(),
            frontier: Frontiers::default(),
        }],
        blobs: BTreeMap::new(),
    };
    assert!(inventory_batches(&oversized, &round_id, "inventory-correlation").is_err());
}
