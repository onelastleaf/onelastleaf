mod tobytes;

pub use self::tobytes::ToBytes;
use std::simd::{Simd, cmp::SimdPartialEq};
use xxhash_rust::xxh3::xxh3_64_with_seed;

const EMPTY_FINGERPRINT: u8 = 0x80;
const BFS_PROBE_BUDGET: usize = 256;

/// K is the generic type used by callers for lookups.
/// V is the value type that callers of this HashMap store and retrieve.
/// B is the number of slots in each bucket. It should be the power of 2.
/// S is the number of subtables in the HashMap. It should be the power of 2. For SIMD optimization,
/// it can only be one of 8, 16, 32, and 64.
/// H is the number of candidates hash positions per key.
// TODO: PartialEq has some problem about +0.0, -0.0 and NaN
pub struct HashMap<
    K: ToBytes + PartialEq + Copy,
    V,
    const B: usize = 8,
    const S: usize = 256,
    const H: usize = 3,
> {
    subtables: [Subtable<K, V, B>; S],
    seeds: [u64; H],
}

pub struct Subtable<K: PartialEq, V, const B: usize> {
    buckets: Vec<Bucket<K, V, B>>,
}

// TODO: Option<Slot> is not suitable for the SIMD comparison
pub struct Bucket<K: PartialEq, V, const B: usize> {
    slots: [Option<Slot<K, V>>; B],
    fingerprints: [u8; B],
}

pub struct Slot<K, V> {
    key: K,
    value: V,
}

pub enum InsertError<K, V> {
    /// The key already exists. Insertion does not overwrite existing values.
    DuplicateKey(K, V),

    /// No available slot was found within the BFS search budget.
    /// This does not necessarily mean the HashMap is full.
    NoSpace(K, V),

    /// A slot is occupied, but its fingerprint is marked as empty.
    FalseEmpty(K, V),

    /// A slot is empty, but its fingerprint is marked as occupied.
    FalseOccupied(K, V),

    /// Invariant violated: an expected BFS node is missing from the queue.
    BfsTreeError(K, V),

    /// Invariant violated: source and source_node did not become None together.
    RelocationError(K, V),
}

#[derive(Clone, Copy)]
struct BfsNode {
    bucket: BucketPos,          // who is me
    parent_node: Option<usize>, // who is my parent's bfsnode
    parent: Option<SlotPos>,    // who is my parent's bucket and slot
    fingerprint: u8,            // who is my fingerprint, if here's a empty slot, i'll insert it
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct BucketPos {
    subtable: usize,
    bucket: usize,
}

#[derive(Clone, Copy, PartialEq, Eq)]
struct SlotPos {
    bucket: BucketPos,
    slot: usize,
}

#[derive(Clone, Copy)]
struct Relocation {
    source_node: Option<usize>,
    source: Option<SlotPos>,
    target: SlotPos,
    fingerprint: u8,
}

impl<K, V> Slot<K, V> {
    fn new(key: K, value: V) -> Self {
        Self { key, value }
    }
}

impl<K: PartialEq, V, const B: usize> Bucket<K, V, B> {
    fn new() -> Self {
        Self {
            slots: [const { None }; B],
            fingerprints: [EMPTY_FINGERPRINT; B],
        }
    }

    fn lookup(&self, key: &K, fingerprint: u8) -> Option<&V> {
        let mut bitmask = Simd::<u8, B>::from_array(self.fingerprints)
            .simd_eq(Simd::splat(fingerprint))
            .to_bitmask();

        while bitmask != 0 {
            let index = bitmask.trailing_zeros() as usize;
            bitmask &= bitmask - 1;

            let Some(slot) = &self.slots[index] else {
                eprintln!("[WARN] slot should not be None. fingerprints has something wrong.");
                continue;
            };

            if key == &slot.key {
                return Some(&slot.value);
            }
        }

        None
    }

    fn insert(&mut self, slot: Slot<K, V>, fingerprint: u8) -> Result<(), InsertError<K, V>> {
        debug_assert!(fingerprint != EMPTY_FINGERPRINT);

        let Some(index) = Simd::<u8, B>::from_array(self.fingerprints)
            .simd_eq(Simd::splat(EMPTY_FINGERPRINT))
            .first_set()
        else {
            return Err(InsertError::NoSpace(slot.key, slot.value));
        };

        if self.slots[index].is_some() {
            eprintln!("[WARN] this slot is occupied, but it's empty in fingerprints");
            return Err(InsertError::FalseEmpty(slot.key, slot.value));
        };

        self.fingerprints[index] = fingerprint;
        self.slots[index] = Some(slot);
        Ok(())
    }
}

impl<K: PartialEq, V, const B: usize> Subtable<K, V, B> {
    fn new(n: usize) -> Self {
        Self {
            buckets: std::iter::repeat_with(Bucket::new).take(n).collect(),
        }
    }
}

impl<K: ToBytes + PartialEq + Copy, V, const B: usize, const S: usize, const H: usize>
    HashMap<K, V, B, S, H>
{
    fn new(buckets_per_subtable: usize) -> Self {
        const {
            assert!(B > 0 && B.is_power_of_two());
            assert!(S >= 2 && S.is_power_of_two());
            assert!(S.ilog2() + 8 <= u64::BITS);
            assert!(H > 0);
        }

        assert!(buckets_per_subtable.is_power_of_two());

        Self {
            subtables: std::array::from_fn(|_| Subtable::new(buckets_per_subtable)),
            seeds: std::array::from_fn(|_| std::random::random(..)),
        }
    }

    fn candidates(key: &K, seeds: &[u64; H]) -> [u64; H] {
        key.with_bytes(|bytes| std::array::from_fn(|i| xxh3_64_with_seed(bytes, seeds[i])))
    }

    #[inline]
    fn fingerprint(hash: u64) -> u8 {
        let shift = u64::BITS - S.ilog2() - 8;
        let fingerprint = (hash >> shift) as u8;

        if fingerprint == EMPTY_FINGERPRINT {
            EMPTY_FINGERPRINT + 1
        } else {
            fingerprint
        }
    }

    #[inline]
    fn hash2bucket(&self, hash: u64) -> (usize, usize) {
        let high_bits_number: u32 = S.ilog2();
        let subtable_index: usize = (hash >> (u64::BITS - high_bits_number)) as usize;
        let subtable_len = self.subtables[subtable_index].buckets.len();
        let bucket_index = (hash as usize) & (subtable_len - 1);
        // why (subtable_len - 1)?
        // because subtable_len is power of 2, (subtable_len - 1) has all 1 in low bits

        (subtable_index, bucket_index)
    }

    fn lookup(&self, key: &K) -> Option<&V> {
        let hash_values: [u64; H] = Self::candidates(key, &self.seeds);

        for hash in hash_values {
            let (subtable_index, bucket_index) = self.hash2bucket(hash);

            let fingerprint = Self::fingerprint(hash);
            debug_assert_ne!(fingerprint, EMPTY_FINGERPRINT);

            let result_of_this_hash: Option<&V> =
                self.subtables[subtable_index].buckets[bucket_index].lookup(key, fingerprint);

            if let Some(result) = result_of_this_hash {
                return Some(result);
            }
        }

        None
    }

    fn insert(&mut self, key: K, value: V) -> Result<(), InsertError<K, V>> {
        let hash_values: [u64; H] = Self::candidates(&key, &self.seeds);
        let mut empty_slot_index: Option<(usize, usize, usize, u8)> = None;

        for hash in hash_values {
            let (subtable_index, bucket_index) = self.hash2bucket(hash);
            let bucket = &self.subtables[subtable_index].buckets[bucket_index];

            let fingerprint = Self::fingerprint(hash);
            debug_assert_ne!(fingerprint, EMPTY_FINGERPRINT);

            let vector_fingerprints = Simd::<u8, B>::from_array(bucket.fingerprints);
            let vector_fingerprint = Simd::<u8, B>::splat(fingerprint);
            let vector_empty = Simd::<u8, B>::splat(EMPTY_FINGERPRINT);

            let mut bitmask = vector_fingerprints.simd_eq(vector_fingerprint).to_bitmask();

            while bitmask != 0 {
                let index = bitmask.trailing_zeros() as usize;
                bitmask &= bitmask - 1;

                match bucket.slots[index].as_ref() {
                    Some(slot) if slot.key == key => {
                        return Err(InsertError::DuplicateKey(key, value));
                    }
                    Some(_) => {}
                    None => {
                        eprintln!("[WARN] slot is empty, but fingerprints is occupied");
                        return Err(InsertError::FalseOccupied(key, value));
                    }
                }
            }

            if empty_slot_index.is_none()
                && let Some(index) = vector_fingerprints.simd_eq(vector_empty).first_set()
            {
                if bucket.slots[index].is_some() {
                    eprintln!("[WARN] slot is occupied, but fingerprints is empty");
                    return Err(InsertError::FalseEmpty(key, value));
                } else {
                    let fingerprint = Self::fingerprint(hash);
                    empty_slot_index = Some((subtable_index, bucket_index, index, fingerprint));
                }
            }
        }

        let slot: Slot<K, V> = Slot::new(key, value);

        if let Some(indexs) = empty_slot_index {
            self.subtables[indexs.0].buckets[indexs.1].fingerprints[indexs.2] = indexs.3;
            self.subtables[indexs.0].buckets[indexs.1].slots[indexs.2] = Some(slot);
            Ok(())
        } else {
            self.bfs_displacement(slot, hash_values)
        }
    }

    fn bfs_displacement(
        &mut self,
        new_slot: Slot<K, V>,
        hash_values: [u64; H],
    ) -> Result<(), InsertError<K, V>> {
        let mut bfs_queue: [Option<BfsNode>; BFS_PROBE_BUDGET] = [None; BFS_PROBE_BUDGET];

        let mut head = 0usize;
        let mut tail = 0usize;

        // insert the basic H bucket into bfs_queue
        for init_hash in hash_values {
            let (init_subtable_index, init_bucket_index) = self.hash2bucket(init_hash);
            let init_bucket_pos = BucketPos {
                subtable: init_subtable_index,
                bucket: init_bucket_index,
            };

            let init_fingerprint = Self::fingerprint(init_hash);

            // although these hashes are all from 1 key, their buckets may be duplicated
            if bfs_queue[..tail]
                .iter()
                .flatten()
                .any(|node| node.bucket == init_bucket_pos)
            {
                continue;
            }

            // although `bfs_queue` length < H is very impossible, but I must deal with this thing
            if tail == BFS_PROBE_BUDGET {
                return Err(InsertError::NoSpace(new_slot.key, new_slot.value));
            }

            bfs_queue[tail] = Some(BfsNode {
                bucket: init_bucket_pos,
                parent_node: None,
                parent: None,
                fingerprint: init_fingerprint,
            });
            tail += 1;
        }

        // if successful, find the final empty slot, restore its position, and "how to move it"
        let mut found: Option<(BfsNode, SlotPos)> = None;

        // bfs search the empty slot
        'search: while head < tail {
            // take the current bucket
            let Some(current_bfsnode) = bfs_queue[head] else {
                return Err(InsertError::BfsTreeError(new_slot.key, new_slot.value));
            };
            let current_bucketpos = current_bfsnode.bucket;
            let bucket =
                &self.subtables[current_bucketpos.subtable].buckets[current_bucketpos.bucket];

            // iter current bucket's every slots
            // cause every bucket in `bfs_queue` is checked full. I don't check double times
            for current_slot_index in 0..B {
                // I hope every slot in the Bucket is `Some`.
                // If `None`, it must be something wrong. Either fingerprints is wrong, or I found
                // the empty slot.
                // To be honest, even if the fingerprints are incorrect, we can still find the empty
                // slot, but I don't think this is a error that can be ignored.
                let Some(current_slot) = bucket.slots[current_slot_index].as_ref() else {
                    if bucket.fingerprints[current_slot_index] != EMPTY_FINGERPRINT {
                        return Err(InsertError::FalseOccupied(new_slot.key, new_slot.value));
                    };

                    found = Some((
                        bfs_queue[head].unwrap(),
                        SlotPos {
                            bucket: current_bucketpos,
                            slot: current_slot_index,
                        },
                    ));
                    break 'search;
                };
                // I know there's another FalseEmpty error I haven't covered. This is not a bug——I
                // just don't want to cover it. The fingerprint and slot state should already match.
                // If an empty slot is found, checking the fingerprint once at the end is cheap: the
                // fingerprint is about to be relocated anyway, so loading it into hot CPU cache
                // costs little. But checking the fingerprint every time no empty slot is found
                // would be completely unnecessary.

                // For every candidates buckets per KV in current slot, there may be some empty
                // slots in these buckets.
                // We need to search them out :)
                let hashes = Self::candidates(&current_slot.key, &self.seeds);

                for hash in hashes {
                    // From hash to bucket
                    let (target_subtable_index, target_bucket_index) = self.hash2bucket(hash);
                    let target_bucket_pos = BucketPos {
                        subtable: target_subtable_index,
                        bucket: target_bucket_index,
                    };
                    let target_bucket =
                        &self.subtables[target_subtable_index].buckets[target_bucket_index];

                    // First, the bucket being traversed must not be the "current bucket", since
                    // each KV has H candidate positions, and this position may naturally be one of them.
                    // Second, we also don't want the current bucket to have already been searched
                    // earlier, so we need to prune it.
                    if target_bucket_pos == current_bucketpos
                        || bfs_queue[..tail]
                            .iter()
                            .flatten()
                            .any(|node| node.bucket == target_bucket_pos)
                    {
                        continue;
                    }

                    // Build a BfsNode first so it can be used either way: if an empty slot is found
                    // in this bucket, construct `found` and return; otherwise, push the BfsNode into
                    // `bfs_queue`
                    let next = BfsNode {
                        bucket: target_bucket_pos,
                        parent_node: Some(head),
                        parent: Some(SlotPos {
                            bucket: current_bucketpos,
                            slot: current_slot_index,
                        }),
                        fingerprint: Self::fingerprint(hash), // new bucket, new fingerprint
                    };

                    // Core step: find empty slot in the bucket
                    // I hope this fingerprints has some EMPTY_FINGERPRINT.
                    // If not, we have to push it into `bfs_queue`.
                    let Some(target_slot_index) = Simd::<u8, B>::from_array(
                        self.subtables[target_subtable_index].buckets[target_bucket_index]
                            .fingerprints,
                    )
                    .simd_eq(Simd::splat(EMPTY_FINGERPRINT))
                    .first_set() else {
                        // Sadly, we found nothing, and have to push it into queue.
                        if tail < BFS_PROBE_BUDGET {
                            bfs_queue[tail] = Some(next);
                            tail += 1;
                        };
                        continue;
                    };

                    if target_bucket.slots[target_slot_index].is_some() {
                        return Err(InsertError::FalseEmpty(new_slot.key, new_slot.value));
                    }

                    // If this slot isn't `Some(...)`, it must be `None`.
                    // We found it!
                    found = Some((
                        next,
                        SlotPos {
                            bucket: target_bucket_pos,
                            slot: target_slot_index,
                        },
                    ));
                    break 'search;
                }
            }

            head += 1;
        }

        // We've searched all possible places. Now let's reveal the result:
        // did we actually find an empty slot?

        // Bad End: no, we found nothing, so we must return `NoSpace` error.
        if found.is_none() {
            return Err(InsertError::NoSpace(new_slot.key, new_slot.value));
        }

        // Good End: yes, we found the empty slot!
        // Now we must perform the final step: execute the reverse relocation.
        // But wait, what is `found`? How did it tell us "how to relocate"?
        // Every relocation is three steps:
        // - Who am I?
        // - Where do I come from?
        // - Where am I going?
        //
        // Who am I? We must resolve the ownership issue.
        // Where do I come from? `bfsnode.source`
        // Where am i going? `bfsnode.bucket` + `slotpos` + `bfsnode.fingerprint`
        let (init_empty_slot_bfsnode, init_empty_slot_slotpos) = found.unwrap();

        let mut current_relocation = Relocation {
            source_node: init_empty_slot_bfsnode.parent_node,
            source: init_empty_slot_bfsnode.parent,
            target: init_empty_slot_slotpos,
            fingerprint: init_empty_slot_bfsnode.fingerprint,
        };

        loop {
            match (current_relocation.source_node, current_relocation.source) {
                (Some(next_bfsnode_index), Some(source)) => {
                    // Maybe the parent bfsnode is `None`? If happened, it must covered before relocation.
                    let Some(next_bfsnode) = bfs_queue[next_bfsnode_index] else {
                        return Err(InsertError::FalseEmpty(new_slot.key, new_slot.value));
                    };

                    // Move the source KV, and make the fingerprint empty
                    let source_bucket =
                        &mut self.subtables[source.bucket.subtable].buckets[source.bucket.bucket];
                    source_bucket.fingerprints[source.slot] = EMPTY_FINGERPRINT;
                    let moved_slot = source_bucket.slots[source.slot]
                        .take()
                        .expect("BFS source slot unexpectedly empty");

                    // Move the KV into target empty slot, and update the fingerprint
                    let target_pos = current_relocation.target;
                    let target_bucket = &mut self.subtables[target_pos.bucket.subtable].buckets
                        [target_pos.bucket.bucket];
                    target_bucket.fingerprints[target_pos.slot] = current_relocation.fingerprint;
                    target_bucket.slots[target_pos.slot] = Some(moved_slot);

                    // After finishing moving, update current_relocation confirm next loop
                    let hole = source;

                    current_relocation.source_node = next_bfsnode.parent_node;
                    current_relocation.source = next_bfsnode.parent;
                    current_relocation.target = hole;
                    current_relocation.fingerprint = next_bfsnode.fingerprint;
                }
                (Some(_), None) => {
                    return Err(InsertError::RelocationError(new_slot.key, new_slot.value));
                }
                (None, Some(_)) => {
                    return Err(InsertError::RelocationError(new_slot.key, new_slot.value));
                }
                (None, None) => {
                    // Now we have a empty slot in root bucket, so we can perform final insertion
                    let hole = current_relocation.target;

                    let bucket =
                        &mut self.subtables[hole.bucket.subtable].buckets[hole.bucket.bucket];

                    debug_assert!(bucket.slots[hole.slot].is_none());

                    bucket.slots[hole.slot] = Some(new_slot);
                    bucket.fingerprints[hole.slot] = current_relocation.fingerprint;

                    return Ok(());
                }
            }
        }
    }

    fn resize(&mut self) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, HashSet};

    // B=8: a supported SIMD lane count; S=8: a small, easy-to-control table.
    type TestMap<const H: usize> = HashMap<u64, i32, 8, 8, H>;

    fn seeded_map<const H: usize>(buckets_per_subtable: usize) -> TestMap<H> {
        let mut map = TestMap::<H>::new(buckets_per_subtable);
        map.seeds = std::array::from_fn(|i| 0x9E37_79B9_7F4A_7C15_u64.wrapping_mul(i as u64 + 1));
        map
    }

    fn snapshot<const H: usize>(map: &TestMap<H>) -> Vec<(u8, Option<(u64, i32)>)> {
        map.subtables
            .iter()
            .flat_map(|subtable| subtable.buckets.iter())
            .flat_map(|bucket| {
                (0..8).map(move |i| {
                    (
                        bucket.fingerprints[i],
                        bucket.slots[i].as_ref().map(|slot| (slot.key, slot.value)),
                    )
                })
            })
            .collect()
    }

    fn occupied_count<const H: usize>(map: &TestMap<H>) -> usize {
        snapshot(map)
            .iter()
            .filter(|(_, slot)| slot.is_some())
            .count()
    }

    // Checks both metadata consistency and the hash-location invariant.
    fn assert_storage_invariants<const H: usize>(map: &TestMap<H>) {
        let mut seen_keys = HashSet::new();

        for (si, subtable) in map.subtables.iter().enumerate() {
            for (bi, bucket) in subtable.buckets.iter().enumerate() {
                for index in 0..8 {
                    let fingerprint = bucket.fingerprints[index];
                    let slot = &bucket.slots[index];
                    assert_eq!(
                        slot.is_none(),
                        fingerprint == EMPTY_FINGERPRINT,
                        "slot/fingerprint mismatch at ({si}, {bi}, {index})"
                    );

                    if let Some(slot) = slot {
                        assert!(seen_keys.insert(slot.key), "duplicate key: {}", slot.key);

                        let in_correct_position = TestMap::<H>::candidates(&slot.key, &map.seeds)
                            .into_iter()
                            .any(|hash| {
                                map.hash2bucket(hash) == (si, bi)
                                    && TestMap::<H>::fingerprint(hash) == fingerprint
                            });

                        assert!(
                            in_correct_position,
                            "key {} is stored outside its candidate buckets, or has the wrong fingerprint",
                            slot.key
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn empty_map_and_basic_insert_lookup() {
        let mut map = seeded_map::<2>(2);
        assert_eq!(map.lookup(&10), None);
        assert_eq!(occupied_count(&map), 0);

        assert!(map.insert(10, 100).is_ok());
        assert!(map.insert(20, 200).is_ok());

        assert_eq!(map.lookup(&10), Some(&100));
        assert_eq!(map.lookup(&20), Some(&200));
        assert_eq!(map.lookup(&30), None);
        assert_eq!(occupied_count(&map), 2);
        assert_storage_invariants(&map);
    }

    #[test]
    fn bucket_lookup_checks_keys_after_fingerprint_collision() {
        let mut bucket = Bucket::<u64, i32, 8>::new();
        assert!(bucket.insert(Slot::new(1, 10), 7).is_ok());
        assert!(bucket.insert(Slot::new(2, 20), 7).is_ok());

        assert_eq!(bucket.lookup(&1, 7), Some(&10));
        assert_eq!(bucket.lookup(&2, 7), Some(&20));
        assert_eq!(bucket.lookup(&3, 7), None);
        assert_eq!(bucket.lookup(&1, 8), None);

        for key in 10_u64..16 {
            assert!(bucket.insert(Slot::new(key, key as i32), 7).is_ok());
        }
        assert!(matches!(
            bucket.insert(Slot::new(999, 9), 7),
            Err(InsertError::NoSpace(999, 9))
        ));
    }

    #[test]
    fn duplicate_insert_preserves_original_value_and_layout() {
        let mut map = seeded_map::<2>(2);
        assert!(map.insert(42, 7).is_ok());
        let before = snapshot(&map);

        assert!(matches!(
            map.insert(42, 999),
            Err(InsertError::DuplicateKey(42, 999))
        ));
        assert_eq!(map.lookup(&42), Some(&7));
        assert_eq!(snapshot(&map), before);
        assert_storage_invariants(&map);
    }

    #[test]
    fn fingerprint_never_uses_empty_sentinel() {
        let shift = u64::BITS - 8_usize.ilog2() - 8;
        for raw in 0..=u8::MAX {
            let hash = (raw as u64) << shift;
            let expected = if raw == EMPTY_FINGERPRINT {
                EMPTY_FINGERPRINT + 1
            } else {
                raw
            };
            assert_eq!(TestMap::<2>::fingerprint(hash), expected);
        }
    }

    #[test]
    fn hash_to_bucket_uses_high_and_low_bits() {
        let map = seeded_map::<2>(4);
        let hash = (5_u64 << (u64::BITS - 8_usize.ilog2())) | 0b1010;
        assert_eq!(map.hash2bucket(hash), (5, 2));
    }

    #[test]
    #[should_panic]
    fn zero_buckets_per_subtable_is_rejected() {
        let _ = TestMap::<2>::new(0);
    }

    #[test]
    fn no_space_in_one_candidate_bucket_does_not_modify_map() {
        let mut map = seeded_map::<1>(1);
        let first_hash = TestMap::<1>::candidates(&0, &map.seeds)[0];
        let target = map.hash2bucket(first_hash);

        let keys: Vec<u64> = (0_u64..10_000)
            .filter(|key| {
                let hash = TestMap::<1>::candidates(key, &map.seeds)[0];
                map.hash2bucket(hash) == target
            })
            .take(9)
            .collect();
        assert_eq!(keys.len(), 9);

        for (index, &key) in keys.iter().take(8).enumerate() {
            assert!(map.insert(key, index as i32).is_ok());
        }
        let before = snapshot(&map);
        let rejected_key = keys[8];

        assert!(matches!(
            map.insert(rejected_key, 999),
            Err(InsertError::NoSpace(key, 999)) if key == rejected_key
        ));
        assert_eq!(snapshot(&map), before);
        assert_eq!(map.lookup(&rejected_key), None);
        assert_eq!(occupied_count(&map), 8); // Other subtables still have free slots.
        assert_storage_invariants(&map);
    }

    #[test]
    fn insert_detects_inconsistent_slot_fingerprints() {
        // Occupied fingerprint but no slot.
        let mut map = seeded_map::<2>(1);
        let hash = TestMap::<2>::candidates(&42, &map.seeds)[0];
        let (si, bi) = map.hash2bucket(hash);
        map.subtables[si].buckets[bi].fingerprints[0] = TestMap::<2>::fingerprint(hash);
        let before = snapshot(&map);

        assert!(matches!(
            map.insert(42, 7),
            Err(InsertError::FalseOccupied(42, 7))
        ));
        assert_eq!(snapshot(&map), before);

        // Slot exists but its fingerprint says "empty".
        let mut map = seeded_map::<2>(1);
        let hash = TestMap::<2>::candidates(&43, &map.seeds)[0];
        let (si, bi) = map.hash2bucket(hash);
        map.subtables[si].buckets[bi].slots[0] = Some(Slot::new(999, 11));
        let before = snapshot(&map);

        assert!(matches!(
            map.insert(43, 8),
            Err(InsertError::FalseEmpty(43, 8))
        ));
        assert_eq!(snapshot(&map), before);
    }

    fn bucket_pos(map: &TestMap<2>, hash: u64) -> BucketPos {
        let (subtable, bucket) = map.hash2bucket(hash);
        BucketPos { subtable, bucket }
    }

    fn candidate_positions(map: &TestMap<2>, key: u64) -> [BucketPos; 2] {
        TestMap::<2>::candidates(&key, &map.seeds).map(|hash| bucket_pos(map, hash))
    }

    // Build a valid table state directly, so the BFS test does not rely on
    // lucky insertion sequences or accidental hash collisions.
    fn place_fixture(map: &mut TestMap<2>, where_: BucketPos, key: u64) {
        let hash = TestMap::<2>::candidates(&key, &map.seeds)
            .into_iter()
            .find(|&hash| bucket_pos(map, hash) == where_)
            .expect("fixture key must hash to the chosen bucket");
        let fingerprint = TestMap::<2>::fingerprint(hash);
        let bucket = &mut map.subtables[where_.subtable].buckets[where_.bucket];
        let index = bucket
            .slots
            .iter()
            .position(|slot| slot.is_none())
            .expect("fixture bucket has room");

        bucket.slots[index] = Some(Slot::new(key, key as i32));
        bucket.fingerprints[index] = fingerprint;
    }

    fn place_matching(
        map: &mut TestMap<2>,
        where_: BucketPos,
        used: &mut HashSet<u64>,
        predicate: impl Fn([BucketPos; 2]) -> bool,
    ) {
        let key = (0_u64..100_000)
            .find(|key| {
                !used.contains(key) && {
                    let positions = candidate_positions(map, *key);
                    positions.contains(&where_) && predicate(positions)
                }
            })
            .expect("could not construct BFS fixture");

        place_fixture(map, where_, key);
        used.insert(key);
    }

    #[test]
    fn bfs_relocation_can_follow_two_edges() {
        let mut map = seeded_map::<2>(1);
        let new_key = (0_u64..1_000)
            .find(|&key| {
                let positions = candidate_positions(&map, key);
                positions[0] != positions[1]
            })
            .expect("need two different candidate buckets");
        let [a, b] = candidate_positions(&map, new_key);
        let c = (0..8)
            .map(|subtable| BucketPos {
                subtable,
                bucket: 0,
            })
            .find(|&pos| pos != a && pos != b)
            .unwrap();

        let mut used = HashSet::new();
        used.insert(new_key);

        // A and B (the new key's two candidate buckets) are both full.
        // Every resident of A/B can move only within A, B, C.
        // At least one resident of A can move into C.
        place_matching(&mut map, a, &mut used, |positions| positions.contains(&c));
        for _ in 1..8 {
            place_matching(&mut map, a, &mut used, |positions| {
                positions.iter().all(|pos| [a, b, c].contains(pos))
            });
        }
        for _ in 0..8 {
            place_matching(&mut map, b, &mut used, |positions| {
                positions.iter().all(|pos| [a, b, c].contains(pos))
            });
        }

        // C is also full, but each C resident has a free candidate bucket
        // outside A/B/C. Therefore insertion needs at least two relocations.
        for _ in 0..8 {
            place_matching(&mut map, c, &mut used, |positions| {
                positions.iter().any(|pos| ![a, b, c].contains(pos))
            });
        }

        assert_eq!(occupied_count(&map), 24);
        assert_storage_invariants(&map);
        for &key in &used {
            if key != new_key {
                assert_eq!(map.lookup(&key), Some(&(key as i32)));
            }
        }

        assert!(map.insert(new_key, -999).is_ok());
        assert_eq!(map.lookup(&new_key), Some(&-999));
        assert_eq!(occupied_count(&map), 25);
        assert_storage_invariants(&map);

        for &key in &used {
            if key != new_key {
                assert_eq!(map.lookup(&key), Some(&(key as i32)));
            }
        }
        for pos in [a, b, c] {
            let bucket = &map.subtables[pos.subtable].buckets[pos.bucket];
            assert!(bucket.slots.iter().all(|slot| slot.is_some()));
        }
    }

    #[test]
    fn default_configuration_with_three_candidates_works() {
        // Uses the original defaults: B=8, S=256, H=3.
        let mut map = HashMap::<u64, i32>::new(1);
        map.seeds = [11, 22, 33];

        for key in 0_u64..64 {
            assert!(map.insert(key, key as i32).is_ok());
        }
        for key in 0_u64..64 {
            assert_eq!(map.lookup(&key), Some(&(key as i32)));
        }
        assert_eq!(map.lookup(&10_000), None);
    }

    #[test]
    fn repeated_inserts_agree_with_reference_model() {
        let mut map = seeded_map::<2>(4);
        let mut model = BTreeMap::<u64, i32>::new();
        let mut state = 0x1234_5678_9ABC_DEF0_u64;

        for step in 0..500 {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let key = state % 80;
            let value = step;

            if let Some(old_value) = model.get(&key) {
                assert!(matches!(
                    map.insert(key, value),
                    Err(InsertError::DuplicateKey(k, v)) if k == key && v == value
                ));
                assert_eq!(map.lookup(&key), Some(old_value));
            } else {
                assert!(
                    map.insert(key, value).is_ok(),
                    "insert failed for key {key}"
                );
                model.insert(key, value);
            }

            if step % 25 == 0 {
                for (&key, &value) in &model {
                    assert_eq!(map.lookup(&key), Some(&value));
                }
                assert_storage_invariants(&map);
            }
        }

        for key in 0..80 {
            assert_eq!(map.lookup(&key).copied(), model.get(&key).copied());
        }
        assert_storage_invariants(&map);
    }
}
