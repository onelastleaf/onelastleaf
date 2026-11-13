pub mod tobytes;

use self::tobytes::ToBytes;
use std::simd::{Simd, cmp::SimdPartialEq};
use xxhash_rust::xxh3::xxh3_64_with_seed;

const EMPTY_FINGERPRINT: u8 = 0x80;
const BFS_PROBE_BUDGET: usize = 256;

/// K is the generic type used by callers for lookups.
/// V is the value type that callers of this HashMap store and retrieve.
/// B is the number of slots in each bucket. It should be the power of 2. For SIMD optimization,
/// it can only be one of 8, 16, 32, and 64.
/// S is the number of subtables in the HashMap. It should be the power of 2.
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

#[derive(Debug, PartialEq, Eq)]
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

impl<K, V> InsertError<K, V> {
    fn message(&self) -> &'static str {
        match self {
            Self::DuplicateKey(..) => {
                "DuplicateKey: the key already exists; insertion does not overwrite existing values"
            }
            Self::NoSpace(..) => {
                "NoSpace: no available slot was found within the BFS search budget; the HashMap may still have free space"
            }
            Self::FalseEmpty(..) => {
                "FalseEmpty: a slot is occupied, but its fingerprint marks it as empty"
            }
            Self::FalseOccupied(..) => {
                "FalseOccupied: a slot is empty, but its fingerprint marks it as occupied"
            }
            Self::BfsTreeError(..) => {
                "BfsTreeError: an expected BFS node is missing from the search queue"
            }
            Self::RelocationError(..) => {
                "RelocationError: source and source_node have inconsistent states; both must be Some or both None"
            }
        }
    }
}

macro_rules! fail {
    ($error:expr) => {{
        let error = $error;
        let _ = crate::logging::error(module_path!(), error.message());
        return Err(error);
    }};
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
    pub fn new(buckets_per_subtable: usize) -> Self {
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

    pub fn lookup(&self, key: &K) -> Option<&V> {
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

    pub fn insert(&mut self, key: K, value: V) -> Result<(), InsertError<K, V>> {
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
                        fail!(InsertError::DuplicateKey(key, value));
                    }
                    Some(_) => {}
                    None => {
                        eprintln!("[WARN] slot is empty, but fingerprints is occupied");
                        fail!(InsertError::FalseOccupied(key, value));
                    }
                }
            }

            if empty_slot_index.is_none()
                && let Some(index) = vector_fingerprints.simd_eq(vector_empty).first_set()
            {
                if bucket.slots[index].is_some() {
                    eprintln!("[WARN] slot is occupied, but fingerprints is empty");
                    fail!(InsertError::FalseEmpty(key, value));
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
                fail!(InsertError::NoSpace(new_slot.key, new_slot.value));
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
                fail!(InsertError::BfsTreeError(new_slot.key, new_slot.value));
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
                        fail!(InsertError::FalseOccupied(new_slot.key, new_slot.value));
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
                        fail!(InsertError::FalseEmpty(new_slot.key, new_slot.value));
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
            fail!(InsertError::NoSpace(new_slot.key, new_slot.value));
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
                        fail!(InsertError::FalseEmpty(new_slot.key, new_slot.value));
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
                    fail!(InsertError::RelocationError(new_slot.key, new_slot.value));
                }
                (None, Some(_)) => {
                    fail!(InsertError::RelocationError(new_slot.key, new_slot.value));
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

    /// Doubles the selected subtable's bucket count.
    /// Returns `false` without modifying the map if the subtable index is out of
    /// bounds, or `true` after resizing.
    pub fn resize(&mut self, subtable_index: usize) -> bool {
        let Some(old_subtable_ref) = self.subtables.get(subtable_index) else {
            return false;
        };

        let old_len = old_subtable_ref.buckets.len();
        let new_len = old_len * 2;

        // Determine whether a candidate hash corresponds to a slot's
        // original bucket and fingerprint.
        //
        // This must use old_len, not self.hash2bucket(),
        // because the subtable length will change after replacement.
        let matches_old = |hash: u64, bucket_index: usize, fingerprint: u8| {
            let s = (hash >> (u64::BITS - S.ilog2())) as usize;
            let b = (hash as usize) & (old_len - 1);

            s == subtable_index && b == bucket_index && Self::fingerprint(hash) == fingerprint
        };

        // Replace the old subtable with a new, empty, doubled subtable.
        let old_subtable =
            std::mem::replace(&mut self.subtables[subtable_index], Subtable::new(new_len));

        // Migrate every KV from the old subtable.
        for (old_bucket_index, old_bucket) in old_subtable.buckets.into_iter().enumerate() {
            let old_fingerprints = old_bucket.fingerprints;

            for (slot_index, old_slot) in old_bucket.slots.into_iter().enumerate() {
                let Some(slot) = old_slot else {
                    continue;
                };

                // Find the candidate hash responsible for the old position.
                let hash = Self::candidates(&slot.key, &self.seeds)
                    .into_iter()
                    .find(|&hash| matches_old(hash, old_bucket_index, old_fingerprints[slot_index]))
                    .expect("Stored slot has no matching candidate");

                // With doubled capacity, the destination is either:
                // old_bucket_index or old_bucket_index + old_len.
                let new_bucket_index = (hash as usize) & (new_len - 1);

                // Each destination belongs to one old bucket, so preserving the
                // slot index cannot cause collisions. The fingerprint is unchanged.
                let target_bucket = &mut self.subtables[subtable_index].buckets[new_bucket_index];
                target_bucket.slots[slot_index] = Some(slot);
                target_bucket.fingerprints[slot_index] = old_fingerprints[slot_index];
            }
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{BTreeMap, BTreeSet};

    type RelocationMap = HashMap<u64, String, 1, 2, 2>;

    fn new_test_map<const B: usize, const S: usize, const H: usize>(
        buckets_per_subtable: usize,
        seeds: [u64; H],
    ) -> HashMap<u64, String, B, S, H> {
        let mut map = HashMap::new(buckets_per_subtable);
        map.seeds = seeds;
        map
    }

    fn assert_encoding<Value: ToBytes + ?Sized>(value: &Value, expected_bytes: &[u8]) {
        Value::with_bytes(value, |bytes| {
            assert_eq!(bytes, expected_bytes);
        });
    }

    fn assert_key_round_trips<Key: ToBytes + PartialEq + Copy + std::fmt::Debug>(keys: &[Key]) {
        let mut map = HashMap::<Key, usize, 8, 4, 3>::new(8);
        map.seeds = [11, 29, 47];

        for (key_index, &key) in keys.iter().enumerate() {
            assert_eq!(map.insert(key, key_index), Ok(()));
        }
        for (key_index, key) in keys.iter().enumerate() {
            assert_eq!(map.lookup(key), Some(&key_index));
        }
    }

    fn assert_bucket_lookup_for_width<const B: usize>() {
        let mut bucket = Bucket::<usize, usize, B>::new();
        for slot_index in 0..B {
            bucket.slots[slot_index] = Some(Slot::new(slot_index, slot_index + 100));
            bucket.fingerprints[slot_index] = 0xff;
        }
        for slot_index in 0..B {
            assert_eq!(bucket.lookup(&slot_index, 0xff), Some(&(slot_index + 100)));
        }
        assert_eq!(bucket.lookup(&B, 0xff), None);
    }

    // Pick a key that lands in the candidate buckets needed by a test.
    // For example, a BFS relocation test may need candidates A and B, but an
    // arbitrary key might land in C and D. Try consecutive keys until one fits.
    // An illustrative search starting at key 0 could look like this:
    //   key 0 -> candidates C, D -> skip
    //   key 1 -> candidates A, C -> skip
    //   key 2 -> candidates A, B -> return 2
    // Each attempt uses this map's seeds and must match expected_buckets in order.
    // Search up to 100,000 keys starting at first_key; fail the test if none match.
    fn find_key_with_candidate_buckets<const B: usize, const S: usize, const H: usize>(
        map: &HashMap<u64, String, B, S, H>,
        expected_buckets: [(usize, usize); H],
        first_key: u64,
    ) -> u64 {
        (first_key..first_key + 100_000)
            .find(|candidate_key| {
                HashMap::<u64, String, B, S, H>::candidates(candidate_key, &map.seeds)
                    .map(|candidate_hash| map.hash2bucket(candidate_hash))
                    == expected_buckets
            })
            .expect("the fixed seeds must provide a key for the requested bucket layout")
    }

    // Build occupied buckets without invoking the insertion path under test.
    // Every fixture entry still uses a real candidate bucket and fingerprint.
    fn place_entry_in_candidate<const B: usize, const S: usize, const H: usize>(
        map: &mut HashMap<u64, String, B, S, H>,
        key: u64,
        value: String,
        candidate_index: usize,
        slot_index: usize,
    ) {
        let candidate_hash =
            HashMap::<u64, String, B, S, H>::candidates(&key, &map.seeds)[candidate_index];
        let (subtable_index, bucket_index) = map.hash2bucket(candidate_hash);
        let target_bucket = &mut map.subtables[subtable_index].buckets[bucket_index];
        assert!(target_bucket.slots[slot_index].is_none());
        target_bucket.slots[slot_index] = Some(Slot::new(key, value));
        target_bucket.fingerprints[slot_index] =
            HashMap::<u64, String, B, S, H>::fingerprint(candidate_hash);
    }

    fn assert_map_matches<const B: usize, const S: usize, const H: usize>(
        map: &HashMap<u64, String, B, S, H>,
        expected_entries: &BTreeMap<u64, String>,
    ) {
        for (expected_key, expected_value) in expected_entries {
            assert_eq!(map.lookup(expected_key), Some(expected_value));
        }

        let mut stored_keys = BTreeSet::new();
        for (subtable_index, subtable) in map.subtables.iter().enumerate() {
            for (bucket_index, bucket) in subtable.buckets.iter().enumerate() {
                for (slot_index, stored_slot) in bucket.slots.iter().enumerate() {
                    let stored_fingerprint = bucket.fingerprints[slot_index];
                    match stored_slot {
                        Some(stored_slot) => {
                            assert!(stored_keys.insert(stored_slot.key), "duplicate stored key");
                            assert_eq!(
                                expected_entries.get(&stored_slot.key),
                                Some(&stored_slot.value)
                            );
                            let candidate_hashes = HashMap::<u64, String, B, S, H>::candidates(
                                &stored_slot.key,
                                &map.seeds,
                            );
                            assert!(candidate_hashes.into_iter().any(|candidate_hash| {
                                map.hash2bucket(candidate_hash) == (subtable_index, bucket_index)
                                    && HashMap::<u64, String, B, S, H>::fingerprint(candidate_hash)
                                        == stored_fingerprint
                            }));
                        }
                        None => assert_eq!(stored_fingerprint, EMPTY_FINGERPRINT),
                    }
                }
            }
        }
        assert_eq!(stored_keys.len(), expected_entries.len());
    }

    #[test]
    fn test_to_bytes_u8() {
        assert_encoding(&0_u8, &[0]);
        assert_encoding(&0x12_u8, &[0x12]);
        assert_encoding(&u8::MAX, &[0xff]);
    }

    #[test]
    fn test_to_bytes_u16() {
        assert_encoding(&0_u16, &[0; 2]);
        assert_encoding(&0x1234_u16, &[0x34, 0x12]);
        assert_encoding(&u16::MAX, &[0xff; 2]);
    }

    #[test]
    fn test_to_bytes_u32() {
        assert_encoding(&0_u32, &[0; 4]);
        assert_encoding(&0x12345678_u32, &[0x78, 0x56, 0x34, 0x12]);
        assert_encoding(&u32::MAX, &[0xff; 4]);
    }

    #[test]
    fn test_to_bytes_u64() {
        assert_encoding(&0_u64, &[0; 8]);
        assert_encoding(
            &0x0102030405060708_u64,
            &[0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01],
        );
        assert_encoding(&u64::MAX, &[0xff; 8]);
    }

    #[test]
    fn test_to_bytes_u128() {
        assert_encoding(&0_u128, &[0; 16]);
        assert_encoding(
            &0x0102030405060708090a0b0c0d0e0f10_u128,
            &[
                0x10, 0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03,
                0x02, 0x01,
            ],
        );
        assert_encoding(&u128::MAX, &[0xff; 16]);
    }

    #[test]
    fn test_to_bytes_usize() {
        assert_encoding(&0_usize, &vec![0; size_of::<usize>()]);
        let mut expected_example_bytes = vec![0; size_of::<usize>()];
        expected_example_bytes[0] = 0x34;
        expected_example_bytes[1] = 0x12;
        assert_encoding(&0x1234_usize, &expected_example_bytes);
        assert_encoding(&usize::MAX, &vec![0xff; size_of::<usize>()]);
    }

    #[test]
    fn test_to_bytes_i8() {
        assert_encoding(&0_i8, &[0]);
        assert_encoding(&-2_i8, &[0xfe]);
        assert_encoding(&i8::MIN, &[0x80]);
        assert_encoding(&i8::MAX, &[0x7f]);
    }

    #[test]
    fn test_to_bytes_i16() {
        assert_encoding(&0_i16, &[0; 2]);
        assert_encoding(&-2_i16, &[0xfe, 0xff]);
        assert_encoding(&i16::MIN, &[0, 0x80]);
        assert_encoding(&i16::MAX, &[0xff, 0x7f]);
    }

    #[test]
    fn test_to_bytes_i32() {
        assert_encoding(&0_i32, &[0; 4]);
        assert_encoding(&-2_i32, &[0xfe, 0xff, 0xff, 0xff]);
        assert_encoding(&i32::MIN, &[0, 0, 0, 0x80]);
        assert_encoding(&i32::MAX, &[0xff, 0xff, 0xff, 0x7f]);
    }

    #[test]
    fn test_to_bytes_i64() {
        assert_encoding(&0_i64, &[0; 8]);
        assert_encoding(&-2_i64, &[0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
        assert_encoding(&i64::MIN, &[0, 0, 0, 0, 0, 0, 0, 0x80]);
        assert_encoding(&i64::MAX, &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x7f]);
    }

    #[test]
    fn test_to_bytes_i128() {
        assert_encoding(&0_i128, &[0; 16]);
        let mut expected_negative_bytes = [0xff; 16];
        expected_negative_bytes[0] = 0xfe;
        assert_encoding(&-2_i128, &expected_negative_bytes);
        let mut expected_minimum_bytes = [0; 16];
        expected_minimum_bytes[15] = 0x80;
        assert_encoding(&i128::MIN, &expected_minimum_bytes);
        let mut expected_maximum_bytes = [0xff; 16];
        expected_maximum_bytes[15] = 0x7f;
        assert_encoding(&i128::MAX, &expected_maximum_bytes);
    }

    #[test]
    fn test_to_bytes_isize() {
        assert_encoding(&0_isize, &vec![0; size_of::<isize>()]);
        let mut expected_negative_bytes = vec![0xff; size_of::<isize>()];
        expected_negative_bytes[0] = 0xfe;
        assert_encoding(&-2_isize, &expected_negative_bytes);
        let mut expected_minimum_bytes = vec![0; size_of::<isize>()];
        *expected_minimum_bytes.last_mut().unwrap() = 0x80;
        assert_encoding(&isize::MIN, &expected_minimum_bytes);
        let mut expected_maximum_bytes = vec![0xff; size_of::<isize>()];
        *expected_maximum_bytes.last_mut().unwrap() = 0x7f;
        assert_encoding(&isize::MAX, &expected_maximum_bytes);
    }

    #[test]
    fn test_to_bytes_f32() {
        assert_encoding(&1.5_f32, &[0, 0, 0xc0, 0x3f]);
        assert_encoding(&-2.5_f32, &[0, 0, 0x20, 0xc0]);
        assert_encoding(&f32::MIN_POSITIVE, &[0, 0, 0x80, 0]);
        assert_encoding(&f32::MAX, &[0xff, 0xff, 0x7f, 0x7f]);
    }

    #[test]
    fn test_to_bytes_f64() {
        assert_encoding(&1.5_f64, &[0, 0, 0, 0, 0, 0, 0xf8, 0x3f]);
        assert_encoding(&-2.5_f64, &[0, 0, 0, 0, 0, 0, 0x04, 0xc0]);
        assert_encoding(&f64::MIN_POSITIVE, &[0, 0, 0, 0, 0, 0, 0x10, 0]);
        assert_encoding(&f64::MAX, &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xef, 0x7f]);
    }

    #[test]
    fn test_to_bytes_str() {
        assert_encoding("", b"");
        assert_encoding("hello\0world", b"hello\0world");
        assert_encoding("你好🦀", b"\xe4\xbd\xa0\xe5\xa5\xbd\xf0\x9f\xa6\x80");
    }

    #[test]
    fn test_to_bytes_string() {
        assert_encoding(&String::new(), b"");
        assert_encoding(&String::from("hello\0world"), b"hello\0world");
        assert_encoding(
            &String::from("你好🦀"),
            b"\xe4\xbd\xa0\xe5\xa5\xbd\xf0\x9f\xa6\x80",
        );
    }

    #[test]
    fn test_to_bytes_references() {
        let integer = 0x1234_u16;
        let integer_reference = &integer;
        assert_encoding(&integer_reference, &[0x34, 0x12]);
        assert_encoding(&&integer_reference, &[0x34, 0x12]);
        let string_slice = "你好";
        assert_encoding(&string_slice, b"\xe4\xbd\xa0\xe5\xa5\xbd");
        let owned_string = String::from("hello");
        assert_encoding(&&owned_string, b"hello");
    }

    #[test]
    fn test_slot_new() {
        let slot = Slot::new(String::from("key"), vec![1, 2, 3]);
        assert_eq!(slot.key, "key");
        assert_eq!(slot.value, vec![1, 2, 3]);
    }

    #[test]
    fn test_bucket_new() {
        let bucket = Bucket::<u64, String, 8>::new();
        assert!(bucket.slots.iter().all(Option::is_none));
        assert_eq!(bucket.fingerprints, [EMPTY_FINGERPRINT; 8]);
    }

    #[test]
    fn test_bucket_lookup() {
        let mut bucket = Bucket::<u64, String, 8>::new();
        assert_eq!(bucket.lookup(&10, 7), None);
        for (slot_index, key) in [(0, 10), (3, 20), (7, 30)] {
            bucket.slots[slot_index] = Some(Slot::new(key, format!("value {key}")));
            bucket.fingerprints[slot_index] = 7;
        }

        for key in [10, 20, 30] {
            assert_eq!(bucket.lookup(&key, 7), Some(&format!("value {key}")));
        }
        assert_eq!(bucket.lookup(&40, 7), None);
        assert_eq!(bucket.lookup(&10, 8), None);
    }

    #[test]
    fn test_bucket_lookup_all_supported_simd_widths() {
        assert_bucket_lookup_for_width::<1>();
        assert_bucket_lookup_for_width::<2>();
        assert_bucket_lookup_for_width::<4>();
        assert_bucket_lookup_for_width::<8>();
        assert_bucket_lookup_for_width::<16>();
        assert_bucket_lookup_for_width::<32>();
        assert_bucket_lookup_for_width::<64>();
    }

    #[test]
    fn test_subtable_new() {
        for bucket_count in [0, 1, 4] {
            let subtable = Subtable::<u64, String, 8>::new(bucket_count);
            assert_eq!(subtable.buckets.len(), bucket_count);
            for bucket in subtable.buckets {
                assert!(bucket.slots.iter().all(Option::is_none));
                assert_eq!(bucket.fingerprints, [EMPTY_FINGERPRINT; 8]);
            }
        }
    }

    #[test]
    fn test_hashmap_new() {
        let map = HashMap::<u64, String>::new(1);
        assert_eq!(map.subtables.len(), 256);
        assert_eq!(map.seeds.len(), 3);
        for subtable in &map.subtables {
            assert_eq!(subtable.buckets.len(), 1);
            for bucket in &subtable.buckets {
                assert!(bucket.slots.iter().all(Option::is_none));
                assert_eq!(bucket.fingerprints, [EMPTY_FINGERPRINT; 8]);
            }
        }
    }

    #[test]
    #[should_panic(expected = "buckets_per_subtable.is_power_of_two()")]
    fn test_hashmap_new_rejects_zero_buckets() {
        HashMap::<u64, String>::new(0);
    }

    #[test]
    #[should_panic(expected = "buckets_per_subtable.is_power_of_two()")]
    fn test_hashmap_new_rejects_non_power_of_two_buckets() {
        HashMap::<u64, String>::new(3);
    }

    #[test]
    fn test_candidates() {
        let key = 0x0102030405060708_u64;
        let seeds = [11, 29, 47];
        let key_bytes = [8, 7, 6, 5, 4, 3, 2, 1];
        let expected_hashes = seeds.map(|seed| xxh3_64_with_seed(&key_bytes, seed));
        assert_eq!(
            HashMap::<u64, (), 8, 4, 3>::candidates(&key, &seeds),
            expected_hashes
        );
        let repeated_seeds = [29, 29, 11];
        assert_eq!(
            HashMap::<u64, (), 8, 4, 3>::candidates(&key, &repeated_seeds),
            [expected_hashes[1], expected_hashes[1], expected_hashes[0]]
        );
    }

    #[test]
    fn test_candidates_string_keys() {
        for key in ["", "hello", "你好"] {
            let expected_hash = xxh3_64_with_seed(key.as_bytes(), 11);
            assert_eq!(
                HashMap::<&str, (), 8, 2, 1>::candidates(&key, &[11]),
                [expected_hash]
            );
        }
    }

    #[test]
    fn test_fingerprint() {
        for fingerprint_byte in 0_u8..=u8::MAX {
            if fingerprint_byte == EMPTY_FINGERPRINT {
                continue;
            }
            let hash = u64::from(fingerprint_byte) << 54;
            assert_eq!(
                HashMap::<u64, (), 8, 4, 3>::fingerprint(hash),
                fingerprint_byte
            );
            let hash_with_other_bits = hash | (3 << 62) | ((1 << 54) - 1);
            assert_eq!(
                HashMap::<u64, (), 8, 4, 3>::fingerprint(hash_with_other_bits),
                fingerprint_byte
            );
        }
    }

    #[test]
    fn test_fingerprint_remaps_empty_marker() {
        assert_eq!(HashMap::<u64, (), 8, 2, 1>::fingerprint(0x80 << 55), 0x81);
        assert_eq!(HashMap::<u64, (), 8, 4, 3>::fingerprint(0x80 << 54), 0x81);
        assert_eq!(HashMap::<u64, ()>::fingerprint(0x80 << 48), 0x81);
    }

    #[test]
    fn test_hash2bucket() {
        let mut map = new_test_map::<8, 4, 3>(4, [11, 29, 47]);
        map.subtables[2] = Subtable::new(8);
        map.subtables[3] = Subtable::new(1);
        for (hash, expected_bucket) in [
            (0, (0, 0)),
            (1, (0, 1)),
            (3, (0, 3)),
            (4, (0, 0)),
            ((1 << 62) | 3, (1, 3)),
            ((2 << 62) | 7, (2, 7)),
            (u64::MAX, (3, 0)),
        ] {
            assert_eq!(map.hash2bucket(hash), expected_bucket);
        }
    }

    #[test]
    fn test_lookup() {
        let mut map = new_test_map::<1, 2, 2>(4, [11, 29]);
        assert_eq!(map.lookup(&0), None);
        let stored_key = find_key_with_candidate_buckets(&map, [(0, 0), (1, 3)], 0);
        place_entry_in_candidate(&mut map, stored_key, String::from("second candidate"), 1, 0);
        assert_eq!(
            map.lookup(&stored_key).map(String::as_str),
            Some("second candidate")
        );
        assert_eq!(map.lookup(&(stored_key + 1)), None);
    }

    #[test]
    fn test_insert() {
        let mut map = new_test_map::<8, 2, 2>(1, [11, 29]);
        let inserted_key = find_key_with_candidate_buckets(&map, [(0, 0), (1, 0)], 0);
        assert_eq!(
            map.insert(inserted_key, String::from("first candidate")),
            Ok(())
        );
        let inserted_slot = map.subtables[0].buckets[0].slots[0].as_ref().unwrap();
        assert_eq!(inserted_slot.key, inserted_key);
        assert_eq!(inserted_slot.value, "first candidate");
        assert!(
            map.subtables[1].buckets[0]
                .slots
                .iter()
                .all(Option::is_none)
        );
        assert_map_matches(
            &map,
            &BTreeMap::from([(inserted_key, String::from("first candidate"))]),
        );
    }

    #[test]
    fn test_insert_rejects_duplicate_in_later_candidate() {
        let mut map = new_test_map::<1, 2, 2>(4, [11, 29]);
        let stored_key = find_key_with_candidate_buckets(&map, [(0, 0), (1, 3)], 0);
        place_entry_in_candidate(&mut map, stored_key, String::from("original"), 1, 0);

        assert_eq!(
            map.insert(stored_key, String::from("rejected")),
            Err(InsertError::DuplicateKey(
                stored_key,
                String::from("rejected")
            ))
        );
        assert!(map.subtables[0].buckets[0].slots[0].is_none());
        assert_map_matches(
            &map,
            &BTreeMap::from([(stored_key, String::from("original"))]),
        );
    }

    #[test]
    fn test_insert_distinguishes_keys_with_same_fingerprint() {
        type CollisionMap = HashMap<u64, String, 2, 2, 1>;
        let mut map = new_test_map::<2, 2, 1>(1, [11]);
        let original_key = 0_u64;
        let original_hash = CollisionMap::candidates(&original_key, &map.seeds)[0];
        let colliding_key = (1..100_000)
            .find(|candidate_key| {
                let candidate_hash = CollisionMap::candidates(candidate_key, &map.seeds)[0];
                map.hash2bucket(candidate_hash) == map.hash2bucket(original_hash)
                    && CollisionMap::fingerprint(candidate_hash)
                        == CollisionMap::fingerprint(original_hash)
            })
            .unwrap();
        place_entry_in_candidate(&mut map, original_key, String::from("original"), 0, 0);

        assert_eq!(map.insert(colliding_key, String::from("collision")), Ok(()));
        assert_map_matches(
            &map,
            &BTreeMap::from([
                (original_key, String::from("original")),
                (colliding_key, String::from("collision")),
            ]),
        );
    }

    #[test]
    fn test_insert_uses_later_candidate_when_first_is_full() {
        let mut map = new_test_map::<1, 2, 2>(4, [11, 29]);
        let stored_key = find_key_with_candidate_buckets(&map, [(0, 0), (0, 0)], 0);
        let inserted_key = find_key_with_candidate_buckets(&map, [(0, 0), (1, 3)], stored_key + 1);
        place_entry_in_candidate(&mut map, stored_key, String::from("original"), 0, 0);

        assert_eq!(
            map.insert(inserted_key, String::from("later candidate")),
            Ok(())
        );
        assert_eq!(
            map.subtables[1].buckets[3].slots[0].as_ref().unwrap().key,
            inserted_key
        );
        assert_map_matches(
            &map,
            &BTreeMap::from([
                (stored_key, String::from("original")),
                (inserted_key, String::from("later candidate")),
            ]),
        );
    }

    #[test]
    fn test_insert_fills_bucket_to_last_simd_lane() {
        let mut map = new_test_map::<64, 2, 1>(1, [11]);
        let mut expected_entries = BTreeMap::new();
        let mut first_unused_key = 0_u64;
        for expected_slot_index in 0..64 {
            let inserted_key = find_key_with_candidate_buckets(&map, [(0, 0)], first_unused_key);
            let inserted_value = format!("slot {expected_slot_index}");
            assert_eq!(map.insert(inserted_key, inserted_value.clone()), Ok(()));
            assert_eq!(
                map.subtables[0].buckets[0].slots[expected_slot_index]
                    .as_ref()
                    .unwrap()
                    .key,
                inserted_key
            );
            expected_entries.insert(inserted_key, inserted_value);
            first_unused_key = inserted_key + 1;
        }
        let rejected_key = find_key_with_candidate_buckets(&map, [(0, 0)], first_unused_key);
        assert_eq!(
            map.insert(rejected_key, String::from("rejected")),
            Err(InsertError::NoSpace(rejected_key, String::from("rejected")))
        );
        assert_eq!(map.lookup(&rejected_key), None);
        assert!(
            map.subtables[1].buckets[0]
                .slots
                .iter()
                .all(Option::is_none)
        );
        assert_map_matches(&map, &expected_entries);
    }

    #[test]
    fn test_insert_and_lookup_unsigned_keys() {
        assert_key_round_trips(&[0_u8, 1, u8::MAX]);
        assert_key_round_trips(&[0_u16, 1, u16::MAX]);
        assert_key_round_trips(&[0_u32, 1, u32::MAX]);
        assert_key_round_trips(&[0_u64, 1, u64::MAX]);
        assert_key_round_trips(&[0_u128, 1, u128::MAX]);
        assert_key_round_trips(&[0_usize, 1, usize::MAX]);
    }

    #[test]
    fn test_insert_and_lookup_signed_keys() {
        assert_key_round_trips(&[i8::MIN, -1, 0, 1, i8::MAX]);
        assert_key_round_trips(&[i16::MIN, -1, 0, 1, i16::MAX]);
        assert_key_round_trips(&[i32::MIN, -1, 0, 1, i32::MAX]);
        assert_key_round_trips(&[i64::MIN, -1, 0, 1, i64::MAX]);
        assert_key_round_trips(&[i128::MIN, -1, 0, 1, i128::MAX]);
        assert_key_round_trips(&[isize::MIN, -1, 0, 1, isize::MAX]);
    }

    #[test]
    fn test_insert_and_lookup_finite_float_keys() {
        assert_key_round_trips(&[-2.5_f32, 1.5, f32::MIN_POSITIVE, f32::MAX]);
        assert_key_round_trips(&[-2.5_f64, 1.5, f64::MIN_POSITIVE, f64::MAX]);
    }

    #[test]
    fn test_insert_and_lookup_string_keys() {
        assert_key_round_trips(&["", "hello", "hello\0world", "你好🦀"]);
        let owned_key = String::from("owned string borrowed as a key");
        assert_key_round_trips(&[&owned_key]);
        let string_slice = "nested reference";
        assert_key_round_trips(&[&string_slice]);
    }

    #[test]
    fn test_bfs_displacement() {
        let mut map = new_test_map::<1, 2, 2>(4, [11, 29]);
        let displaced_key = find_key_with_candidate_buckets(&map, [(0, 0), (0, 1)], 0);
        let inserted_key =
            find_key_with_candidate_buckets(&map, [(0, 0), (0, 0)], displaced_key + 1);
        place_entry_in_candidate(&mut map, displaced_key, String::from("displaced"), 0, 0);
        let inserted_hashes = RelocationMap::candidates(&inserted_key, &map.seeds);

        assert_eq!(
            map.bfs_displacement(
                Slot::new(inserted_key, String::from("inserted")),
                inserted_hashes
            ),
            Ok(())
        );
        assert_eq!(
            map.subtables[0].buckets[0].slots[0].as_ref().unwrap().key,
            inserted_key
        );
        assert_eq!(
            map.subtables[0].buckets[1].slots[0].as_ref().unwrap().key,
            displaced_key
        );
        let displaced_hashes = RelocationMap::candidates(&displaced_key, &map.seeds);
        assert_ne!(
            RelocationMap::fingerprint(displaced_hashes[0]),
            RelocationMap::fingerprint(displaced_hashes[1])
        );
        assert_map_matches(
            &map,
            &BTreeMap::from([
                (displaced_key, String::from("displaced")),
                (inserted_key, String::from("inserted")),
            ]),
        );
    }

    #[test]
    fn test_bfs_displacement_multiple_steps() {
        let mut map = new_test_map::<1, 2, 2>(4, [11, 29]);
        let mut expected_entries = BTreeMap::new();
        let mut first_unused_key = 0_u64;
        let mut displaced_keys = Vec::new();
        let relocation_path = [(0, 0), (0, 1), (1, 0), (1, 1)];
        for (&source_bucket, &target_bucket) in relocation_path.iter().zip(&relocation_path[1..]) {
            let displaced_key = find_key_with_candidate_buckets(
                &map,
                [source_bucket, target_bucket],
                first_unused_key,
            );
            let displaced_value = format!("displaced from {source_bucket:?}");
            place_entry_in_candidate(&mut map, displaced_key, displaced_value.clone(), 0, 0);
            expected_entries.insert(displaced_key, displaced_value);
            displaced_keys.push(displaced_key);
            first_unused_key = displaced_key + 1;
        }
        let inserted_key =
            find_key_with_candidate_buckets(&map, [(0, 0), (0, 0)], first_unused_key);

        // Exercise the public insertion path into BFS as well as reverse relocation.
        assert_eq!(map.insert(inserted_key, String::from("inserted")), Ok(()));
        expected_entries.insert(inserted_key, String::from("inserted"));
        for (displaced_key, &(target_subtable_index, target_bucket_index)) in
            displaced_keys.into_iter().zip(&relocation_path[1..])
        {
            assert_eq!(
                map.subtables[target_subtable_index].buckets[target_bucket_index].slots[0]
                    .as_ref()
                    .unwrap()
                    .key,
                displaced_key
            );
        }
        assert_map_matches(&map, &expected_entries);
    }

    #[test]
    fn test_bfs_displacement_prefers_shorter_path() {
        let mut map = new_test_map::<1, 2, 2>(4, [11, 29]);
        let first_root_key = find_key_with_candidate_buckets(&map, [(0, 0), (0, 2)], 0);
        let second_root_key =
            find_key_with_candidate_buckets(&map, [(0, 1), (0, 3)], first_root_key + 1);
        let intermediate_key =
            find_key_with_candidate_buckets(&map, [(0, 2), (1, 0)], second_root_key + 1);
        let inserted_key =
            find_key_with_candidate_buckets(&map, [(0, 0), (0, 1)], intermediate_key + 1);
        place_entry_in_candidate(&mut map, first_root_key, String::from("first root"), 0, 0);
        place_entry_in_candidate(&mut map, second_root_key, String::from("second root"), 0, 0);
        place_entry_in_candidate(
            &mut map,
            intermediate_key,
            String::from("intermediate"),
            0,
            0,
        );

        assert_eq!(map.insert(inserted_key, String::from("inserted")), Ok(()));
        assert_eq!(
            map.subtables[0].buckets[0].slots[0].as_ref().unwrap().key,
            first_root_key
        );
        assert_eq!(
            map.subtables[0].buckets[2].slots[0].as_ref().unwrap().key,
            intermediate_key
        );
        assert_eq!(
            map.subtables[0].buckets[3].slots[0].as_ref().unwrap().key,
            second_root_key
        );
        assert!(map.subtables[1].buckets[0].slots[0].is_none());
        assert_map_matches(
            &map,
            &BTreeMap::from([
                (first_root_key, String::from("first root")),
                (second_root_key, String::from("second root")),
                (intermediate_key, String::from("intermediate")),
                (inserted_key, String::from("inserted")),
            ]),
        );
    }

    #[test]
    fn test_bfs_displacement_returns_no_space_for_cycle() {
        let mut map = new_test_map::<1, 2, 2>(4, [11, 29]);
        let first_cycle_key = find_key_with_candidate_buckets(&map, [(0, 0), (0, 1)], 0);
        let second_cycle_key =
            find_key_with_candidate_buckets(&map, [(0, 1), (0, 0)], first_cycle_key + 1);
        let rejected_key =
            find_key_with_candidate_buckets(&map, [(0, 0), (0, 0)], second_cycle_key + 1);
        place_entry_in_candidate(
            &mut map,
            first_cycle_key,
            String::from("first cycle entry"),
            0,
            0,
        );
        place_entry_in_candidate(
            &mut map,
            second_cycle_key,
            String::from("second cycle entry"),
            0,
            0,
        );
        let rejected_hashes = RelocationMap::candidates(&rejected_key, &map.seeds);

        assert_eq!(
            map.bfs_displacement(
                Slot::new(rejected_key, String::from("rejected")),
                rejected_hashes
            ),
            Err(InsertError::NoSpace(rejected_key, String::from("rejected")))
        );
        assert!(map.subtables[0].buckets[2].slots[0].is_none());
        assert_eq!(map.lookup(&rejected_key), None);
        assert_map_matches(
            &map,
            &BTreeMap::from([
                (first_cycle_key, String::from("first cycle entry")),
                (second_cycle_key, String::from("second cycle entry")),
            ]),
        );
    }

    #[test]
    fn test_insert_returns_no_space_when_candidate_roots_exceed_search_budget() {
        const CANDIDATE_COUNT: usize = BFS_PROBE_BUDGET * 4;
        type WideCandidateMap = HashMap<u64, String, 1, 2, CANDIDATE_COUNT>;
        let seeds = std::array::from_fn(|seed_index| seed_index as u64);
        let mut map = new_test_map::<1, 2, CANDIDATE_COUNT>(256, seeds);
        let mut expected_entries = BTreeMap::new();
        let mut first_unused_key = 0_u64;

        // Fill each bucket using its first candidate, without requiring BFS.
        for candidate_key in 0_u64..20_000 {
            let first_hash = xxh3_64_with_seed(&candidate_key.to_le_bytes(), seeds[0]);
            let (subtable_index, bucket_index) = map.hash2bucket(first_hash);
            if map.subtables[subtable_index].buckets[bucket_index].slots[0].is_none() {
                let candidate_value = format!("value {candidate_key}");
                place_entry_in_candidate(&mut map, candidate_key, candidate_value.clone(), 0, 0);
                expected_entries.insert(candidate_key, candidate_value);
            }
            if expected_entries.len() == 512 {
                first_unused_key = candidate_key + 1;
                break;
            }
        }
        assert_eq!(expected_entries.len(), 512);
        let distinct_roots: BTreeSet<_> = WideCandidateMap::candidates(&first_unused_key, &seeds)
            .map(|candidate_hash| map.hash2bucket(candidate_hash))
            .into_iter()
            .collect();
        assert!(distinct_roots.len() > BFS_PROBE_BUDGET);

        assert_eq!(
            map.insert(first_unused_key, String::from("rejected")),
            Err(InsertError::NoSpace(
                first_unused_key,
                String::from("rejected")
            ))
        );
        assert_eq!(map.lookup(&first_unused_key), None);
        assert_map_matches(&map, &expected_entries);
    }

    #[test]
    fn test_resize() {
        let mut map = new_test_map::<8, 2, 1>(1, [11]);
        assert!(map.resize(0));
        assert_eq!(map.subtables[0].buckets.len(), 2);
        assert_eq!(map.subtables[1].buckets.len(), 1);
        assert_eq!(map.seeds, [11]);
        assert_map_matches(&map, &BTreeMap::new());
    }

    #[test]
    fn test_resize_rejects_invalid_index_without_changes() {
        let mut map = new_test_map::<8, 2, 1>(1, [11]);
        assert_eq!(map.insert(42, String::from("original")), Ok(()));
        for invalid_subtable_index in [2, usize::MAX] {
            assert!(!map.resize(invalid_subtable_index));
            assert_eq!(
                map.subtables
                    .each_ref()
                    .map(|subtable| subtable.buckets.len()),
                [1, 1]
            );
            assert_eq!(map.seeds, [11]);
            assert_map_matches(&map, &BTreeMap::from([(42, String::from("original"))]));
        }
    }

    #[test]
    fn test_resize_splits_full_bucket_and_preserves_slot_indices() {
        let mut map = new_test_map::<8, 2, 1>(1, [11]);
        let mut expected_entries = BTreeMap::new();
        let mut split_entries = Vec::new();
        let mut first_unused_key = 0_u64;
        for source_slot_index in 0..8 {
            let expected_bucket_index = source_slot_index % 2;
            let stored_key = (first_unused_key..first_unused_key + 100_000)
                .find(|candidate_key| {
                    let candidate_hash = xxh3_64_with_seed(&candidate_key.to_le_bytes(), 11);
                    candidate_hash >> 63 == 0 && candidate_hash & 1 == expected_bucket_index as u64
                })
                .unwrap();
            let stored_value = format!("source slot {source_slot_index}");
            place_entry_in_candidate(
                &mut map,
                stored_key,
                stored_value.clone(),
                0,
                source_slot_index,
            );
            expected_entries.insert(stored_key, stored_value);
            split_entries.push((stored_key, expected_bucket_index, source_slot_index));
            first_unused_key = stored_key + 1;
        }
        let untouched_key = find_key_with_candidate_buckets(&map, [(1, 0)], first_unused_key);
        place_entry_in_candidate(&mut map, untouched_key, String::from("untouched"), 0, 7);
        expected_entries.insert(untouched_key, String::from("untouched"));
        let original_fingerprints = map.subtables[0].buckets[0].fingerprints;

        assert!(map.resize(0));
        for (stored_key, expected_bucket_index, original_slot_index) in split_entries {
            let destination_bucket = &map.subtables[0].buckets[expected_bucket_index];
            assert_eq!(
                destination_bucket.slots[original_slot_index]
                    .as_ref()
                    .unwrap()
                    .key,
                stored_key
            );
            assert_eq!(
                destination_bucket.fingerprints[original_slot_index],
                original_fingerprints[original_slot_index]
            );
            assert!(
                map.subtables[0].buckets[1 - expected_bucket_index].slots[original_slot_index]
                    .is_none()
            );
        }
        assert_eq!(map.subtables[1].buckets.len(), 1);
        assert_eq!(
            map.subtables[1].buckets[0].slots[7].as_ref().unwrap().key,
            untouched_key
        );
        assert_map_matches(&map, &expected_entries);
    }

    #[test]
    fn test_resize_uses_stored_fingerprint_to_select_candidate() {
        let mut map = new_test_map::<1, 2, 2>(1, [11, 29]);
        let stored_key = (0..100_000)
            .find(|candidate_key| {
                let candidate_hashes = RelocationMap::candidates(candidate_key, &map.seeds);
                candidate_hashes.map(|candidate_hash| map.hash2bucket(candidate_hash))
                    == [(0, 0); 2]
                    && RelocationMap::fingerprint(candidate_hashes[0])
                        != RelocationMap::fingerprint(candidate_hashes[1])
                    && (candidate_hashes[0] ^ candidate_hashes[1]) & 1 != 0
            })
            .unwrap();
        let stored_hashes = RelocationMap::candidates(&stored_key, &map.seeds);
        let expected_bucket_index = (stored_hashes[1] & 1) as usize;
        place_entry_in_candidate(
            &mut map,
            stored_key,
            String::from("second fingerprint"),
            1,
            0,
        );

        assert!(map.resize(0));
        assert_eq!(
            map.subtables[0].buckets[expected_bucket_index].slots[0]
                .as_ref()
                .unwrap()
                .key,
            stored_key
        );
        assert!(map.subtables[0].buckets[1 - expected_bucket_index].slots[0].is_none());
        assert_map_matches(
            &map,
            &BTreeMap::from([(stored_key, String::from("second fingerprint"))]),
        );
    }

    #[test]
    fn test_resize_multiple_subtables_with_existing_entries() {
        let mut map = new_test_map::<4, 4, 1>(2, [11]);
        let mut expected_entries = BTreeMap::new();
        let mut first_unused_key = 0_u64;
        for subtable_index in 0..4 {
            for bucket_index in 0..2 {
                for slot_index in [0, 3] {
                    let stored_key = find_key_with_candidate_buckets(
                        &map,
                        [(subtable_index, bucket_index)],
                        first_unused_key,
                    );
                    let stored_value = format!("value {stored_key}");
                    place_entry_in_candidate(
                        &mut map,
                        stored_key,
                        stored_value.clone(),
                        0,
                        slot_index,
                    );
                    expected_entries.insert(stored_key, stored_value);
                    first_unused_key = stored_key + 1;
                }
            }
        }

        let mut expected_bucket_counts = [2; 4];
        for selected_subtable_index in [3, 1, 3, 0, 2] {
            assert!(map.resize(selected_subtable_index));
            expected_bucket_counts[selected_subtable_index] *= 2;
            assert_eq!(
                map.subtables
                    .each_ref()
                    .map(|subtable| subtable.buckets.len()),
                expected_bucket_counts
            );
            assert_eq!(map.seeds, [11]);
            assert_map_matches(&map, &expected_entries);
        }
    }

    #[test]
    fn test_resize_preserves_value_ownership() {
        use std::cell::Cell;
        use std::rc::Rc;

        struct TrackedValue {
            drop_count: Rc<Cell<usize>>,
        }

        impl Drop for TrackedValue {
            fn drop(&mut self) {
                self.drop_count.set(self.drop_count.get() + 1);
            }
        }

        let drop_count = Rc::new(Cell::new(0));
        let mut map = HashMap::<u64, TrackedValue, 8, 2, 1>::new(1);
        map.seeds = [11];
        for inserted_key in [0, 1, 2] {
            assert!(
                map.insert(
                    inserted_key,
                    TrackedValue {
                        drop_count: Rc::clone(&drop_count)
                    }
                )
                .is_ok()
            );
        }

        for selected_subtable_index in [0, 1, 0, 1] {
            assert!(map.resize(selected_subtable_index));
            assert_eq!(drop_count.get(), 0);
            for stored_key in [0, 1, 2] {
                let stored_value = map.lookup(&stored_key).unwrap();
                assert!(Rc::ptr_eq(&stored_value.drop_count, &drop_count));
            }
        }
        drop(map);
        assert_eq!(drop_count.get(), 3);
    }

    #[test]
    fn test_insert_and_lookup_read_heavy_workload() {
        let mut map = new_test_map::<8, 4, 3>(32, [11, 29, 47]);
        let mut expected_entries = BTreeMap::new();
        let mut write_count = 0;
        let mut read_count = 0;

        for inserted_key in 0_u64..64 {
            let inserted_value = format!("value {inserted_key}");
            assert_eq!(map.insert(inserted_key, inserted_value.clone()), Ok(()));
            expected_entries.insert(inserted_key, inserted_value);
            write_count += 1;

            for read_index in 0..10 {
                let existing_key = (inserted_key + read_index) % (inserted_key + 1);
                assert_eq!(
                    map.lookup(&existing_key),
                    expected_entries.get(&existing_key)
                );
                read_count += 1;
            }
            for missing_key in [inserted_key + 1, u64::MAX] {
                assert_eq!(map.lookup(&missing_key), None);
                read_count += 1;
            }
        }

        assert_eq!(read_count, write_count * 12);
        assert!(
            map.subtables
                .iter()
                .all(|subtable| subtable.buckets.len() == 32)
        );
        assert_map_matches(&map, &expected_entries);
    }

    #[test]
    fn test_insert_lookup_and_caller_scheduled_resize() {
        let mut map = new_test_map::<2, 2, 1>(1, [11]);
        let scheduled_keys: Vec<_> = (0_u64..10_000)
            .filter(|candidate_key| xxh3_64_with_seed(&candidate_key.to_le_bytes(), 11) >> 63 == 0)
            .take(64)
            .collect();
        assert_eq!(scheduled_keys.len(), 64);
        let mut expected_entries = BTreeMap::new();
        let mut resize_count = 0;

        for inserted_key in scheduled_keys {
            let expected_value = format!("value {inserted_key}");
            let mut pending_value = expected_value.clone();
            loop {
                let buckets_before_insert = map.subtables[0].buckets.len();
                match map.insert(inserted_key, pending_value) {
                    Ok(()) => break,
                    Err(InsertError::NoSpace(returned_key, returned_value)) => {
                        assert_eq!(returned_key, inserted_key);
                        assert_eq!(returned_value, expected_value);
                        assert_eq!(map.lookup(&inserted_key), None);
                        assert_eq!(map.subtables[0].buckets.len(), buckets_before_insert);
                        assert_map_matches(&map, &expected_entries);

                        assert!(
                            resize_count < 16,
                            "insertion must succeed after bounded growth"
                        );
                        assert!(map.resize(0));
                        resize_count += 1;
                        assert_eq!(map.subtables[0].buckets.len(), buckets_before_insert * 2);
                        assert_eq!(map.subtables[1].buckets.len(), 1);
                        assert_map_matches(&map, &expected_entries);
                        pending_value = returned_value;
                    }
                    Err(unexpected_error) => {
                        panic!("unexpected insertion error: {unexpected_error:?}")
                    }
                }
            }
            expected_entries.insert(inserted_key, expected_value);
            assert_map_matches(&map, &expected_entries);
        }

        assert!(resize_count > 1);
        assert_eq!(map.lookup(&u64::MAX), None);
        let (&duplicate_key, _) = expected_entries.first_key_value().unwrap();
        assert_eq!(
            map.insert(duplicate_key, String::from("rejected after resize")),
            Err(InsertError::DuplicateKey(
                duplicate_key,
                String::from("rejected after resize")
            ))
        );
        assert_map_matches(&map, &expected_entries);
    }
}
