mod tobytes;

pub use self::tobytes::ToBytes;
use std::simd::{Simd, cmp::SimdPartialEq};
use xxhash_rust::xxh3::xxh3_64_with_seed;

const EMPTY_FINGERPRINT: u8 = 0x80;

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
    DuplicateKey(K, V),
    NoSpace(K, V),
    FalseEmpty(K, V),
    FalseOccupied(K, V),
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

            if let Some(index) = vector_fingerprints.simd_eq(vector_fingerprint).first_set() {
                if let Some(slot) = bucket.slots[index].as_ref() {
                    if slot.key == key {
                        return Err(InsertError::DuplicateKey(key, value));
                    }
                } else {
                    eprintln!("[WARN] slot is empty, but fingerprints is occupied");
                    return Err(InsertError::FalseOccupied(key, value));
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
            self.subtables[indexs.0].buckets[indexs.1].slots[indexs.2] = Some(slot);
            self.subtables[indexs.0].buckets[indexs.1].fingerprints[indexs.2] = indexs.3;
            Ok(())
        } else {
            todo!();
        }
    }
}
