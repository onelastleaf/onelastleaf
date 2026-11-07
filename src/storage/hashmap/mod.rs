mod tobytes;

pub use self::tobytes::ToBytes;
use std::simd::Simd;
use xxhash_rust::xxh3::xxh3_64_with_seed;

/// K is the generic type used by callers for lookups.
/// V is the value type that callers of this HashMap store and retrieve.
/// B is the number of slots in each bucket. It should be the power of 2.
/// S is the number of subtables in the HashMap. It should be the power of 2. For SIMD optimization,
/// it can only be one of 8, 16, 32, and 64.
/// H is the number of candidates hash positions per key.
pub struct HashMap<
    K: ToBytes + PartialEq,
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
    is_full: bool,
}

pub struct Slot<K, V> {
    key: K,
    value: V,
}

impl<K: PartialEq, V, const B: usize> Bucket<K, V, B> {
    fn new() -> Self {
        Self {
            slots: [const { None }; B],
            fingerprints: [0x80; B],
            is_full: false,
        }
    }

    fn lookup(&self, key: &K, fingerprint: u8) -> Option<&V> {
        todo!()
    }
}

impl<K: PartialEq, V, const B: usize> Subtable<K, V, B> {
    fn new(n: usize) -> Self {
        Self {
            buckets: std::iter::repeat_with(Bucket::new).take(n).collect(),
        }
    }
}

impl<K: ToBytes + PartialEq, V, const B: usize, const S: usize, const H: usize>
    HashMap<K, V, B, S, H>
{
    fn new(buckets_per_subtable: usize) -> Self {
        assert!(B > 0);
        assert!(H > 0);
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

        if fingerprint == 0x80 {
            0x81
        } else {
            fingerprint
        }
    }

    fn lookup(&self, key: &K) -> Option<&V> {
        let hash_values: [u64; H] = Self::candidates(key, &self.seeds);

        for hash in hash_values {
            let high_bits_number: u32 = S.ilog2();
            let subtable_index: usize = (hash >> (usize::BITS - high_bits_number)) as usize;
            let subtable_len = self.subtables[subtable_index].buckets.len();
            let bucket_index = (hash as usize) & (subtable_len - 1);
            // why (subtable_len - 1)?
            // because subtable_len is power of 2, (subtable_len - 1) has all 1 in low bits

            let fingerprint = Self::fingerprint(hash);

            self.subtables[subtable_index].buckets[bucket_index].lookup(key, fingerprint);
        }

        None
    }
}
