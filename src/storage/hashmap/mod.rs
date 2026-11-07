mod tobytes;

pub use self::tobytes::ToBytes;
use xxhash_rust::xxh3::xxh3_64_with_seed;

pub struct HashMap<K: ToBytes, V, const B: usize = 8, const H: usize = 3> {
    subtables: [Subtable<K, V, B>; 256],
    seeds: [u64; H],
}

pub struct Subtable<K, V, const B: usize> {
    buckets: Vec<Bucket<K, V, B>>,
}

// TODO: Option<Slot> is not suitable for the SIMD comparison
pub struct Bucket<K, V, const B: usize> {
    slots: [Option<Slot<K, V>>; B],
    is_full: bool,
}

pub struct Slot<K, V> {
    hash: u64,
    key: K,
    value: V,
}

impl<K, V, const B: usize> Bucket<K, V, B> {
    fn new() -> Self {
        Self {
            slots: [const { None }; B],
            is_full: false,
        }
    }

    fn lookup(&self, key: &K, hash: u64) -> Option<V> {
        todo!()
    }
}

impl<K, V, const B: usize> Subtable<K, V, B> {
    fn new(n: usize) -> Self {
        Self {
            buckets: std::iter::repeat_with(Bucket::new).take(n).collect(),
        }
    }
}

impl<K: ToBytes, V, const B: usize, const H: usize> HashMap<K, V, B, H> {
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

    fn lookup(&self, key: &K) -> Option<V> {
        let hash_values = Self::candidates(key, &self.seeds);

        for i in 0..H {
            let subtable_index: usize = (hash_values[i] >> 56) as usize;
            let subtable_len: usize = self.subtables[subtable_index].buckets.len();
            let mask = subtable_len - 1;
            let bucket_index = (hash_values[i] as usize) & mask;

            if !self.subtables[subtable_index].buckets[bucket_index].is_full {
                self.subtables[subtable_index].buckets[bucket_index].lookup(key, hash_values[i]);
            }
        }

        None
    }
}
