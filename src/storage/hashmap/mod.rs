use xxhash_rust::xxh3::xxh3_64_with_seed;

pub struct HashMap<K, V, const B: usize = 8, const H: usize = 3> {
    subtables: [Subtable<K, V, B>; 256],
    seeds: [u64; H],
}

pub struct Subtable<K, V, const B: usize> {
    buckets: Vec<Bucket<K, V, B>>,
}

// TODO: Option<Slot> is not suitable for the SIMD comparison
pub struct Bucket<K, V, const B: usize> {
    slots: [Option<Slot<K, V>>; B],
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
        }
    }
}

impl<K, V, const B: usize> Subtable<K, V, B> {
    fn new(n: usize) -> Self {
        Self {
            buckets: std::iter::repeat_with(Bucket::new).take(n).collect(),
        }
    }
}

impl<K, V, const B: usize, const H: usize> HashMap<K, V, B, H> {
    fn new(buckets_per_subtable: usize) -> Self {
        assert!(B > 0);
        assert!(H > 0);
        assert!(buckets_per_subtable.is_power_of_two());

        Self {
            subtables: std::array::from_fn(|_| Subtable::new(buckets_per_subtable)),
            seeds: std::array::from_fn(|_| std::random::random(..)),
        }
    }
}
