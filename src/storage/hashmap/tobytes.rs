pub trait ToBytes {
    fn with_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R;
}

macro_rules! impl_to_bytes { // integers and floats implementation
    ($($t:ty),*) => {
        $(
            impl ToBytes for $t {
                fn with_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
                    f(&self.to_le_bytes())
                }
            }
        )*
    };
}

impl_to_bytes!(
    u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, f32, f64
);

// String implementation
impl ToBytes for str {
    fn with_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(self.as_bytes())
    }
}

impl ToBytes for String {
    fn with_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(self.as_bytes())
    }
}

impl<T: ToBytes + ?Sized> ToBytes for &T {
    fn with_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        T::with_bytes(*self, f)
    }
}
