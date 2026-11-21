mod header;
mod payload;

pub use header::Header;
pub use payload::*;

pub struct Reply<T>(std::marker::PhantomData<T>);
