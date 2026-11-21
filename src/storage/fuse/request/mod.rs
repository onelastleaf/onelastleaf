mod extensions;
mod header;
mod payload;

pub use extensions::{Extensions, SecurityContext};
pub use header::Header;
pub use payload::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request<'a> {
    pub header: header::Header,
    pub payload: payload::Payload<'a>,
    pub extensions: Option<extensions::Extensions<'a>>,
}
