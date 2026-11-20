mod extensions;
mod header;
mod payload;

pub(super) use extensions::{Extensions, SecurityContext};
pub(super) use header::Header;
pub(super) use payload::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Request<'a> {
    pub header: header::Header,
    pub payload: payload::Payload<'a>,
    pub extensions: Option<extensions::Extensions<'a>>,
}
