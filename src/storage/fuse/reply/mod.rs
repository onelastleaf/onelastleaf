mod header;
mod payload;

pub use header::Header;
pub use payload::*;

use nix::errno::Errno;

// Why I use `dyn`?
//
// To be honest, I never wanted to use `dyn` in the first place, because a `vtable` means a runtime
// table lookup. That's runtime overhead. If I can turn that into compile-time overhead, I can cut
// down the runtime cost.
//
// If I turn `Reply` into an `enum + match`, I can optimize static compilation and do a bunch of
// really nice things, like inlining. But writing `Reply` as `dyn` is really elegant. `dyn` is a
// completely stateless middleman: the caller makes calls through a fixed interface, the provider
// writes the package that provides it, and neither side needs to know how that stateless interface
// in the middle is implemented.
//
// But if I use `impl` and `match`, I have to encode all the information in the state. I'd have to
// nail the interface definition down exactly; I couldn't just stuff in a closure—I'd have to stuff
// in a state value. I'd need to define what state `read` passes in and what state `lookup` should
// pass in. Sure, the caller—any library using this filesystem—always ends up needing to know these
// states anyway, but it gets really ugly.
//
// So I decided to go with `dyn` after all. There aren't that many requests. This `dyn` only gets
// hit on reply; it's not a call that happens hundreds or thousands of times per second, and each
// call only costs about 1–2 ms, so it's actually acceptable.
//
// But using `dyn` still sticks in my craw. I'm the kind of person who gets really uncomfortable
// when I grep for `dyn` in a project. I treat the codebase as a work of art; using `dyn` makes me
// miserable, and I have to vent.
type Finish<T> = Box<dyn FnOnce(Result<T, Errno>) -> Result<(), CantReplyError> + Send + 'static>;

/// What is "CantReplyError"?
/// When file system instance want to commit a reply, it find the protocol stack thread totally
/// disappear —— no reason, just can't reply. File systme instance don't know why, it can only throw
/// a CantReplyError.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CantReplyError {
    /// The reply queue's receiver was dropped before the reply could be queued.
    QueueClosed,
}

impl std::fmt::Display for CantReplyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::QueueClosed => f.write_str("the FUSE reply queue has closed"),
        }
    }
}

impl std::error::Error for CantReplyError {}

#[must_use = "complete the reply or pass it to a worker"]
pub struct Reply<T> {
    finish: Option<Finish<T>>,
}

impl<T> Reply<T> {
    pub(super) fn new(
        finish: impl FnOnce(Result<T, Errno>) -> Result<(), CantReplyError> + Send + 'static,
    ) -> Self {
        Self {
            finish: Some(Box::new(finish)),
        }
    }

    pub fn complete(mut self, result: Result<T, Errno>) -> Result<(), CantReplyError> {
        let finish = self.finish.take().expect("a pending reply has a callback");
        finish(result)
    }
}

impl<T> Drop for Reply<T> {
    fn drop(&mut self) {
        if let Some(finish) = self.finish.take() {
            let _ = finish(Err(Errno::EIO));
        }
    }
}
