use super::session::ParseError;

pub(super) struct Cursor<'a> {
    pub(super) bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
    pub(super) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub(super) fn take(&mut self, number: usize) -> Result<&'a [u8], ParseError> {
        let (taken, remaining) = self
            .bytes
            .split_at_checked(number)
            .ok_or(ParseError::InsufficientLength)?;

        self.bytes = remaining;
        Ok(taken)
    }

    pub(super) fn skip(&mut self, number: usize) -> Result<(), ParseError> {
        self.take(number)?;
        Ok(())
    }

    pub(super) fn u32(&mut self) -> Result<u32, ParseError> {
        let mut bytes = [0; 4];
        bytes.copy_from_slice(self.take(4)?);
        Ok(u32::from_ne_bytes(bytes))
    }

    pub(super) fn u64(&mut self) -> Result<u64, ParseError> {
        let mut bytes = [0; 8];
        bytes.copy_from_slice(self.take(8)?);
        Ok(u64::from_ne_bytes(bytes))
    }

    pub(super) fn read_until_nul(&mut self) -> Result<&'a [u8], ParseError> {
        let end = self
            .bytes
            .iter()
            .position(|&byte| byte == 0)
            .ok_or(ParseError::InsufficientLength)?;

        let field = &self.bytes[..end];
        self.bytes = &self.bytes[end + 1..];
        Ok(field)
    }

    pub(super) fn finish(&self) -> bool {
        self.bytes.is_empty()
    }
}
