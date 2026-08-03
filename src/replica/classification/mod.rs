mod detect;
mod ebcdic;
mod encoding;
mod unicode;

#[cfg(test)]
mod tests;

use std::{fs, path::Path};

use infer::MatcherType;
use sha2::{Digest, Sha256};

use super::ReplicaError;
use detect::{decode_text, text_media_type};

pub use encoding::encode_text;
pub(crate) use encoding::is_supported_text_encoding;

#[derive(Clone, Debug)]
pub enum ClassifiedFile {
    Text(DecodedText),
    Binary(BinaryFile),
}

#[derive(Clone, Debug)]
pub struct DecodedText {
    pub text: String,
    pub encoding: String,
    pub has_byte_order_mark: bool,
    pub media_type: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct BinaryFile {
    pub bytes: Vec<u8>,
    pub sha256: String,
    pub media_type: String,
}

pub fn classify_path(path: &Path) -> Result<ClassifiedFile, ReplicaError> {
    let bytes =
        fs::read(path).map_err(|error| ReplicaError::io("read working-tree file", error))?;
    classify_bytes(bytes)
}

pub fn classify_bytes(bytes: Vec<u8>) -> Result<ClassifiedFile, ReplicaError> {
    let inferred = infer::get(&bytes);
    if let Some(kind) = inferred
        && kind.matcher_type() != MatcherType::Text
    {
        return Ok(binary(bytes, kind.mime_type().to_owned()));
    }

    let size_bytes = u64::try_from(bytes.len())
        .map_err(|_| ReplicaError::InvalidArgument("file size does not fit in u64".to_owned()))?;
    if let Some((text, encoding, has_byte_order_mark)) = decode_text(&bytes) {
        let inferred_media_type = inferred
            .filter(|kind| kind.matcher_type() == MatcherType::Text)
            .map(|kind| kind.mime_type());
        return Ok(ClassifiedFile::Text(DecodedText {
            media_type: text_media_type(&text, inferred_media_type),
            text,
            encoding: encoding.name().to_owned(),
            has_byte_order_mark,
            size_bytes,
        }));
    }

    Ok(binary(bytes, "application/octet-stream".to_owned()))
}

fn binary(bytes: Vec<u8>, media_type: String) -> ClassifiedFile {
    let digest = Sha256::digest(&bytes);
    let sha256 = super::lower_hex(&digest);
    ClassifiedFile::Binary(BinaryFile {
        bytes,
        sha256,
        media_type,
    })
}
