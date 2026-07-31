use std::{borrow::Cow, fs, path::Path};

use chardetng::{EncodingDetector, Iso2022JpDetection, Utf8Detection};
use encoding_rs::{Encoding, UTF_8, UTF_16BE, UTF_16LE};
use sha2::{Digest, Sha256};

use super::ReplicaError;

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
    if let Some(kind) = infer::get(&bytes) {
        return Ok(binary(bytes, kind.mime_type().to_owned()));
    }

    let size_bytes = u64::try_from(bytes.len())
        .map_err(|_| ReplicaError::InvalidArgument("file size does not fit in u64".to_owned()))?;
    if let Some((encoding, bom_len)) = bom_encoding(&bytes)
        && let Some(text) =
            encoding.decode_without_bom_handling_and_without_replacement(&bytes[bom_len..])
    {
        return Ok(ClassifiedFile::Text(DecodedText {
            text: text.into_owned(),
            encoding: encoding.name().to_owned(),
            has_byte_order_mark: true,
            media_type: "text/plain".to_owned(),
            size_bytes,
        }));
    }

    if let Ok(text) = std::str::from_utf8(&bytes) {
        return Ok(ClassifiedFile::Text(DecodedText {
            text: text.to_owned(),
            encoding: UTF_8.name().to_owned(),
            has_byte_order_mark: false,
            media_type: "text/plain".to_owned(),
            size_bytes,
        }));
    }

    let mut detector = EncodingDetector::new(Iso2022JpDetection::Allow);
    detector.feed(&bytes, true);
    let encoding = detector.guess(None, Utf8Detection::Allow);
    if let Some(text) = encoding.decode_without_bom_handling_and_without_replacement(&bytes) {
        return Ok(ClassifiedFile::Text(DecodedText {
            text: text.into_owned(),
            encoding: encoding.name().to_owned(),
            has_byte_order_mark: false,
            media_type: "text/plain".to_owned(),
            size_bytes,
        }));
    }

    Ok(binary(bytes, "application/octet-stream".to_owned()))
}

pub fn encode_text(
    text: &str,
    encoding_name: &str,
    has_byte_order_mark: bool,
) -> Result<(Vec<u8>, bool), ReplicaError> {
    let encoding = Encoding::for_label(encoding_name.as_bytes()).ok_or_else(|| {
        ReplicaError::CorruptStore("document has an unknown text encoding".to_owned())
    })?;
    let (encoded, _, had_errors) = encoding.encode(text);
    if had_errors {
        return Ok((text.as_bytes().to_vec(), true));
    }

    let mut bytes = Vec::with_capacity(encoded.len() + 3);
    if has_byte_order_mark {
        if encoding == UTF_8 {
            bytes.extend_from_slice(&[0xEF, 0xBB, 0xBF]);
        } else if encoding == UTF_16LE {
            bytes.extend_from_slice(&[0xFF, 0xFE]);
        } else if encoding == UTF_16BE {
            bytes.extend_from_slice(&[0xFE, 0xFF]);
        }
    }
    match encoded {
        Cow::Borrowed(slice) => bytes.extend_from_slice(slice),
        Cow::Owned(owned) => bytes.extend_from_slice(&owned),
    }
    Ok((bytes, false))
}

fn bom_encoding(bytes: &[u8]) -> Option<(&'static Encoding, usize)> {
    if bytes.starts_with(&[0xEF, 0xBB, 0xBF]) {
        Some((UTF_8, 3))
    } else if bytes.starts_with(&[0xFF, 0xFE]) {
        Some((UTF_16LE, 2))
    } else if bytes.starts_with(&[0xFE, 0xFF]) {
        Some((UTF_16BE, 2))
    } else {
        None
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_exact_utf_encodings_and_known_binary_signatures() {
        let text = classify_bytes(b"hello".to_vec()).unwrap();
        assert!(matches!(text, ClassifiedFile::Text(_)));

        let utf16 = classify_bytes(vec![0xFF, 0xFE, b'h', 0, b'i', 0]).unwrap();
        let ClassifiedFile::Text(utf16) = utf16 else {
            panic!("UTF-16 input was classified as binary")
        };
        assert_eq!(utf16.text, "hi");
        assert!(utf16.has_byte_order_mark);

        let png = classify_bytes(vec![137, 80, 78, 71, 13, 10, 26, 10]).unwrap();
        assert!(matches!(png, ClassifiedFile::Binary(_)));
    }
}
